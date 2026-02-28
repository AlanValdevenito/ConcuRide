use crate::functions::{distance, new_connection};
use crate::gateway_connection::GatewayConnection;
use crate::models::driver::DriverInfo;
use crate::models::gateway::{AuthorizePayment, PaymentDeny};
use crate::models::passenger::{
    CompleteLogin, ConfirmPayment, DriverArrived, Drivers, GetDrivers, Login, PassengerInfo,
    PaymentAuthorized, Reconnect, ReconnectDriver, Recover, RecoverResponse, RequestRide,
    RideAccepted, SetReceiver, Traveling,
};
use crate::models::passenger::{KeepLooking, RequestDrive};
use crate::receiver::TcpReceiver;
use crate::sender::TcpSender;
use actix::clock::sleep;
use actix::{Actor, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix_async_handler::async_handler;
use colored::*;
use common::connection::connect_to_server_leader;
use common::{serialize::serialize_message, tcp_message::TcpMessage};
use log::{debug, error, info};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::split;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Semaphore;
use tokio_stream::wrappers::LinesStream;

const MAX_RANGE: u64 = 1000;

pub struct Passenger {
    /// Nombre del pasajero
    pub name: String,
    /// Informacion del pasajero
    pub info: PassengerInfo,
    /// Booleano de recuperacion en caso de caida
    pub recovery: bool,
    /// Booleano del estado de la autorizacion
    pub payment_authorized: bool,
    /// Booleano del estado del pago
    pub payment_confirmed: bool,
    /// Comunicacion con el servidor
    pub server_receiver: Option<Addr<TcpReceiver>>,
    /// Comunicacion con el servidor
    pub server_sender: Addr<TcpSender>,
    /// Conexiones
    pub connections: Vec<Addr<TcpSender>>,
    // Informacion de los conductores que recibe del servidor
    pub drivers_info: Vec<DriverInfo>,
    /// Socket del conductor seleccionado
    pub driver_socket: Option<String>,
    // Comunicacion con el Gateway
    pub gateway: Addr<GatewayConnection>,
    pub finished_signal: Arc<Semaphore>,
}

impl Passenger {
    /// Recorre la lista de conductores, hasta encontrar a uno que se encuentre en un radio de distancia aceptado
    /// Si no encuentra ninguno, amplia el rango. Esto lo repite n cantidad de veces hasta encontrar un conductor o que se alcance el rango limite.
    pub fn select_driver(&mut self, range: u64) -> Option<DriverInfo> {
        for i in 0..self.drivers_info.len() {
            let driver = &self.drivers_info[i];
            let distance = distance(self.info.position, driver.position);
            if distance < range {
                info!("Conductor encontrado a distancia {distance}");

                // Sacamos el driver elegido para no repetirlo en caso de que este caido
                let driver = self.drivers_info.remove(i);
                return Some(driver);
            }
        }
        if range > MAX_RANGE {
            info!("{}", "No se encontraron conductores 😫".red());
            None
        } else {
            let expanded_range = range + 10;
            info!(
                "No hay conductores en tu zona, ampliando el rango a {}\n",
                expanded_range
            );
            self.select_driver(expanded_range)
        }
    }

    /// Guarda el estado del pasajero en un archivo, el cual se denomina de la forma "passenger_{nombre del pasajero}"
    async fn save_state(
        name: String,
        drivers_info: Vec<DriverInfo>,
        driver_socket: Option<String>,
        payment_authorized: bool,
        payment_confirmed: bool,
        id: usize,
    ) {
        let status_file_name = format!("passenger_{}.json", name);

        if let Ok(mut status_file) = tokio::fs::File::create(status_file_name).await {
            let write_result = status_file
                .write_all(
                    json!({
                        "drivers": drivers_info,
                        "driver_socket": driver_socket,
                        "payment_authorized": payment_authorized,
                        "id": payment_confirmed,
                        "id": id,
                    })
                    .to_string()
                    .as_bytes(),
                )
                .await;
            if let Err(err) = write_result {
                debug!("Fallo al guardar estado en el archivo. Error: {err}");
            }
        }
    }

    /// Se acerca al destino en 1 tanto en la coordenada x como en la coordenada y.
    fn move_to(&mut self, destination: (u64, u64)) {
        match self.info.position.0 {
            x if x < destination.0 => self.info.position.0 += 1,
            _ => self.info.position.0 -= 1,
        }

        match self.info.position.1 {
            y if y < destination.1 => self.info.position.1 += 1,
            _ => self.info.position.1 -= 1,
        }

        info!(
            "Posición: ({}, {})",
            self.info.position.0, self.info.position.1
        );
    }
}

impl Actor for Passenger {
    type Context = Context<Self>;
}

/// Se utiliza para almacenar la direccion del actor TcpReceiver que se utiliza para comunicarse con el servidor
impl Handler<SetReceiver> for Passenger {
    type Result = ();

    fn handle(&mut self, msg: SetReceiver, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::SetReceiver");
        self.server_receiver = Some(msg.receiver);
    }
}

/// Solicita al servidor los conductores disponibles para poder comunicarse con ellos y concretar el viaje
/// Luego de enviar el mensaje valida que la conexion con el servidor exista, en caso de que se haya perdido la conexion
/// se reconecta con el nuevo lider y reenvia el mensaje para no perder informacion.
#[async_handler]
impl Handler<GetDrivers> for Passenger {
    type Result = ();

    fn handle(&mut self, _msg: GetDrivers, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::GetDrivers");
        debug!("Send TcpMessage to TcpSender\n");

        //println!("\n\n Duermo para cortar la conexion\n\n");
        //sleep(Duration::from_secs(5)).await;

        let id = self.info.id;
        let serialized_msg = serialize_message("get_drivers", json!({"id": id}));

        if let Err(e) = self
            .server_sender
            .try_send(TcpMessage::new(serialized_msg.clone()))
        {
            error!("Error al enviar el mensaje: {}", e);
        }

        // Veo si el actor TcpSender sigue vivo (si el servidor lider se desconecta y genera un Broken Pipe entonces el actor muere)
        let server_connected = if let Some(receiver) = self.server_receiver.as_ref() {
            receiver.connected()
        } else {
            false
        };

        debug!("Estado de conexion {:?}\n", server_connected);

        if !server_connected {
            if let Err(e) = _ctx.address().try_send(Reconnect { serialized_msg }) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

/// Selecciona un conductor y se comunica con el gateway para solicitar autorizacion para realizar el viaje
/// Almacena la conexion con el driver seleccionado
#[allow(clippy::unused_unit)]
#[async_handler]
impl Handler<Drivers> for Passenger {
    type Result = ();

    async fn handle(
        &mut self,
        msg: Drivers,
        _ctx: &mut <Passenger as Actor>::Context,
    ) -> Self::Result {
        debug!("Passenger::Drivers");
        debug!("Send TcpMessage to TcpSender\n");

        let range = 5;
        self.drivers_info = msg.drivers_info;
        Passenger::save_state(
            self.name.clone(),
            self.drivers_info.clone(),
            self.driver_socket.clone(),
            self.payment_authorized,
            self.payment_confirmed,
            self.info.id,
        )
        .await;

        if let Some(driver_info) = self.select_driver(range) {
            let addr = _ctx.address().clone();
            // self.driver_socket = Some(driver_info.socket.clone());
            let driver = new_connection(driver_info.socket.clone(), addr).await;
            // TODO: mejorar el manejo de errores
            if driver.is_err() {
                error!("{}", "Error al conectarse al conductor".red());

                if let Err(e) = _ctx.address().try_send(Drivers {
                    drivers_info: self.drivers_info.clone(),
                }) {
                    error!("Error al enviar el mensaje: {}", e);
                }
            } else {
                info!(
                    "{}",
                    format!(
                        "[{:?}] Conductor ubicado en {:?} conectado\n",
                        driver_info.socket.clone(),
                        driver_info.position.clone(),
                    )
                    .green()
                );
                let serialized_msg = serialize_message("AuthorizePayment", AuthorizePayment {});

                if let Err(e) = self
                    .gateway
                    .try_send(TcpMessage::new(serialized_msg.clone()))
                {
                    error!("Error al enviar el mensaje: {}", e);
                }

                if self.connections.is_empty() {
                    self.connections
                        .push(driver.expect("Se espera que sea un conductor"));
                } else {
                    // guarda la coneccion con el driver
                    self.connections[0] = driver.expect("Se espera que sea un conductor");
                }
            }
        }
    }
}

/// Se loguea en el servidor, registrandose con su nombre
/// Si la conexion esta caida, se conecta con el nuevo lider
#[async_handler]
impl Handler<Login> for Passenger {
    type Result = ();

    fn handle(&mut self, msg: Login, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::Login");
        debug!("Send TcpMessage to TcpSender\n");
        let serialized_msg = serialize_message("login", msg);

        if let Err(e) = self
            .server_sender
            .try_send(TcpMessage::new(serialized_msg.clone()))
        {
            error!("Error al enviar el mensaje: {}", e);
        }

        // Veo si el actor TcpSender sigue vivo (si el servidor lider se desconecta y genera un Broken Pipe entonces el actor muere)
        let server_connected = if let Some(receiver) = self.server_receiver.as_ref() {
            receiver.connected()
        } else {
            false
        };

        debug!("Estado de conexion {:?}\n", server_connected);

        if !server_connected {
            if let Err(e) = _ctx.address().try_send(Reconnect { serialized_msg }) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }

        sleep(Duration::from_secs(1)).await; // para agregarle realismo y que no sea todo tan inmediato
    }
}

/// Se encarga de actualizar la posicion del pasajero simulando el viaje.
/// Una vez que llega a destino finaliza el viaje y le solicita la confirmacion del pago al Gateway.
#[async_handler]
impl Handler<Traveling> for Passenger {
    type Result = ();

    async fn handle(&mut self, _msg: Traveling, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::Traveling");

        actix::clock::sleep(Duration::from_millis(500)).await;

        Passenger::save_state(
            self.name.clone(),
            self.drivers_info.clone(),
            self.driver_socket.clone(),
            self.payment_authorized,
            self.payment_confirmed,
            self.info.id,
        )
        .await;

        self.move_to(self.info.destination);

        // Si todavia no llegue al destino, mando Traveling
        if self.info.position != self.info.destination {
            if let Err(e) = _ctx.address().try_send(Traveling {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
            // return;
        } else {
            info!("Llegaste a destino! Viaje terminado ");

            let name = self.name.clone();
            let serialized_msg = serialize_message("ConfirmPayment", ConfirmPayment { name });

            if let Err(e) = self
                .gateway
                .try_send(TcpMessage::new(serialized_msg.clone()))
            {
                error!("Error al enviar el mensaje: {}", e);
            }
            // Mando signal de que terminó el programa
            self.finished_signal.add_permits(1);
        }
    }
}

/// Indica que el conductor se encuentra en el lugar de partida, listo para comenzar el viaje.
/// Inicia el viaje.
impl Handler<DriverArrived> for Passenger {
    type Result = ();

    fn handle(&mut self, _msg: DriverArrived, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::DriverArrived");
        info!("El conductor llegó a tu ubicación. Viajando...🏎️💨");
        if let Err(e) = _ctx.address().try_send(Traveling {}) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

/// Recibe del servidor el ID con el cual se identifica en el servidor y lo guarda
#[async_handler]
impl Handler<CompleteLogin> for Passenger {
    type Result = ();

    fn handle(&mut self, msg: CompleteLogin, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::CompleteLogin");
        info!("{}", "Login complete\n".green());

        self.info.id = msg.id;
    }
}

/// Indica que el conductor acepto el viaje
/// Luego espera a que el conductor le notifique que llego al punto de partida
#[async_handler]
impl Handler<RideAccepted> for Passenger {
    type Result = ();

    fn handle(&mut self, msg: RideAccepted, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::RideAccepted");

        self.driver_socket = Some(msg.driver_socket.clone());

        Passenger::save_state(
            self.name.clone(),
            self.drivers_info.clone(),
            self.driver_socket.clone(),
            self.payment_authorized,
            self.payment_confirmed,
            self.info.id,
        )
        .await;

        info!("{}", "Esperando que el conductor me pase a buscar".blue());
    }
}

/// Solicita al conductor un viaje a un destino en particular
#[async_handler]
impl Handler<RequestDrive> for Passenger {
    type Result = ();

    fn handle(&mut self, _msg: RequestDrive, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::RequestDrive");

        let request_ride = RequestRide {
            id: self.info.id,
            origin: self.info.position,
            destination: self.info.destination,
        };
        let serialized_msg = serialize_message("request_ride", request_ride);

        debug!("{:?} cantidad", self.connections);
        if let Err(e) = self.connections[0].try_send(TcpMessage::new(serialized_msg)) {
            error!("Error al enviar el mensaje: {}", e);
        }

        // println!("\n\n Duermo para cortar la conexion\n\n");
        // sleep(Duration::from_secs(5)).await;
    }
}

///  En caso de que el conductor rechace el viaje, vuelve a iniciar otra busqueda
#[async_handler]
impl Handler<KeepLooking> for Passenger {
    type Result = ();

    fn handle(&mut self, _msg: KeepLooking, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::KeepLooking");

        if let Err(e) = _ctx.address().try_send(Drivers {
            drivers_info: self.drivers_info.clone(),
        }) {
            error!("Error al enviar el mensaje: {}", e);
        }

        sleep(Duration::from_secs(1)).await;
    }
}

/// Indica que el pago fue autorizado por el Gateway actualizando el estado
#[async_handler]
impl Handler<PaymentAuthorized> for Passenger {
    type Result = ();

    fn handle(&mut self, _msg: PaymentAuthorized, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::PaymentAuthorized");
        self.payment_authorized = true;
        Passenger::save_state(
            self.name.clone(),
            self.drivers_info.clone(),
            self.driver_socket.clone(),
            self.payment_authorized,
            self.payment_confirmed,
            self.info.id,
        )
        .await;
    }
}

// El pago fue denegado por el Gateway, comienzo nuevamente la busqueda por un conductor
#[async_handler]
impl Handler<PaymentDeny> for Passenger {
    type Result = ();

    fn handle(&mut self, _msg: PaymentDeny, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::PaymentDeny");
        self.payment_authorized = false;
        Passenger::save_state(
            self.name.clone(),
            self.drivers_info.clone(),
            self.driver_socket.clone(),
            self.payment_authorized,
            self.payment_confirmed,
            self.info.id,
        )
        .await;

        // println!("\nSimular caida\n");
        // actix::clock::sleep(Duration::from_secs(10)).await;

        if let Err(e) = _ctx.address().try_send(GetDrivers {}) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

/// Luego de identificar la perdida de conexion y que el conductor se encontraba en viaje, recibe el estado del mismo de parte del conductor
#[async_handler]
impl Handler<RecoverResponse> for Passenger {
    type Result = ();

    fn handle(&mut self, msg: RecoverResponse, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::RecoverResponse");
        // Si el conductor ya llego a buscarme y estaba en viaje
        if msg.end_ride {
            // Si el conductor ya me dejo en mi destino, lo actualizo
            info!("Llegaste a destino! Viaje terminado ");

            let name = self.name.clone();
            let serialized_msg = serialize_message("ConfirmPayment", ConfirmPayment { name });

            if let Err(e) = self
                .gateway
                .try_send(TcpMessage::new(serialized_msg.clone()))
            {
                error!("Error al enviar el mensaje: {}", e);
            }
            // Mando signal de que terminó el programa
            self.finished_signal.add_permits(1);
        } else if msg.picked_up {
            self.info.position = msg.position;
            if let Err(e) = _ctx.address().try_send(Traveling {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
    }
}

/// En caso de que se pierda la conexion con el servidor lider, intenta reconectarse al nuevo lider. Se reconecta y almacena todos los caneles de comunicacion con el servidor lider, luego le reenvia el mensaje que no pudo enviarse debido a la caida del mismo
#[async_handler]
impl Handler<Reconnect> for Passenger {
    type Result = ();

    async fn handle(&mut self, msg: Reconnect, _ctx: &mut Self::Context) -> Self::Result {
        info!("Passenger::Reconnect");
        info!("No hay respuesta del servidor");

        let client_addr = _ctx.address().clone();

        info!("Reconectando al nuevo lider");
        let stream = async move { connect_to_server_leader().await }.await;
        let Ok(stream) = stream else {
            return;
        };

        let addr = stream
            .local_addr()
            .expect("Error al obtener socket de conexion con el server lider");
        let (read, write_half) = split(stream);

        let sender = TcpSender {
            write: Some(write_half),
            addr,
        }
        .start();

        let receiver = TcpReceiver::create(|_ctx| {
            TcpReceiver::add_stream(LinesStream::new(BufReader::new(read).lines()), _ctx);
            TcpReceiver {
                client_addr: client_addr.clone(),
                addr,
            }
        });

        // Actualizamos el sender del Passenger
        self.server_sender = sender;

        // Actualizamos el receiver del Passenger
        self.server_receiver = Some(receiver);

        // Reenviamos el mensaje
        if let Err(e) = self
            .server_sender
            .try_send(TcpMessage::new(msg.serialized_msg.clone()))
        {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

/// Recibe del servidor la notificacion de que el conductor se reconecto y se intenta reconectar al conductor. Si es posible se almacena la conexion y envia al conductor un mensaje que indica el intento de reconexion. En caso contrario notifica que hay un error.
#[async_handler]
impl Handler<ReconnectDriver> for Passenger {
    type Result = ();

    async fn handle(&mut self, msg: ReconnectDriver, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::ReconnectDriver");

        let addr = _ctx.address().clone();

        // debug!("Duermo para esperar que el conductor se ponga a escuchar conexiones...");
        // actix::clock::sleep(Duration::from_secs(3)).await;

        debug!("Intento conectarme al conductor...");

        let connection_driver = new_connection(msg.socket, addr).await;

        if let Ok(driver) = connection_driver {
            if self.connections.is_empty() {
                self.connections.push(driver.clone());
            } else {
                self.connections[0] = driver.clone(); // guarda la coneccion con el driver
            }

            let position = self.info.position;
            let serialized_msg =
                serialize_message("reconnect_driver", json!({"position": position }));

            if let Err(e) = driver.try_send(TcpMessage::new(serialized_msg)) {
                error!("Error al enviar el mensaje: {}", e);
            }
        } else {
            error!("Error al reconectarme al conductor");
        }
    }
}

/// Dado que perdimos la conexion, restauramos nuestro estado utilizando el archivo correspondiente.
/// Si el pago habia sido confirmado, entonces indica que el viaje termino.
/// Si el pago no fue autorizado, entonces indica que no estoy en viaje ni solicitando uno.
/// Caso contrario, si tengo un conductor asignado, entonces estoy en viaje y le solicito el estado del mismo.
#[allow(clippy::unused_unit)]
#[async_handler]
impl Handler<Recover> for Passenger {
    type Result = ();

    async fn handle(&mut self, msg: Recover, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::Recover");

        if let Some(name) = msg.status["name"].as_str() {
            self.name = name.into();
        }
        if let Some(id) = msg.status["id"].as_u64() {
            self.info.id = id as usize;
        }
        if let Some(driver_socket) = msg.status["driver_socket"].as_str() {
            self.driver_socket = Some(driver_socket.into());
        }

        if let Some(payment_authorized) = msg.status["payment_authorized"].as_bool() {
            self.payment_authorized = payment_authorized;
        }
        if let Some(payment_confirmed) = msg.status["payment_confirmed"].as_bool() {
            self.payment_confirmed = payment_confirmed;
        }

        debug!("Se leyo el estado anterior");
        debug!("Pasajero nombre: {:?}", self.name);
        debug!("Pasajero ID: {:?}", self.info.id);
        debug!("Conductor: {:?}", self.driver_socket);
        debug!("Autorizacion del pago: {:?}", self.payment_authorized);
        debug!("Confirmacion del pago: {:?}", self.payment_confirmed);

        let name = self.name.clone();
        if let Err(e) = _ctx.address().try_send(Login { name }) {
            error!("Error al enviar el mensaje Login: {}", e);
        }

        if self.payment_confirmed {
            info!("El viaje ya habia terminado");
        } else if !self.payment_authorized {
            info!("El pago no fue autorizado, se debe reintentar");
            if let Err(e) = _ctx.address().try_send(GetDrivers {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
        } else if let Some(driver_socket) = self.driver_socket.clone() {
            debug!("driver_socket recuperado = {}", driver_socket);
            let driver = new_connection(driver_socket, _ctx.address().clone()).await;

            if let Ok(driver) = driver {
                let id = self.info.id;
                let serialized_msg = serialize_message("recover", json!({"id_passenger": id}));

                if let Err(e) = driver.try_send(TcpMessage::new(serialized_msg)) {
                    error!("Error al enviar el mensaje \"recover\": {}", e);
                }
            }
        }
    }
}
