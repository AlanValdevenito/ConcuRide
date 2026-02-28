use crate::models::messages::{
    AvailableDriver, BusyDriver, EndRide, NewConnection, PassengerRecover, PickUpPassenger,
    Reconnect, ReconnectPassenger, Recovery, Register, RequestRide, RideToDestination, SetReceiver,
    StartRide,
};
use crate::models::passenger::PassengerConnectionMessage;
use crate::passenger_connection::PassengerConnection;
use crate::receiver::TcpReceiver;
use crate::sender::TcpSender;
use actix::{Actor, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix_async_handler::async_handler;
use common::connection::connect_to_server_leader;
use common::{serialize::serialize_message, tcp_message::TcpMessage};
use log::{debug, error, info};
use serde_json::json;
use std::time::Duration;
use tokio::io::{split, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_stream::wrappers::LinesStream;

pub struct Driver {
    pub server_receiver: Option<Addr<TcpReceiver>>,
    pub server_sender: Addr<TcpSender>,
    pub connections: Vec<Addr<PassengerConnection>>,
    // pub drivers: Vec<String>,
    pub id_current_passenger: usize,
    pub is_available: bool,
    pub is_registered: bool,
    pub position: (u64, u64),
    pub passenger_position: Option<(u64, u64)>,
    pub destination: Option<(u64, u64)>,
    pub name: String,
    pub current_passenger_connection: Option<Addr<PassengerConnection>>,
    pub passenger_picked_up: bool,
    pub driver_socket: String,
}

impl Driver {
    /// Se acerca al destino en 1 tanto en la coordenada x como en la coordenada y.
    fn move_to(&mut self, destination: (u64, u64)) {
        match self.position.0 {
            x if x < destination.0 => self.position.0 += 1,
            _ => self.position.0 -= 1,
        }

        match self.position.1 {
            y if y < destination.1 => self.position.1 += 1,
            _ => self.position.1 -= 1,
        }

        info!("Posición: ({}, {})", self.position.0, self.position.1);
    }

    /// Guarda el estado en un archivo json con el nombre del driver (identificador en el server)
    /// Se usa para persistir la informacion en caso de perdidas de conexion
    /// Almacena toda la informacion relevante del conductor para reestaurar el estado
    #[allow(clippy::too_many_arguments)]
    async fn save_state(
        name: String,
        is_available: bool,
        position: (u64, u64),
        id_current_passenger: usize,
        passenger_position: Option<(u64, u64)>,
        destination: Option<(u64, u64)>,
        passenger_picked_up: bool,
        is_registered: bool,
        driver_socket: String,
    ) {
        let status_file_name = format!("driver_{}.json", name);

        if let Ok(mut status_file) = tokio::fs::File::create(status_file_name).await {
            let write_result = status_file
                .write_all(
                    json!({
                        "name": name,
                        "is_available": is_available,
                        "position": position,
                        "id_current_passenger": id_current_passenger,
                        "passenger_position": passenger_position,
                        "destination": destination,
                        "passenger_picked_up": passenger_picked_up,
                        "is_registered": is_registered,
                        "driver_socket": driver_socket,
                    })
                    .to_string()
                    .as_bytes(),
                )
                .await;
            if let Err(err) = write_result {
                debug!("Error al escribir en archivo. Error: {err}")
            }
        }
    }
}

impl Actor for Driver {
    type Context = Context<Self>;
}

/// Se utiliza para almacenar el contexto del actor server_receiver el cual es el canal de comunicacion con el servidor lider
impl Handler<SetReceiver> for Driver {
    type Result = ();

    fn handle(&mut self, msg: SetReceiver, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::SetReceiver");
        self.server_receiver = Some(msg.receiver);
    }
}

/// Este mensaje reestablece la conexion con el servidor, dado que el servidor lider se desconecto
/// entonces debemos conectarnos con el nuevo lider y reenviar el mensaje que no se pudo enviar al
/// lider anterior, para no perder ningun tipo de informacion del lado del server.
#[async_handler]
impl Handler<Reconnect> for Driver {
    type Result = ();

    async fn handle(&mut self, msg: Reconnect, _ctx: &mut Self::Context) -> Self::Result {
        info!("No hay respuesta del servidor");

        let client_addr = _ctx.address().clone();

        let (new_server_sender, new_server_receiver) = async move {
            info!("Reconectando al nuevo lider");

            let Ok(stream) = connect_to_server_leader().await else {
                error!("No se pudo reconectar al nuevo lider.");
                return (None, None);
            };

            let Ok(addr) = stream.local_addr() else {
                error!("No se pudo reconectar al nuevo lider. No tengo local address.");
                return (None, None);
            };

            let (read, write_half) = split(stream);

            let sender = TcpSender {
                write: Some(write_half),
                addr,
            }
            .start();

            let receiver = TcpReceiver::create(|ctx| {
                TcpReceiver::add_stream(LinesStream::new(BufReader::new(read).lines()), ctx);
                TcpReceiver { client_addr, addr }
            });

            (Some(sender), Some(receiver))
        }
        .await;

        if new_server_sender.is_none() || new_server_receiver.is_none() {
            return;
        }

        // Actualizamos el receiver del Driver
        self.server_receiver = new_server_receiver;

        // Actualizamos el sender del Driver
        if let Some(server_sender) = new_server_sender {
            self.server_sender = server_sender;
        }

        // Reenviamos el mensaje
        if let Err(e) = self
            .server_sender
            .try_send(TcpMessage::new(msg.serialized_msg.clone()))
        {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

/// En este mensaje se le envia al servidor el mensaje new_driver, para de esta manera identificarnos como un conductor
/// Luego de enviar el mensaje se valida que la conexion con el servidor exista
/// En caso de que no exista se envia el mensaje Reconnect a si mismo. (actor driver)
#[async_handler]
impl Handler<Register> for Driver {
    type Result = ();

    async fn handle(&mut self, msg: Register, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::Register");
        debug!("Sent TcpMessage to TcpSender\n");

        let serialized_msg = serialize_message("new_driver", msg);

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
        self.is_registered = true;
        // guardo estado
        Driver::save_state(
            self.name.clone(),
            self.is_available,
            self.position,
            self.id_current_passenger,
            self.passenger_position,
            self.destination,
            self.passenger_picked_up,
            self.is_registered,
            self.driver_socket.clone(),
        )
        .await;
    }
}

/// Se recibe este mensaje indicando que se establecio una conexion con un nuevo pasajero
/// Se almacena el elemento en connections, suponiendo de que el conductor puede recibir mensajes de
/// mas de un pasajero, mientras esta desocupado
impl Handler<NewConnection> for Driver {
    type Result = ();

    fn handle(&mut self, msg: NewConnection, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::NewConnection\n");
        self.connections.push(msg.addr);
    }
}

/// Recibe el mensaje de un pasajero
/// En caso de estar ocupado le rechaza la peticion de viaje al pasajero
/// Caso contrario, se la acepta y loggea la trayectoria del viaje
/// A su vez, notifica al servidor de que se encuentra ocupado. En caso de que se haya perdido la conexion con el mismo, se reconecta
#[allow(clippy::unused_unit)]
#[async_handler]
impl Handler<RequestRide> for Driver {
    type Result = ();

    async fn handle(&mut self, msg: RequestRide, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::RequestRide");
        debug!("Send TcpMessage to PassengerConnection\n");

        // Si el conductor esta ocupado, responde "no"
        if !self.is_available {
            let serialized_msg = serialize_message("no", ());
            let response = PassengerConnectionMessage::new(serialized_msg);

            if let Err(e) = msg.addr.try_send(response) {
                error!("Error al enviar el mensaje: {}", e);
            }
        } else {
            // Si el conductor esta disponible, responde "ok" y loggea la trayectoria del viaje
            self.id_current_passenger = msg.id_passenger;
            self.passenger_position = Some(msg.origin);
            self.destination = Some(msg.destination);
            self.is_available = false;
            self.current_passenger_connection = Some(msg.addr.clone());
            let driver_socket = self.driver_socket.clone();
            let serialized_msg = serialize_message("ok", json!({"driver_socket": driver_socket}));
            let response = PassengerConnectionMessage::new(serialized_msg);

            if let Err(e) = msg.addr.try_send(response) {
                error!("Error al enviar el mensaje: {}", e);
            }

            Driver::save_state(
                self.name.clone(),
                self.is_available,
                self.position,
                self.id_current_passenger,
                self.passenger_position,
                self.destination,
                self.passenger_picked_up,
                self.is_registered,
                self.driver_socket.clone(),
            )
            .await;

            // Le aviso al server que estoy ocupado para que no reparta mi socket
            let busy_driver_msg = BusyDriver {
                name: self.name.clone(),
            };

            // println!("Duremo para ver la desconexion");
            // actix::clock::sleep(Duration::from_secs(5)).await;

            let serialized_msg = serialize_message("busy_driver", busy_driver_msg);

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

            if let Err(e) = _ctx.address().try_send(StartRide {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
    }
}

/// Envia el mensaje PickupPassenger
impl Handler<StartRide> for Driver {
    type Result = ();

    fn handle(&mut self, _msg: StartRide, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::StartRide");
        info!("Buscando al pasajero");

        if let Err(e) = _ctx.address().try_send(PickUpPassenger {}) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

/// Simula el translado del conductor hacia la posicion del pasajero
/// Si todavia no se encuentra en la posicion se envia el mensaje a si mismo de forma recursiva
/// una vez que llega a destino, notifica al pasajero y comienza el viaje
#[async_handler]
impl Handler<PickUpPassenger> for Driver {
    type Result = ();

    async fn handle(&mut self, _msg: PickUpPassenger, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::PickUpPassenger");

        actix::clock::sleep(Duration::from_millis(500)).await;

        let Some(passenger_position) = self.passenger_position else {
            error!("No voy a buscar al pasajero porque no tengo su posicion");
            return;
        };

        self.move_to(passenger_position);

        // Si todavia no llegue a la posición del pasajero, mando PickUpPassenger
        if self.position != passenger_position {
            if let Err(e) = _ctx.address().try_send(PickUpPassenger {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
            return;
        }
        info!("Esperando al pasajero...");

        self.passenger_picked_up = true;

        let serialized_msg = serialize_message("driver_arrived", ());
        let driver_arrived_msg = PassengerConnectionMessage::new(serialized_msg);
        if let Some(passenger_connection) = self.current_passenger_connection.as_ref() {
            if let Err(e) = passenger_connection.try_send(driver_arrived_msg) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
        info!("Viajando hacia el destino");

        if let Err(e) = _ctx.address().try_send(RideToDestination {}) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

/// Simula el translado del viaje
/// Si todavia no llego al destino pactado, envia el mismo mensaje a si mismo recursivamente
/// Una vez que arribo, envia el mensaje Endride a si mismo
#[async_handler]
impl Handler<RideToDestination> for Driver {
    type Result = ();

    async fn handle(&mut self, _msg: RideToDestination, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::RideToDestination");

        actix::clock::sleep(Duration::from_millis(500)).await;

        Driver::save_state(
            self.name.clone(),
            self.is_available,
            self.position,
            self.id_current_passenger,
            self.passenger_position,
            self.destination,
            self.passenger_picked_up,
            self.is_registered,
            self.driver_socket.clone(),
        )
        .await;

        let Some(destination) = self.destination else {
            error!("Salgo de RideToDestination por falta de destination");
            return;
        };

        self.move_to(destination);

        // Si todavia no llegue al destino, mando RideToDestination
        if self.position != destination {
            if let Err(e) = _ctx.address().try_send(RideToDestination {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
            return;
        }

        info!("Se llegó al destino");

        if let Err(e) = _ctx.address().try_send(EndRide { dst: self.position }) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

/// Dado que finalizo el viaje, actualiza la posicion a la posicion de destino del viaje
/// le notifica al server su nueva posicion y que se encuentra disponible para recibir nuevos pasajeros
/// Verifica que la conexion con el servidor exista, si no existe se reconecta con el nuevo lider
#[async_handler]
impl Handler<EndRide> for Driver {
    type Result = ();

    async fn handle(&mut self, msg: EndRide, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::EndRide");
        info!("Viaje finalizado");

        self.position = msg.dst;
        info!("Posicion actualizada en {:?}", self.position);
        let msg = AvailableDriver {
            name: self.name.clone(),
            position: self.position,
        };

        let serialized_msg = serialize_message("available_driver", msg);

        if let Err(e) = self
            .server_sender
            .try_send(TcpMessage::new(serialized_msg.clone()))
        {
            error!("Error al enviar el mensaje: {}", e);
        }

        // debug!("Duermo esperando si el servidor sigue vivo...");
        // actix::clock::sleep(Duration::from_secs(5)).await;
        // debug!("Despierto...");

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

        self.is_available = true;
        self.passenger_picked_up = false;

        Driver::save_state(
            self.name.clone(),
            self.is_available,
            self.position,
            self.id_current_passenger,
            self.passenger_position,
            self.destination,
            self.passenger_picked_up,
            self.is_registered,
            self.driver_socket.clone(),
        )
        .await;

        info!("Conductor disponible\n");
    }
}

/// Valido si el pasajero que me esta solicitando recuperarse es el actual, en caso que no sea quiere decir que el viaje con ese pasajero culmino
/// En el caso de que si lo sea, le envio la posicion actual donde nos encontramos
impl Handler<PassengerRecover> for Driver {
    type Result = ();

    fn handle(&mut self, msg: PassengerRecover, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::PassengerRecover");

        if self.id_current_passenger == msg.id_passenger as usize {
            self.current_passenger_connection = Some(msg.passenger_connection.clone());
            let end_ride = if let Some(destination) = self.destination {
                self.position == destination
            } else {
                false
            };
            let serialized_msg = serialize_message(
                "recover_response",
                json!({"picked_up": self.passenger_picked_up, "position": self.position, "end_ride": end_ride}),
            );
            if let Err(e) = msg
                .passenger_connection
                .try_send(PassengerConnectionMessage::new(serialized_msg))
            {
                error!("Error al enviar el mensaje: {}", e);
            }
        } else {
            let serialized_msg = serialize_message(
                "recover_response",
                json!({"picked_up": false, "position": self.position, "end_ride": true}),
            );
            if let Err(e) = msg
                .passenger_connection
                .try_send(PassengerConnectionMessage::new(serialized_msg))
            {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
    }
}

/// Restauro los estados de las variables del driver segun quedo grabado en el archivo
/// Le informo al servidor mi nueva ip
/// Luego si ya recogi al pasajero continuo con el viaje y sino lo paso a buscar
#[async_handler]
impl Handler<Recovery> for Driver {
    type Result = ();

    async fn handle(&mut self, msg: Recovery, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::Recovery");

        if let Some(destination) = msg.status["destination"].as_array() {
            if let (Some(x), Some(y)) = (destination[0].as_u64(), destination[1].as_u64()) {
                self.destination = Some((x, y));
            }
        }

        if let Some(is_available) = msg.status["is_available"].as_bool() {
            self.is_available = is_available;
        }
        if let Some(is_registered) = msg.status["is_registered"].as_bool() {
            self.is_registered = is_registered;
        }
        if let Some(id_current_passenger) = msg.status["id_current_passenger"].as_u64() {
            self.id_current_passenger = id_current_passenger as usize;
        }

        if let Some(passenger_picked_up) = msg.status["passenger_picked_up"].as_bool() {
            self.passenger_picked_up = passenger_picked_up;
        }

        if let Some(passenger_position) = msg.status["passenger_position"].as_array() {
            if let (Some(x), Some(y)) = (
                passenger_position[0].as_u64(),
                passenger_position[1].as_u64(),
            ) {
                self.passenger_position = Some((x, y));
            }
        }

        if let Some(position) = msg.status["position"].as_array() {
            if let (Some(x), Some(y)) = (position[0].as_u64(), position[1].as_u64()) {
                self.position = (x, y);
            }
        }

        debug!("Se leyo el estado anterior");
        debug!("Pasajero destino: {:?}", self.destination);
        debug!("Disponible: {:?}", self.is_available);
        debug!("ID pasajero: {:?}", self.id_current_passenger);
        debug!("Pasajero levantado: {:?}", self.passenger_picked_up);
        debug!("Pasajero posicion: {:?}", self.passenger_position);
        debug!("Posicion: {:?}\n", self.position);
        debug!("Esta registrado? {:?}\n", self.is_registered);

        let id = self.id_current_passenger;
        let name = self.name.clone();

        // Si no estoy disponible y...
        if !self.is_available {
            let serialized_msg = serialize_message(
                "reconnect_driver",
                json!({"id_passenger": id, "socket": msg.socket.clone(), "name": name}),
            );

            if let Err(e) = self.server_sender.try_send(TcpMessage::new(serialized_msg)) {
                error!("Error al enviar el mensaje: {}", e);
            }

            // ...si ya levante al pasajero, lo llevo a su destino
            if self.passenger_picked_up {
                if let Err(e) = _ctx.address().try_send(RideToDestination {}) {
                    error!("Error al enviar el mensaje: {}", e);
                }
            } else {
                // ...si no levante al pasajero, primero voy a buscarlo
                if let Err(e) = _ctx.address().try_send(PickUpPassenger {}) {
                    error!("Error al enviar el mensaje: {}", e);
                }
            }
        }
    }
}

/// Recibo, luego de la caida, la posicion del lado del pasajero, dado que puede que exista una falta de sincronizacion por la caida
impl Handler<ReconnectPassenger> for Driver {
    type Result = ();

    fn handle(&mut self, msg: ReconnectPassenger, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Driver::ReconnectPassenger");
        self.current_passenger_connection = Some(msg.passenger_connection);

        info!("Sincronizando posicion con pasajero");
        debug!(
            "Posicion actual {:?} Posicion nueva {:?}",
            self.position.clone(),
            msg.position.clone()
        );
        self.position = msg.position;
        self.passenger_position = Some(msg.position);
    }
}
