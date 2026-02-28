use crate::connection_listener::ConnectionListener;
use crate::models::common::ReconnectDriver;
use crate::models::common::{
    CheckIfNeighborAcked, ConnectToServer, Election, NeighborAck, NewLeader, Ping, SendLeaderId,
    SetLeader, Stop, WaitForNeighborAck,
};
use crate::sender::TcpSender;
use crate::server_connection::ServerConnection;
use crate::udp_receiver::Listen;
use crate::udp_sender::UdpMessage;
use crate::{
    models::{
        common::{
            NewServerConnection, Pong, StartElection, UpdateDriverSocket, UpdateNetworkState,
        },
        driver::{BusyDriver, DriverInfo, Drivers, GetDrivers, NewDriver, UpdateDriver},
        passenger::PassengerLogin,
    },
    udp_sender::UdpSender,
};
use actix::ActorContext;
use actix::{Actor, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix_async_handler::async_handler;
use colored::Colorize;
use common::constants::{MAX_SERVERS, SERVER_UDP_PORT_RANGE};
use common::utils::id_to_tcp_address;
use common::{serialize::serialize_message, tcp_message::TcpMessage};
use log::{debug, error, info};
use serde_json::json;
use std::collections::HashMap;
use std::io;
use std::time::Duration;
use tokio::io::{split, AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_stream::wrappers::LinesStream;

/// Representa un servidor, el cual puede ser líder o réplica.
pub struct Server {
    /// ID del server
    pub id: u64,
    /// ID del server lider
    pub id_leader: u64,
    /// Booleano indicando si es líder
    pub im_leader: bool,
    /// Drivers disponibles segun su nombre
    pub drivers: HashMap<String, DriverInfo>,
    /// Drivers ocupados segun su nombre
    pub busy_drivers: HashMap<String, DriverInfo>,
    /// ID de pasajeros segun su nombre
    pub passengers: HashMap<String, u64>,
    /// Conexiones con pasajeros segun sus IDs
    pub passengers_address: HashMap<u64, Addr<TcpSender>>,
    /// Sockets de servidores replicas
    pub udp_sockets_replicas: HashMap<u64, String>,
    /// Conexiones con servers segun su ID
    pub servers: HashMap<u64, Addr<TcpSender>>,
    /// Contador de IDs
    pub ids_count: u64,
    /// Sender UDP
    pub udp_sender: Option<Addr<UdpSender>>,
    /// Conexion con el lider. Es None en caso de ser el lider.
    pub leader_connection: Option<Addr<ServerConnection>>,
    /// Listado de servers desconectados
    pub disconnected_servers: Vec<u64>,
    /// Booleano indicando si se recibio el ACK del vecino
    pub received_neighbor_ack: bool,
    /// Booleano indicando si hay una eleccion de lider en curso
    pub election_on_course: bool,
    /// ID del vecino actual
    pub actual_neighbor_id: u64,
    /// Numero de secuencia actual
    pub sequence_number: u64,
    /// Booleano que indica si se recibio ACK para un numero de secuencia
    pub message_received_ack: HashMap<u64, bool>,
}

impl Server {
    /// Actualiza el `actual_neighbor_id`, devuelve true o false dependeindo de si
    /// hay algun vecino conectado o no.
    fn update_neighbor(&mut self, neighbor_id: u64) -> bool {
        if !self.disconnected_servers.contains(&self.actual_neighbor_id) {
            return true;
        }
        self.actual_neighbor_id = get_neighbor_id(neighbor_id);
        let mut count = 0;
        loop {
            if count >= MAX_SERVERS {
                return false;
            }
            if !self.disconnected_servers.contains(&self.actual_neighbor_id)
                && self.actual_neighbor_id != self.id
            {
                break;
            }
            self.actual_neighbor_id = get_neighbor_id(self.actual_neighbor_id);
            count += 1;
        }
        true
    }
}

impl Actor for Server {
    type Context = Context<Self>;
}

/// Luego de que el Driver se haya conectado, se recibe este mensaje para identificarlo como driver
/// En el se recibe el nombre del driver (identificador). En caso de que el driver no este registrado
/// lo registra y lo guarda en la lista de drivers disponibes
impl Handler<NewDriver> for Server {
    type Result = ();

    fn handle(&mut self, msg: NewDriver, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::NewDriver");

        if let Some(driver) = self.drivers.get(&msg.name) {
            info!("El conductor '{}' existe con ID {}\n", msg.name, driver.id);
            if let Err(e) = _ctx.address().try_send(UpdateDriverSocket {
                name: msg.name.clone(),
                socket: msg.socket.clone(),
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }
            return;
        }

        if let Some(driver) = self.busy_drivers.get(&msg.name) {
            info!(
                "El conductor '{}' existe con ID {} y esta ocupado\n",
                msg.name, driver.id
            );
            if let Err(e) = _ctx.address().try_send(UpdateDriverSocket {
                name: msg.name.clone(),
                socket: msg.socket.clone(),
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }

            return;
        }

        self.ids_count += 1;
        let id = self.ids_count;

        let driver_info = DriverInfo {
            socket: msg.socket,
            position: msg.position,
            name: msg.name.clone(),
            id,
        };

        self.drivers.insert(msg.name.clone(), driver_info); //si no existe creo el conductor

        info!("Nuevo conductor {} con ID {}\n", msg.name, id);

        // debug!("{:?}", &self.drivers.get(&msg.name));
    }
}

impl Handler<GetDrivers> for Server {
    type Result = ();
    /// Envia al pasajero un listado con los conductores disponibles.
    fn handle(&mut self, msg: GetDrivers, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::GetDrivers");
        debug!("Sent TcpMessage to TcpSender\n");
        let drivers = Drivers {
            drivers_info: self.drivers.values().cloned().collect(),
        };
        let serialized_msg = serialize_message("drivers", drivers);
        if let Err(e) = msg.addr.try_send(TcpMessage::new(serialized_msg)) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

impl Handler<PassengerLogin> for Server {
    type Result = ();

    /// Maneja el login de un pasajero. Se guarda su conexion, y le asigna un ID
    /// en caso de que no se haya logeado antes.
    fn handle(&mut self, msg: PassengerLogin, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::PassengerLogin");
        debug!("Sent TcpMessage to TcpSender");

        if let Some(id) = self.passengers.get(&msg.name) {
            info!("El pasajero '{}' existe con ID {}\n", msg.name, id);
            self.passengers_address.insert(*id, msg.addr.clone());

            let serialized_msg = serialize_message("identificador", json!({"id": id}));
            if let Err(e) = msg
                .addr
                .try_send(TcpMessage(format!("{}\n", serialized_msg)))
            {
                error!("Error al enviar el mensaje: {}", e);
            }
        } else {
            self.ids_count += 1;
            let id = self.ids_count;

            self.passengers.insert(msg.name.clone(), id); //si no existe creo el pasajero
            self.passengers_address.insert(id, msg.addr.clone());

            info!("Nuevo pasajero {} con ID {}\n", msg.name, id);

            let serialized_msg = serialize_message("identificador", json!({"id": id}));
            if let Err(e) = msg
                .addr
                .try_send(TcpMessage(format!("{}\n", serialized_msg)))
            {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
    }
}

impl Handler<UpdateDriver> for Server {
    type Result = ();

    /// Actualiza la posicion del conductor.
    /// El conductor envia este mensaje luego de terminar un viaje.
    fn handle(&mut self, msg: UpdateDriver, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::UpdatePosition");
        // Muevo el conductor al hashmap de conductores disponibles
        let Some(mut driver) = self.busy_drivers.remove(&msg.name) else {
            return;
        };
        let previous_position = driver.position;
        driver.position = msg.new_position;
        self.drivers.insert(msg.name.clone(), driver);

        info!(
            "{}",
            format!(
                "El conductor {} se movio de {:?} a {:?}\n",
                msg.name, previous_position, msg.new_position
            )
            .green()
        );
    }
}

impl Handler<BusyDriver> for Server {
    type Result = ();

    /// Marca como ocupado a el conductor indicado por msg.name.
    /// El conductor manda este mensaje al iniciar un viaje.
    fn handle(&mut self, msg: BusyDriver, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::BusyDriver");
        // Muevo el conductor al hashmap de conductores ocupados
        let Some(driver) = self.drivers.remove(&msg.name) else {
            return;
        };
        self.busy_drivers.insert(msg.name, driver);
    }
}

impl Handler<NewServerConnection> for Server {
    type Result = ();

    /// Este mensaje se envia cuando una replica se conecta al servidor lider.
    /// Si la replica estaba desconectada, se la elimina de la lista de desconectados
    /// y se guarda la conexion.
    /// Tambien se envia el estado a todas las replicas.
    fn handle(&mut self, msg: NewServerConnection, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::NewServerConnection");

        info!("Nuevo servidor replica con ID {}", msg.id);

        if self.disconnected_servers.contains(&msg.id) {
            debug!("Entro a eliminar un ID repetido");
            self.disconnected_servers.retain(|&x| x != msg.id);
            debug!("{:?}", self.disconnected_servers);
        }

        // Insertar el servidor replica en el diccionario
        self.servers.insert(msg.id, msg.addr.clone());
        self.udp_sockets_replicas.insert(msg.id, msg.socket.clone());

        for addr_replica in self.servers.values() {
            info!("{}", "Enviando pong a TODAS las replicas\n".green());
            if let Err(e) = _ctx.address().try_send(Pong {
                addr: addr_replica.clone(),
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
    }
}

#[async_handler]
impl Handler<Ping> for Server {
    type Result = Result<(), String>;

    /// Envia ping al servidor lider.
    ///
    /// Si el servidor lider esta desconectado (conexion es None), no envia nada.
    /// Si soy lider, no envio nada.
    /// Antes de enviar el ping, chequeo si el lider esta conectado.
    /// Si no esta conectado, marco la conexion como None desato el algoritmo de eleccion.
    async fn handle(&mut self, _msg: Ping, _ctx: &mut Self::Context) -> Self::Result {
        // Mando ping al lider solo si no soy el lider
        if self.leader_connection.is_none() {
            return Ok(());
        }
        if !self.im_leader {
            debug!("Server::Ping");
            if let Some(leader_connection) = self.leader_connection.clone() {
                let leader_connected = leader_connection.connected();
                if !leader_connected {
                    info!("Lider {} desconectado", self.id_leader);
                    self.leader_connection = None;
                    self.election_on_course = true;
                    if let Err(e) = _ctx.address().try_send(StartElection {}) {
                        error!("Error al enviar el mensaje: {}", e);
                    }

                    return Err(String::from("Lider desconectado"));
                }
                if let Some(leader_connection) = self.leader_connection.as_ref() {
                    leader_connection
                        .try_send(Ping {})
                        .map_err(|e| e.to_string())?;
                }
                info!("Ping sale bien");
            }
        }
        Ok(())
    }
}

impl Handler<Pong> for Server {
    type Result = ();

    /// Envia Pong a los servidores replicas.
    /// El Pong contiene todo el estado de la red.
    fn handle(&mut self, msg: Pong, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::Pong");

        debug!("Enviando actualizacion de estado...");
        debug!("Contador de ids: {:?}", self.ids_count);
        debug!("Servidores: {:?}", self.udp_sockets_replicas);
        debug!("Servers desconectados: {:?}", self.disconnected_servers);
        debug!("Conductores: {:?}", self.drivers);
        debug!("Conductores ocupados: {:?}", self.busy_drivers);
        debug!("Pasajeros: {:?}", self.passengers);

        let network_state = UpdateNetworkState {
            ids_count: self.ids_count,
            drivers: self.drivers.clone(),
            busy_drivers: self.busy_drivers.clone(),
            passengers: self.passengers.clone(),
            udp_sockets_replicas: self.udp_sockets_replicas.clone(),
            disconnected_servers: self.disconnected_servers.clone(),
        };

        let serialized_msg = serialize_message("pong", network_state);
        info!("{}", "Enviando pong a una replica\n".green());
        if let Err(e) = msg.addr.try_send(TcpMessage::new(serialized_msg)) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

impl Handler<UpdateNetworkState> for Server {
    type Result = ();

    /// Recibe el estado de la red, y actualiza su estado local.
    fn handle(&mut self, msg: UpdateNetworkState, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::UpdateNetworkState");

        self.drivers = msg.drivers;
        self.busy_drivers = msg.busy_drivers;
        self.passengers = msg.passengers;
        self.udp_sockets_replicas = msg.udp_sockets_replicas;
        self.ids_count = msg.ids_count;
        self.disconnected_servers = msg.disconnected_servers;

        debug!("Actualizacion de estado...");
        debug!("Contador de ids: {:?}", self.ids_count);
        debug!("Servidores: {:?}", self.udp_sockets_replicas);
        debug!("Servers desconectados: {:?}", self.disconnected_servers);
        debug!("Conductores: {:?}", self.drivers);
        debug!("Conductores ocupados: {:?}", self.busy_drivers);
        debug!("Pasajeros: {:?}\n", self.passengers);
    }
}

impl Handler<SetLeader> for Server {
    type Result = ();

    /// Setea la conexion con el servidor lider.
    fn handle(&mut self, msg: SetLeader, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::SetLeader\n");
        self.id_leader = msg.id_leader;
        self.leader_connection = Some(msg.leader_connection);
    }
}

/// Devuelve IP y puerto UDP de un server segun el id
pub fn get_address(id: u64) -> String {
    let port = SERVER_UDP_PORT_RANGE + id;

    let address = format!("127.0.0.1:{port}");
    address
}

/// Recibe el id de un server y devuelve el id del vecino de ese server.
pub fn get_neighbor_id(id: u64) -> u64 {
    if id == MAX_SERVERS {
        1
    } else {
        id + 1
    }
}

impl Handler<StartElection> for Server {
    type Result = ();

    /// Desata una elección de líder.
    ///
    /// Marca el lider actual como desconectado, envia el mensaje Election a una replica
    /// vecina, y espera por el ACK.
    fn handle(&mut self, _msg: StartElection, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::StartElection");
        info!(
            "Marcando server {} como desconectado por StartElection",
            self.id_leader
        );
        self.disconnected_servers.push(self.id_leader);

        let neighbors_connected = self.update_neighbor(self.actual_neighbor_id);
        if !neighbors_connected {
            if let Err(e) = _ctx.address().try_send(NewLeader {
                id_sender: self.id,
                leader_id: self.id,
                sequence_number: self.sequence_number,
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }
            self.sequence_number += 1;
            return;
        }

        let neighbor_address = get_address(self.actual_neighbor_id);

        println!("ID del lider: {:?}", self.id_leader);
        println!("ID del vecino: {:?}", self.actual_neighbor_id);
        println!("Address del vecino: {:?}", neighbor_address);

        debug!(
            "Soy la replica de ID {} mando election a {}\n",
            self.id, neighbor_address
        );

        let server_ids = vec![self.id];
        let election_message = Election {
            server_ids,
            disconnected_leader_id: self.id_leader,
            sequence_number: self.sequence_number,
        };
        self.sequence_number += 1;
        let serialized_msg = serialize_message("election", election_message.clone());
        self.sequence_number += 1;
        if let Some(udp_sender) = self.udp_sender.as_ref() {
            if let Err(e) = udp_sender.try_send(UdpMessage {
                message: serialized_msg.clone(),
                dst: neighbor_address.clone(),
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
        if let Err(e) = _ctx.address().try_send(WaitForNeighborAck {
            neighbor_id: self.actual_neighbor_id,
            serialized_msg,
            sequence_number: election_message.sequence_number,
        }) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

#[async_handler]
impl Handler<WaitForNeighborAck> for Server {
    type Result = ();
    /// Duerme unos segundos para esperar por el ACK del vecino, y luego
    /// se autoenvia CheckIfNeighborAcked.
    ///
    /// El ACK del vecino lo recibe UdpReceiver, que luego le manda un
    /// mensaje NeighborAck al Server.
    async fn handle(&mut self, msg: WaitForNeighborAck, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Esperando ACK del vecino...");
        actix::clock::sleep(Duration::from_millis(300)).await;
        if let Err(e) = _ctx.address().try_send(CheckIfNeighborAcked {
            neighbor_id: msg.neighbor_id,
            serialized_msg: msg.serialized_msg,
            sequence_number: msg.sequence_number,
        }) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

#[async_handler]
impl Handler<CheckIfNeighborAcked> for Server {
    type Result = ();
    /// Recibe un numero de secuencia de un mensaje que fue enviado a un vecino,
    /// y se fija si ya se recibió el ACK para ese mensaje.
    ///
    /// En caso de no haber recibido el ACK, se reenvia el mensaje original
    /// (puede ser Election, NewLeader, etc) al siguiente vecino.
    async fn handle(
        &mut self,
        msg: CheckIfNeighborAcked,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        // Si el vecino respondió, no hago nada
        if self.message_received_ack.contains_key(&msg.sequence_number) {
            debug!("ACK recibido");
            self.received_neighbor_ack = false;
            // self.message_received_ack.remove(&msg.sequence_number);
            return;
        }
        info!(
            "Marcando server {} como desconectado porque no recibo ACK de {}",
            msg.neighbor_id, msg.serialized_msg
        );
        self.disconnected_servers.push(msg.neighbor_id);
        // Si no respondió asumo que está caído y le mando el mensaje a su vecino
        let neighbors_connected = self.update_neighbor(self.actual_neighbor_id);
        if !neighbors_connected {
            info!("No hay replicas conectadas, soy el lider");
            // Me mando NewLeader a mi mismo
            if let Err(e) = _ctx.address().try_send(NewLeader {
                id_sender: self.id,
                leader_id: self.id,
                sequence_number: self.sequence_number,
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }
            self.sequence_number += 1;
            return;
        }
        debug!(
            "No se recibio ACK de {} de parte de id {}, reenviando a id {}",
            msg.serialized_msg, msg.neighbor_id, self.actual_neighbor_id
        );
        let next_neighbor_address = get_address(self.actual_neighbor_id);
        // Reenvio el mensaje original a el siguiente vecino
        if let Some(udp_sender) = self.udp_sender.as_ref() {
            if let Err(e) = udp_sender.try_send(UdpMessage {
                message: msg.serialized_msg.clone(),
                dst: next_neighbor_address,
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
        // Espero el ACK del siguiente vecino
        if let Err(e) = _ctx.address().try_send(WaitForNeighborAck {
            serialized_msg: msg.serialized_msg,
            neighbor_id: self.actual_neighbor_id,
            sequence_number: msg.sequence_number,
        }) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

impl Handler<Election> for Server {
    type Result = ();

    /// Al recibir el mensaje Election, marco el lider actual como desconectado.
    ///
    /// Si no hay replicas conectadas, me autodeclaro lider.
    ///
    /// Si hay replicas conectadas, me fijo si la elección de líder termino, chequeando
    /// si mi ID esta en la lista. Si la elección terminó, envio el mensaje NewLeader a mi
    /// vecino indicando el nuevo líder, y espero el ACK.
    ///
    /// Si la elección no terminó, agrego mi ID a la lista de IDs de Election y reenvío el mensaje
    /// a mi vecino y espero el ACK.
    fn handle(&mut self, msg: Election, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::Election");
        info!(
            "Marcando server {} como desconectado pq me llega de election",
            msg.disconnected_leader_id
        );
        // Marco el server lider como desconectado
        self.disconnected_servers.push(msg.disconnected_leader_id);
        // Si el lider actual es el que se cayo, seteo la conexion como None
        if self.id_leader == msg.disconnected_leader_id {
            self.leader_connection = None;
        }
        // actualizo mi siguiente vecino
        let neighbors_connected = self.update_neighbor(self.actual_neighbor_id);
        // Si no hay replicas conectadas entonces soy el lider
        if !neighbors_connected {
            info!("No hay replicas conectadas, soy el lider");
            if let Err(e) = _ctx.address().try_send(NewLeader {
                id_sender: self.id,
                leader_id: self.id,
                sequence_number: self.sequence_number,
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }
            self.sequence_number += 1;
            return;
        }

        let neighbor_address = get_address(self.actual_neighbor_id);

        println!("ID del lider: {:?}", self.id_leader);
        println!("ID del vecino: {:?}", self.actual_neighbor_id);
        println!("Address del vecino: {:?}", neighbor_address);

        // Si mi id esta en el mensaje election es porque yo lo envie y me llego de vuelta.
        // Es decir, terminó el algoritmo
        let election_finished = msg.server_ids.contains(&self.id);
        if election_finished {
            let mut leader_id = 0;
            // Determino el lider buscando el mayor id en la lista de ids
            for id in msg.server_ids {
                if id > leader_id {
                    leader_id = id;
                }
            }
            info!("Nuevo lider elegido: {leader_id}\n");
            self.id_leader = leader_id;
            let new_leader = NewLeader {
                leader_id,
                id_sender: self.id,
                sequence_number: self.sequence_number,
            };
            self.sequence_number += 1;
            let serialized_msg = serialize_message("new_leader", new_leader.clone());
            // Le mando el mensaje new_leader a mi vecino
            if let Err(e) = self
                .udp_sender
                .as_ref()
                .expect("Error al intentar mandar un mensaje UDP por el UdpSender")
                .try_send(UdpMessage {
                    message: serialized_msg.clone(),
                    dst: neighbor_address,
                })
            {
                error!("Error al enviar el mensaje: {}", e);
            }
            // Espero por el ACK de mi vecino
            if let Err(e) = _ctx.address().try_send(WaitForNeighborAck {
                neighbor_id: self.actual_neighbor_id,
                serialized_msg,
                sequence_number: new_leader.sequence_number,
            }) {
                error!("Error al enviar el mensaje: {}", e);
            }
            return;
        }
        // Si la eleccion no termino, reenvio el mensaje Election a mi vecino
        debug!(
            "Soy la replica de ID {} mando election a {}\n",
            self.id, neighbor_address
        );
        // Agrego mi ID a la lista de IDs
        let mut server_ids = msg.server_ids;
        server_ids.push(self.id);
        let election_msg = Election {
            server_ids,
            disconnected_leader_id: msg.disconnected_leader_id,
            sequence_number: self.sequence_number,
        };
        self.sequence_number += 1;
        let serialized_msg = serialize_message("election", election_msg.clone());

        // Mando Election a mi vecino
        if let Err(e) = self
            .udp_sender
            .as_ref()
            .expect("UdpSender es None en Server")
            .try_send(UdpMessage {
                message: serialized_msg.clone(),
                dst: neighbor_address,
            })
        {
            error!("Error al enviar el mensaje: {}", e);
        }
        // Espero el ACK de mi vecino
        if let Err(e) = _ctx.address().try_send(WaitForNeighborAck {
            neighbor_id: self.actual_neighbor_id,
            serialized_msg,
            sequence_number: election_msg.sequence_number,
        }) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

#[async_handler]
impl Handler<NewLeader> for Server {
    type Result = ();

    /// Al recibir NewLeader, piso el ID del lider anterior con el nuevo.
    ///
    /// Me fijo si soy el nuevo lider, en tal caso, me pongo a escuchar conexiones.
    ///
    /// Si el ID del sender del mensaje es igual a mi ID, quiere decir que el mensaje
    /// recorrió todo el anillo, en tal caso no lo reenvio.
    ///
    /// Si no soy el sender del mensaje, lo reenvio y espero el ACK.
    ///
    /// Si no soy el lider, me conecto al nuevo lider.
    async fn handle(&mut self, msg: NewLeader, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::NewLeader");

        self.id_leader = msg.leader_id;

        // Si soy el lider y antes no era lider, entonces me pongo a escuchar conexiones
        if msg.leader_id == self.id && !self.im_leader {
            info!("Soy el lider");
            self.im_leader = true;
            // Escucho conexiones
            let connection_listener = ConnectionListener {
                socket: id_to_tcp_address(self.id),
                server_addr: _ctx.address(),
            }
            .start();
            if let Err(e) = connection_listener.try_send(Listen {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
        // Reenvio el mensaje solo si no soy el sender original
        if msg.id_sender != self.id {
            println!("Reenvio new_leader");
            let neighbors_connected = self.update_neighbor(self.actual_neighbor_id);
            // Si no hay replicas conectadas entonces soy el lider
            if !neighbors_connected {
                info!("No hay replicas conectadas, soy el lider");
                if let Err(e) = _ctx.address().try_send(NewLeader {
                    id_sender: self.id,
                    leader_id: self.id,
                    sequence_number: self.sequence_number,
                }) {
                    error!("Error al enviar el mensaje: {}", e);
                }
                self.sequence_number += 1;
            // Si hay replicas conectadas entonces reenvio el mensaje NewLeader a mi vecino
            } else {
                let neighbor_address = get_address(self.actual_neighbor_id);

                let election_message = serialize_message("new_leader", msg.clone());
                if let Err(e) = self
                    .udp_sender
                    .as_ref()
                    .expect("UdpSender no esta seteado en Server")
                    .try_send(UdpMessage {
                        message: election_message.clone(),
                        dst: neighbor_address,
                    })
                {
                    error!("Error al enviar el mensaje: {}", e);
                }
                // Espero el ACK de mi vecino
                if let Err(e) = _ctx.address().try_send(WaitForNeighborAck {
                    neighbor_id: self.actual_neighbor_id,
                    serialized_msg: election_message,
                    sequence_number: msg.sequence_number,
                }) {
                    error!("Error al enviar el mensaje: {}", e);
                }
            }
        }
        // Si no soy lider, me conecto al lider
        if msg.leader_id != self.id && self.leader_connection.is_none() {
            if let Err(e) = _ctx.address().try_send(ConnectToServer {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
    }
}

#[async_handler]
impl Handler<NeighborAck> for Server {
    type Result = ();

    /// Marca un numero de secuencia como ACKed.
    async fn handle(&mut self, msg: NeighborAck, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::NeighborAck");
        // saco al server de disconnected
        self.disconnected_servers.retain(|id| *id != msg.id);
        // marco que recibi el ACK para el sequence number
        self.message_received_ack.insert(msg.sequence_number, true);
        debug!("Received {} ACK from id {}", msg.message, msg.id)
    }
}

/// Crea una nueva conexión TCP.
///
/// En caso de error, reintenta 5 veces.
///
/// # Argumentos
///
/// * `socket` - La dirección TCP a la cual se debe conectar.
///
/// # Devuelve
///
/// La conexión TCP.
///
/// # Errores
///
/// Luego de reintentar la conexión 5 veces, devuelve error.
async fn create_tcp_connection(socket: String) -> Result<TcpStream, io::Error> {
    info!("create_tcp_connection");
    let mut connection_result = TcpStream::connect(socket.clone()).await;
    let mut count = 0;
    while connection_result.is_err() {
        info!("Fallo la conexion al nuevo lider, reintentando");
        actix::clock::sleep(Duration::from_millis(100)).await;
        if count == 5 {
            return connection_result;
        }
        connection_result = TcpStream::connect(socket.clone()).await;
        count += 1;
    }
    let stream_server_r = connection_result.expect("");
    Ok(stream_server_r)
}

#[async_handler]
impl Handler<ConnectToServer> for Server {
    type Result = ();
    /// Se conecta al servidor lider
    async fn handle(&mut self, _msg: ConnectToServer, _ctx: &mut Self::Context) -> Self::Result {
        println!("*************************\n\nServer::ConnectToServer\n\n***************************************");

        actix::clock::sleep(Duration::from_millis(100)).await;
        info!("Conectandome al server lider!");
        let leader_address = id_to_tcp_address(self.id_leader);
        // Intento conectarme al lider varias veces
        let stream_server_r = create_tcp_connection(leader_address).await;
        if stream_server_r.is_err() {
            error!("Fallo al conectarse al lider.");
            return;
        }
        let stream_server_r = stream_server_r.expect("No deberia fallar");
        info!("{}", "Conectado al servidor lider\n".green());

        let (read_server_r, write_half_server_r) = split(stream_server_r);

        // Inicializacion del server
        let server_connection = ServerConnection::create(|ctx| {
            ServerConnection::add_stream(
                LinesStream::new(BufReader::new(read_server_r).lines()),
                ctx,
            );
            let write = Some(write_half_server_r);
            ServerConnection {
                addr: _ctx.address(),
                write,
                response_time: Instant::now(),
            }
        });

        info!("Reconectado al nuevo lider");
        self.leader_connection = Some(server_connection);
        self.election_on_course = false;
    }
}

impl Handler<Stop> for Server {
    type Result = ();
    /// Este mensaje se usa para detener los servers desde el test
    fn handle(&mut self, _msg: Stop, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::Stop");
        _ctx.stop();
    }
}

impl Handler<SendLeaderId> for Server {
    type Result = ();
    /// Este mensaje se usa para obtener el ID del lider desde el test
    fn handle(&mut self, msg: SendLeaderId, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::SendLeaderId");
        let message = self.id_leader.to_string();
        if let Err(e) = self
            .udp_sender
            .as_ref()
            .expect("UdpSender no esta seteado en Server")
            .try_send(UdpMessage {
                message,
                dst: msg.addr,
            })
        {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

impl Handler<ReconnectDriver> for Server {
    type Result = ();

    /// Le envia un mensaje al pasajero dado por msg.id_passenger para que
    /// se reconecte al conductor que se habia caido previamente.
    fn handle(&mut self, msg: ReconnectDriver, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::ReconnectDriver\n");

        if let Err(e) = _ctx.address().try_send(UpdateDriverSocket {
            name: msg.name.clone(),
            socket: msg.socket.clone(),
        }) {
            error!("Error al enviar el mensaje UpdateDriverSocket: {}", e);
        }

        match self.passengers_address.get(&msg.id_passenger) {
            Some(passengers_address) => {
                let serialized_msg =
                    serialize_message("reconnect_driver", json!({"socket": msg.socket}));
                if let Err(e) = passengers_address.try_send(TcpMessage::new(serialized_msg)) {
                    error!("Error al enviar el mensaje \"reconnect_driver\": {}", e);
                }
            }
            _ => {
                println!(
                    "No se encontró ninguna dirección para el ID {}",
                    msg.id_passenger
                );
            }
        }
    }
}

impl Handler<UpdateDriverSocket> for Server {
    type Result = ();

    /// Actualiza el socket del conductor.
    fn handle(&mut self, msg: UpdateDriverSocket, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Server::UpdateDriverSocket\n");

        debug!("Actualizando Socket driver: {}", msg.name.clone());

        if let Some(driver_info) = self.drivers.get_mut(&msg.name.clone()) {
            debug!("Viejo Socket: {}", driver_info.socket.clone());
            debug!("Nuevo Socket: {}", msg.socket.clone());
            driver_info.socket = msg.socket.clone();
        }

        if let Some(driver_info) = self.busy_drivers.get_mut(&msg.name.clone()) {
            debug!("Viejo Socket: {}", driver_info.socket.clone());
            debug!("Nuevo Socket: {}", msg.socket.clone());
            driver_info.socket = msg.socket;
        }
    }
}
