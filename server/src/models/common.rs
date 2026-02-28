use crate::models::driver::DriverInfo;
use crate::sender::TcpSender;
use crate::server_connection::ServerConnection;
use actix::Addr;
use actix::Message;
use serde::Serialize;
use std::collections::HashMap;

pub const MAX_RESPONSE_TIME: u64 = 5;

/// Mensaje enviado al servidor cuando se recibe una nueva conexión.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct NewServerConnection {
    /// Address del actor TcpSender
    pub addr: Addr<TcpSender>,
    /// Socket de la conexión (IP y puerto).
    pub socket: String,
    /// Id de la conexión.
    pub id: u64,
}

/// Este mensaje se utiliza para verificar que el servidor
/// lider siga conectado.
#[derive(Message, Debug)]
#[rtype(result = "Result<(), String>")]
pub struct Ping {}

/// Este mensaje sirve como respuesta a Ping.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Pong {
    pub addr: Addr<TcpSender>,
}

/// El servidor lider envia este mensaje a los servidores réplica
/// luego de recibir Ping. Contiene todo el estado de la red, es decir,
/// información de los pasajeros y conductores.
#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct UpdateNetworkState {
    /// Contador de ids
    pub ids_count: u64,
    /// Un hashmap con los conductores disponibles.
    pub drivers: HashMap<String, DriverInfo>,
    /// Un hashmap con los conductores ocupados.
    pub busy_drivers: HashMap<String, DriverInfo>,
    /// Un hashmap que mapea el nombre del pasajero a un id
    pub passengers: HashMap<String, u64>,
    /// Sockets UDP de servidores replica segun el id
    pub udp_sockets_replicas: HashMap<u64, String>,
    /// Vector con los ids de los servidores que estan desconectados
    pub disconnected_servers: Vec<u64>,
}

/// Mensaje para indicar que se desató una elección de líder.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct StartElection {}

/// Mensaje para comunicar que hay una elección de líder en curso.
/// Se lo envían las réplicas entre sí.
#[derive(Message, Debug, Serialize, Clone)]
#[rtype(result = "()")]
pub struct Election {
    /// ID del líder que se cayó.
    pub disconnected_leader_id: u64,
    /// Listado de IDs de servidores réplicas conectados
    pub server_ids: Vec<u64>,
    /// Número de secuencia del mensaje.
    pub sequence_number: u64,
}

/// Mensaje para indicar que hay un nuevo líder.
/// Se lo envían las réplicas entre sí.
#[derive(Message, Debug, Serialize, Clone)]
#[rtype(result = "()")]
pub struct NewLeader {
    /// ID del server que determinó el nuevo líder.
    pub id_sender: u64,
    /// ID del nuevo líder.
    pub leader_id: u64,
    /// Número de secuencia.
    pub sequence_number: u64,
}

/// Este mensaje se manda a la réplica cuando se inicia.
/// Se usa para establecer la conexion con el server líder.
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct SetLeader {
    pub id_leader: u64,
    pub leader_connection: Addr<ServerConnection>,
}

/// Mensaje que se envía una réplica a si misma para verificar
/// si la réplica a la cual le mandé un mensaje del algoritmo de
/// elección me respondió un ACK.
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct CheckIfNeighborAcked {
    /// ID de la réplica a quien le envíe un mensaje
    pub neighbor_id: u64,
    /// Mensaje que envié
    pub serialized_msg: String,
    /// Numero de secuencia.
    pub sequence_number: u64,
}

/// Mensaje para esperar por un ACK de una réplica.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct WaitForNeighborAck {
    /// Mensaje que envíe a la réplica
    pub serialized_msg: String,
    /// ID de la réplica
    pub neighbor_id: u64,
    /// Numero de secuencia del mensaje
    pub sequence_number: u64,
}

/// Representa un ACK de una réplica vecina.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct NeighborAck {
    /// ID de la réplica
    pub id: u64,
    /// Mensaje al cual corresponde el ACK
    pub message: String,
    /// Numero de secuencia
    pub sequence_number: u64,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct ConnectToServer {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Stop {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct SendLeaderId {
    pub addr: String,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct ReconnectDriver {
    pub id_passenger: u64,
    pub socket: String,
    pub name: String,
}

#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct UpdateDriverSocket {
    pub socket: String,
    pub name: String,
}
