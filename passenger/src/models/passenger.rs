use actix::{Addr, Message};
use serde::Serialize;
use serde_json::Value;

use crate::receiver::TcpReceiver;

use super::driver::DriverInfo;

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct Login {
    /// Nombre del pasajero
    pub name: String,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct GetDrivers {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Drivers {
    // Informacion de los conductores
    pub drivers_info: Vec<DriverInfo>,
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct RequestRide {
    pub id: usize,
    pub origin: (u64, u64),
    pub destination: (u64, u64),
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Traveling {}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct CompleteLogin {
    /// ID del pasajero asignado por el servidor
    pub id: usize,
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct RequestDrive {}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct RideAccepted {
    pub driver_socket: String,
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct KeepLooking {}

#[derive(Serialize, Clone)]
pub struct PassengerInfo {
    /// ID del pasajero asignado por el servidor
    pub id: usize,
    /// Posicion origen
    pub position: (u64, u64),
    /// Posicion destino
    pub destination: (u64, u64),
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct PaymentAuthorized {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct RecoverResponse {
    /// Booleano que indica si el conductor levanto al pasajero
    pub picked_up: bool,
    /// Posicion actual del conductor
    pub position: (u64, u64),
    /// Booleano que indica si el viaje finalizo
    pub end_ride: bool,
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct DriverArrived {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Reconnect {
    /// Mensaje serializado que debe reenviarse
    pub serialized_msg: String,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct SetReceiver {
     /// Comunicacion con el servidor. Direccion del actor TcpReceiver.
    pub receiver: Addr<TcpReceiver>,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct ReconnectDriver {
    /// Socket del conductor caido
    pub socket: String,
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct ConfirmPayment {
    pub name: String,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Recover {
    /// Estado leìdo del archivo corrrespondiente
    pub status: Value,
}
