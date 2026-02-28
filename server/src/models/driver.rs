use crate::sender::TcpSender;
use actix::Addr;
use actix::Message;
use serde::Deserialize;
use serde::Serialize;

/// Representa la información de un conductor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverInfo {
    pub name: String,
    pub socket: String,
    pub id: u64,
    pub position: (u64, u64),
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct NewDriver {
    pub name: String,
    pub socket: String,
    pub position: (u64, u64),
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct Drivers {
    pub drivers_info: Vec<DriverInfo>,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct UpdateDriver {
    pub new_position: (u64, u64),
    pub name: String,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct GetDrivers {
    pub addr: Addr<TcpSender>,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct BusyDriver {
    pub name: String,
}
