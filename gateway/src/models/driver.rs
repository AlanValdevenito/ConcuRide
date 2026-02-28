use actix::Message;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct DriverInfo {
    pub name: String,
    pub socket: String,
    pub id: u64,
    pub position: (u64, u64),
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct Drivers {
    pub drivers_info: Vec<DriverInfo>,
}
