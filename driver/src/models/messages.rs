use actix::{Addr, Message};
use serde::Serialize;
use serde_json::Value;

use crate::{passenger_connection::PassengerConnection, receiver::TcpReceiver};

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct Register {
    pub socket: String,
    pub name: String,
    pub position: (u64, u64),
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct NewConnection {
    pub addr: Addr<PassengerConnection>,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct RequestRide {
    pub id_passenger: usize,
    pub origin: (u64, u64),
    pub destination: (u64, u64),
    pub addr: Addr<PassengerConnection>,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct EndRide {
    pub dst: (u64, u64),
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct AvailableDriver {
    pub name: String,
    pub position: (u64, u64),
}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct BusyDriver {
    pub name: String,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct StartRide {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct PickUpPassenger {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct RideToDestination {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct PassengerRecover {
    pub id_passenger: u64,
    pub passenger_connection: Addr<PassengerConnection>,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct SetReceiver {
    pub receiver: Addr<TcpReceiver>,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Reconnect {
    pub serialized_msg: String,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Recovery {
    pub status: Value,
    pub socket: String,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct ReconnectPassenger {
    pub passenger_connection: Addr<PassengerConnection>,
    pub position: (u64, u64),
}
