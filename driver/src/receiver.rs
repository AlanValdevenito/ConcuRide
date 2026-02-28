use crate::driver::Driver;
use actix::{Actor, Addr, Context, Message, StreamHandler};
use log::debug;
use std::net::SocketAddr;

pub struct TcpReceiver {
    pub addr: SocketAddr,
    pub client_addr: Addr<Driver>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct TcpMessage(pub String);

impl Actor for TcpReceiver {
    type Context = Context<Self>;
}

/// TcpReceiver solo representa la conexion del driver con el server
impl StreamHandler<Result<String, std::io::Error>> for TcpReceiver {
    fn handle(&mut self, _read: Result<String, std::io::Error>, _ctx: &mut Self::Context) {
        debug!("TcpReceiver::StreamHandler");
    }
}
