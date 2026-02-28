use crate::connection::TcpConnection;
use actix::Addr;
use actix::Message;

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct PassengerLogin {
    pub name: String,
    pub addr: Addr<TcpConnection>,
}
