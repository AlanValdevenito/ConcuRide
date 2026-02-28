use crate::connection::TcpConnection;
use actix::Addr;
use actix::Message;

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct NewConnection {
    pub addr: Addr<TcpConnection>,
}
