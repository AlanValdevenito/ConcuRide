use crate::sender::TcpSender;
use actix::Addr;
use actix::Message;

/// Mensaje que indica el logeo de un pasajero
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct PassengerLogin {
    /// Nombre del pasajero
    pub name: String,
    /// Address del actor del TcpSender del pasajero
    pub addr: Addr<TcpSender>,
}
