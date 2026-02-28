use actix::Message;

#[derive(Message)]
#[rtype(result = "()")]
pub struct TcpMessage(pub String);

impl TcpMessage {
    pub fn new(msg: String) -> Self {
        Self(format!("{msg}\n"))
    }
}
