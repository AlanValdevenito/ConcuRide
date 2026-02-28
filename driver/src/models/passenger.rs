use actix::Message;
use serde::Serialize;

#[derive(Message, Serialize)]
#[rtype(result = "()")]
pub struct PassengerConnectionMessage(pub String);

impl PassengerConnectionMessage {
    pub fn new(message: String) -> Self {
        Self(format!("{message}\n"))
    }
}
