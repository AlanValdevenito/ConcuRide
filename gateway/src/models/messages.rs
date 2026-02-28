use actix::Message;
use serde::{Deserialize, Serialize};

#[derive(Message, Debug, Deserialize)]
#[rtype(result = "()")]
pub struct Login {}

#[derive(Message, Debug, Deserialize, Serialize)]
#[rtype(result = "()")]
pub struct PaymentAuthorizationStatus {
    pub is_authorized: bool,
}
