use actix::{Addr, Message};
use serde::{Deserialize, Serialize};

use crate::passenger::Passenger;

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct ApprovePayment {}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct AuthorizePayment {}

#[derive(Message, Debug, Serialize)]
#[rtype(result = "()")]
pub struct PaymentDeny {}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct SetPassenger {
    // Direccion del actor del pasajero
    pub passenger: Addr<Passenger>,
}

#[derive(Message, Debug, Deserialize)]
#[rtype(result = "()")]
pub struct Login {}
