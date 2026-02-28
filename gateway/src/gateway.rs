use crate::connection::TcpConnection;
use crate::models::{common::NewConnection, passenger::PassengerLogin};
use actix::{Actor, Addr, Context, Handler};
use common::{serialize::serialize_message, tcp_message::TcpMessage};
use log::{debug, error, info};
use serde_json::json;
use std::collections::HashMap;

pub struct Gateway {
    pub connections: Vec<Addr<TcpConnection>>,
    pub passengers: HashMap<String, u64>,
    pub ids_count: u64,
}

impl Actor for Gateway {
    type Context = Context<Self>;
}

/// Recibe una nueva conexion de un pasajero, si el pasajero ya existia le envia su id
/// Caso contrario lo almacena y le envia su id
impl Handler<PassengerLogin> for Gateway {
    type Result = ();

    fn handle(&mut self, msg: PassengerLogin, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Gateway::PassengerLogin");
        debug!("Sent TcpMessage to TcpConnection");

        if let Some(value) = self.passengers.get(&msg.name) {
            info!("El pasajero '{}' existe con ID {}\n", msg.name, value);
            let serialized_msg = serialize_message("identificador", json!({"id": value}));

            if let Err(e) = msg
                .addr
                .try_send(TcpMessage(format!("{}\n", serialized_msg)))
            {
                error!("Error al enviar el mensaje: {}", e);
            }
        } else {
            self.ids_count += 1;
            let id = self.ids_count;

            self.passengers.insert(msg.name.clone(), id); //si no existe creo el pasajero

            info!("Nuevo pasajero {} con ID {}\n", msg.name, id);

            let serialized_msg = serialize_message("identificador", json!({"id": id}));

            if let Err(e) = msg
                .addr
                .try_send(TcpMessage(format!("{}\n", serialized_msg)))
            {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
    }
}

/// Recibe una nueva conexion, a priori sin identificar
impl Handler<NewConnection> for Gateway {
    type Result = ();

    fn handle(&mut self, msg: NewConnection, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Gateway::NewConnection\n");
        self.connections.push(msg.addr);
    }
}
