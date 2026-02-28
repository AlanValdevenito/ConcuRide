use crate::{
    models::common::{Election, NeighborAck, NewLeader, SendLeaderId},
    server::Server,
};
use actix::{Actor, Addr, AsyncContext, Context, Handler, Message};
use actix_async_handler::async_handler;
use colored::Colorize;
use common::{
    deserialize::{deserialize_field, split_message},
    serialize::serialize_message,
};
use core::str;
use log::{debug, error, info};
use serde_json::json;
use std::sync::Arc;
use tokio::net::UdpSocket;

const MAX_PACKET_SIZE: usize = 65535;

/// Representa un actor que recibe mensajes UDP a traves de un socket
pub struct UdpReceiver {
    /// Socket UDP
    pub socket: Arc<UdpSocket>,
    /// Address del actor socket
    pub server: Option<Addr<Server>>,
}

impl Actor for UdpReceiver {
    type Context = Context<Self>;
}

/// Manda a escuchar mensajes UDP al UdpReceiver
#[derive(Message)]
#[rtype(result = "()")]
pub struct Listen {}

/// Mensaje que representa el pedido el envio de un
/// mensaje UDP.
#[derive(Message)]
#[rtype(result = "()")]
pub struct SendMessage {
    message: String,
    addr: String,
}

/// Mensaje usado para setear la conexion con el server
#[derive(Message)]
#[rtype(result = "()")]
pub struct SetServer {
    pub server_addr: Addr<Server>,
}

impl Handler<SetServer> for UdpReceiver {
    type Result = ();
    /// Setea la conexion con el server.
    fn handle(&mut self, msg: SetServer, _ctx: &mut Context<Self>) {
        self.server = Some(msg.server_addr);
    }
}

#[async_handler]
impl Handler<Listen> for UdpReceiver {
    type Result = ();

    /// Escucha mensajes UDP, los deserializa y envia el respectivo mensaje de actix al server.
    async fn handle(&mut self, _msg: Listen, _ctx: &mut Context<Self>) {
        info!("UdpReceiver::Listen");
        let socket = self.socket.clone();
        let mut buf = [0; MAX_PACKET_SIZE];
        let result = async move {
            let Ok((size, addr)) = socket.recv_from(&mut buf).await else {
                error!("Error al recibir mensaje desde socket UDP, ignorando mensaje");
                return None;
            };
            let Ok(message) = str::from_utf8(&buf[..size]) else {
                error!("Error al convertir los bytes leidos a string, ignorando mensaje");
                return None;
            };
            Some((socket, message.to_string(), addr))
        }
        .await;
        if result.is_none() {
            if let Err(e) = _ctx.address().try_send(Listen {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
            return;
        };
        let (socket, message, addr) = result.expect("No deberia fallar por el chequeo de arriba");
        self.socket = socket;
        debug!("Received '{}'\n", message);
        let (message_name, payload) = split_message(&message);

        match message_name.as_str() {
            "election" => {
                debug!("UdpReceiver::election");
                let Ok(server_ids) = deserialize_field(&payload, "server_ids") else {
                    error!("Error al deserializar el campo server_ids");
                    return;
                };
                let Ok(disconnected_leader_id) =
                    deserialize_field(&payload, "disconnected_leader_id")
                else {
                    error!("Error al deserializar el campo disconnected_leader_id");
                    return;
                };
                let Ok(sequence_number) = deserialize_field(&payload, "sequence_number") else {
                    error!("Error al deserializar el campo sequence_number");
                    return;
                };
                let election_msg = Election {
                    server_ids,
                    disconnected_leader_id,
                    sequence_number,
                };
                debug!("UdpReceiver mandando Election a Server");
                let serialized_ack = serialize_message(
                    "ack",
                    json!({"sequence_number": sequence_number, "message": message, "id": 0}),
                );
                if let Err(err) = _ctx.address().try_send(SendMessage {
                    addr: addr.to_string(),
                    message: serialized_ack,
                }) {
                    error!("Error al enviar el mensaje SendMessage: {}", err)
                }
                if let Err(err) = self
                    .server
                    .as_ref()
                    .expect("Error al enviar mensaje al server.")
                    .try_send(election_msg)
                {
                    error!("Error al enviar el mensaje SendMessage: {}", err)
                }
            }
            "new_leader" => {
                println!("UdpReceiver recibo new_leader");
                let Ok(leader_id) = deserialize_field(&payload, "leader_id") else {
                    error!("Error al deserializer el campo leader_id");
                    return;
                };
                let Ok(id_sender) = deserialize_field(&payload, "id_sender") else {
                    error!("Error al deserializar el campo id_sender");
                    return;
                };
                let Ok(sequence_number) = deserialize_field(&payload, "sequence_number") else {
                    error!("Error al deserializar el campo sequence_number");
                    return;
                };
                let new_leader = NewLeader {
                    leader_id,
                    id_sender,
                    sequence_number,
                };
                let serialized_ack = serialize_message(
                    "ack",
                    json!({"sequence_number": sequence_number, "message": message, "id": 0}),
                );
                if let Err(err) = _ctx.address().try_send(SendMessage {
                    addr: addr.to_string(),
                    message: serialized_ack,
                }) {
                    error!("Error al enviar el mensaje SendMessage: {}", err)
                }
                if let Err(err) = self
                    .server
                    .as_ref()
                    .expect("Error al enviar mensaje al server.")
                    .try_send(new_leader)
                {
                    error!("Error al enviar el mensaje SendMessage: {}", err)
                }
            }
            "ack" => {
                debug!("UdpReceiver recibo ACK");
                let Ok(id) = deserialize_field(&payload, "id") else {
                    error!("Error al deserializar campo id");
                    return;
                };
                let Ok(message) = deserialize_field(&payload, "message") else {
                    error!("Error al deserializar campo message");
                    return;
                };
                let Ok(sequence_number) = deserialize_field(&payload, "sequence_number") else {
                    error!("Error al deserializar campo sequence_number");
                    return;
                };

                if let Err(err) = self
                    .server
                    .as_ref()
                    .expect("Server es None en UdpReceiver")
                    .try_send(NeighborAck {
                        id,
                        message,
                        sequence_number,
                    })
                {
                    error!("Error al enviar el mensaje SendMessage: {}", err)
                }
            }
            "get_leader" => {
                println!("{}", "\n[TEST] Recibo \"get_leader\"\n".blue());
                debug!("UdpReceiver recibo get_leader!");
                if let Err(err) = self
                    .server
                    .as_ref()
                    .expect("Server es None en UdpReceiver")
                    .try_send(SendLeaderId {
                        addr: addr.to_string(),
                    })
                {
                    error!("Error al enviar el mensaje SendMessage: {}", err)
                }
            }
            _ => {
                println!("Invalid message");
            }
        };

        if let Err(e) = _ctx.address().try_send(Listen {}) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}

#[async_handler]
impl Handler<SendMessage> for UdpReceiver {
    type Result = ();
    /// Envia un mensaje UDP al address dada por msg.addr
    async fn handle(&mut self, msg: SendMessage, _ctx: &mut Context<Self>) {
        let socket = self.socket.clone();
        let socket = async move {
            socket
                .send_to(msg.message.as_bytes(), msg.addr)
                .await
                .expect("Error al enviar mensaje desde UdpReceiver");
            socket
        }
        .await;
        self.socket = socket;
    }
}
