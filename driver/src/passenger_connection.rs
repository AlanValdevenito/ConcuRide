use crate::models::passenger::PassengerConnectionMessage;
use actix::{Actor, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix_async_handler::async_handler;
use common::deserialize::{deserialize_field, split_message};
use log::{debug, error};
use std::net::SocketAddr;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

use crate::driver::Driver;
use crate::models::messages::{PassengerRecover, ReconnectPassenger, RequestRide};

pub struct PassengerConnection {
    pub write: Option<WriteHalf<TcpStream>>,
    pub addr: SocketAddr,
    pub driver_addr: Addr<Driver>,
}

impl Actor for PassengerConnection {
    type Context = Context<Self>;
}

/// Envia el contenido del mensaje recibido al pasajero
#[async_handler]
impl Handler<PassengerConnectionMessage> for PassengerConnection {
    type Result = ();

    async fn handle(
        &mut self,
        msg: PassengerConnectionMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug!("PassengerConnection::TcpMessage");
        let message = msg.0.clone();
        let mut write = self.write.take().expect(
            "No debería poder llegar otro mensaje antes de que vuelva por usar AtomicResponse",
        );

        let ret_write = async move {
            write
                .write_all(msg.0.as_bytes())
                .await
                .expect("should have sent");
            write
        }
        .await;

        debug!("Send '{}'\n", message.trim());

        self.write = Some(ret_write);
    }
}

/// Recibe un Stream proveniente del pasajero, lo deserializa e interpretra segun corresponda
impl StreamHandler<Result<String, std::io::Error>> for PassengerConnection {
    fn handle(&mut self, read: Result<String, std::io::Error>, ctx: &mut Self::Context) {
        debug!("PassengerConnection::StreamHandler");
        if let Ok(line) = read {
            debug!("Receive {line}\n");

            let (message_name, payload) = split_message(&line);

            debug!("{:?}", message_name.as_str());

            match message_name.as_str() {
                "request_ride" => {
                    debug!("StreamHandler::request_ride\n");
                    let Ok(id_passenger) = deserialize_field(&payload, "id") else {
                        error!("Error al deserializar el campo id_passenger");
                        return;
                    };
                    let Ok(origin) = deserialize_field(&payload, "origin") else {
                        error!("Error al deserializar el campo origin");
                        return;
                    };
                    let Ok(destination) = deserialize_field(&payload, "destination") else {
                        error!("Error al deserializar el campo destination");
                        return;
                    };

                    let request_ride = RequestRide {
                        id_passenger,
                        origin,
                        destination,
                        addr: ctx.address(),
                    };

                    if let Err(e) = self.driver_addr.try_send(request_ride) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "recover" => {
                    debug!("StreamHandler::recover\n");
                    let Ok(id_passenger) = deserialize_field(&payload, "id_passenger") else {
                        error!("Error al deserializar el campo id_passenger");
                        return;
                    };
                    if let Err(e) = self.driver_addr.try_send(PassengerRecover {
                        passenger_connection: ctx.address(),
                        id_passenger,
                    }) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "reconnect_driver" => {
                    debug!("StreamHandler::reconnect_driver\n");

                    let Ok(position) = deserialize_field(&payload, "position") else {
                        error!("Error al deserializar el campo position");
                        return;
                    };

                    if let Err(e) = self.driver_addr.try_send(ReconnectPassenger {
                        passenger_connection: ctx.address(),
                        position,
                    }) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                _ => {
                    error!("Mensaje {message_name} invalido");
                }
            }
        } else {
            error!("[{:?}] Failed to read line {:?}", self.addr, read);
        }
    }

    fn finished(&mut self, _ctx: &mut Self::Context) {
        debug!("Stream finished, stopping actor.");
    }
}
