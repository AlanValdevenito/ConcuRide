use std::net::SocketAddr;

use crate::models::common::{NewServerConnection, Pong, ReconnectDriver};
use crate::models::driver::{BusyDriver, GetDrivers, NewDriver, UpdateDriver};
use crate::models::passenger::PassengerLogin;
use crate::Server;
use actix::{Actor, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix_async_handler::async_handler;
use colored::*;
use common::deserialize::{deserialize_field, split_message};
use common::tcp_message::TcpMessage;
use log::{debug, error, info};
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

/// Representa una conexión TCP con un proceso de la red, el
/// cual puede ser otro servidor, un conductor o un pasajero.
///
/// Implementa el trait StreamHandler por lo tanto, envía y recibe
/// mensajes TCP.
pub struct TcpSender {
    /// Extremo de escritura del socket.
    pub write: Option<WriteHalf<TcpStream>>,
    /// Dirección del socket (IP y puerto)
    pub addr: SocketAddr,
    /// Address del actor server.
    pub server_addr: Addr<Server>,
}

impl Actor for TcpSender {
    type Context = Context<Self>;
}

#[async_handler]
impl Handler<TcpMessage> for TcpSender {
    type Result = ();

    /// Envía el mensaje TCP al otro extremo del socket.
    async fn handle(&mut self, msg: TcpMessage, _ctx: &mut Self::Context) -> Self::Result {
        debug!("TcpSender::TcpMessage");
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
        debug!("Send '{}' to Client\n", message.trim());
        self.write = Some(ret_write);
    }
}

impl StreamHandler<Result<String, std::io::Error>> for TcpSender {
    /// Recibe un mensaje TCP, lo deserializa y envia al Server un mensaje de actix correspondiente.
    fn handle(&mut self, read: Result<String, std::io::Error>, ctx: &mut Self::Context) {
        debug!("TcpSender::StreamHandler");

        if let Ok(line) = read {
            debug!("[{:?}] Receive '{}' from Client", self.addr, line.trim());

            let (message_name, payload) = split_message(&line);

            match message_name.as_str() {
                "new_replica" => {
                    debug!("TcpSender::new_replica\n");
                    let Ok(id) = deserialize_field(&payload, "id") else {
                        error!("Error al deserializar el campo id");
                        return;
                    };
                    let Ok(socket) = deserialize_field(&payload, "socket") else {
                        error!("Error al deserializar el campo socket");
                        return;
                    };

                    let server_connection = NewServerConnection {
                        addr: ctx.address(),
                        socket,
                        id,
                    };

                    if let Err(e) = self.server_addr.try_send(server_connection) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "login" => {
                    debug!("TcpSender::login\n");
                    let Ok(name) = deserialize_field(&payload, "name") else {
                        error!("Error al deserializar el campo name");
                        return;
                    };
                    let passenger_login = PassengerLogin {
                        name,
                        addr: ctx.address(),
                    };

                    if let Err(e) = self.server_addr.try_send(passenger_login) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "get_drivers" => {
                    debug!("TcpSender::get_drivers\n");
                    let get_drivers = GetDrivers {
                        addr: ctx.address(),
                    };

                    if let Err(e) = self.server_addr.try_send(get_drivers) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "new_driver" => {
                    debug!("TcpSender::new_driver\n");
                    let Ok(position) = deserialize_field(&payload, "position") else {
                        error!("Error al deserializar el campo position");
                        return;
                    };
                    let Ok(socket) = deserialize_field(&payload, "socket") else {
                        error!("Error al deserializar el campo socket");
                        return;
                    };
                    let Ok(name) = deserialize_field(&payload, "name") else {
                        error!("Error al deserializar el campo name");
                        return;
                    };
                    let new_driver = NewDriver {
                        socket,
                        name,
                        position,
                    };

                    if let Err(e) = self.server_addr.try_send(new_driver) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "busy_driver" => {
                    info!("TcpSender::busy_driver\n");
                    let Ok(driver_name) = deserialize_field(&payload, "name") else {
                        error!("Error al deserializar el campo name");
                        return;
                    };
                    let busy_driver = BusyDriver { name: driver_name };

                    if let Err(e) = self.server_addr.try_send(busy_driver) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "available_driver" => {
                    debug!("TcpSender::available_driver\n");
                    let Ok(new_position) = deserialize_field(&payload, "position") else {
                        error!("Error al deserializar el campo position");
                        return;
                    };
                    let Ok(name) = deserialize_field(&payload, "name") else {
                        error!("Error al deserializar el campo name");
                        return;
                    };
                    let driver_updated = UpdateDriver { new_position, name };

                    //actualizar posicion y estado del driver
                    if let Err(e) = self.server_addr.try_send(driver_updated) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "ping" => {
                    debug!("TcpSender::ping");
                    info!("{}", "Recibo ping de una replica\n".green());

                    if let Err(e) = self.server_addr.try_send(Pong {
                        addr: ctx.address(),
                    }) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "reconnect_driver" => {
                    debug!("TcpSender::reconnect_driver\n");
                    let Ok(id_passenger) = deserialize_field(&payload, "id_passenger") else {
                        error!("Error al deserializar el campo id_passenger");
                        return;
                    };
                    let Ok(socket) = deserialize_field(&payload, "socket") else {
                        error!("Error al deserializar el campo socket");
                        return;
                    };
                    let Ok(name) = deserialize_field(&payload, "name") else {
                        error!("Error al deserializar el campo name");
                        return;
                    };
                    let reconnect = ReconnectDriver {
                        name,
                        id_passenger,
                        socket,
                    };

                    if let Err(e) = self.server_addr.try_send(reconnect) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                _ => {
                    error!("Mensaje {message_name} invalido\n");
                }
            }
        } else {
            error!("[{:?}] Failed to read line {:?}", self.addr, read);
        }
    }
}
