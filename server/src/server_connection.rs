use crate::models::common::{Ping, StartElection, UpdateNetworkState, MAX_RESPONSE_TIME};
use crate::Server;
use actix::{Actor, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix_async_handler::async_handler;
use colored::Colorize;
use common::deserialize::{deserialize_field, split_message};
use common::serialize::serialize_message;
use common::tcp_message::TcpMessage;
use log::{debug, error, info};
use tokio::time::{Duration, Instant};
use tokio::{
    io::{AsyncWriteExt, WriteHalf},
    net::TcpStream,
};

/// Representa una conexión TCP con otro servidor.
#[derive(Debug)]
pub struct ServerConnection {
    /// Address del server
    pub addr: Addr<Server>,
    /// Extremo de escritura del socket
    pub write: Option<WriteHalf<TcpStream>>,
    /// Tiempo de respuesta luego del cual se considera caido el server lider
    pub response_time: Instant,
}

impl Actor for ServerConnection {
    type Context = Context<Self>;
}
#[async_handler]
impl Handler<TcpMessage> for ServerConnection {
    type Result = ();

    /// Envia un mensaje TCP al otro extemo del socket
    async fn handle(&mut self, msg: TcpMessage, _ctx: &mut Self::Context) -> Self::Result {
        debug!("ServerConnection::TcpMessage");
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

/// Envia Ping al servidor lider a modo de consultar si el servidor lider permanece con vida
/// Este mensaje se envia cada cierta cantidad de tiempo determinado, en cada llamada se valida si recibio respuesta del lider.
impl Handler<Ping> for ServerConnection {
    type Result = Result<(), String>;

    fn handle(&mut self, _msg: Ping, _ctx: &mut Self::Context) -> Self::Result {
        debug!("ServerConnection::Ping");

        let tiempo = self.response_time.elapsed();

        if tiempo > Duration::from_secs(MAX_RESPONSE_TIME) {
            debug!("Tiempo transcurrdio: {:?}", tiempo);
            info!("{}", "Lider desconectado\n".green());

            if let Err(e) = self.addr.try_send(StartElection {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
        } else {
            info!("{}", "Enviando ping al lider\n".green());
            let serialized_msg = serialize_message("ping", ());
            self.response_time = Instant::now();

            _ctx.address()
                .try_send(TcpMessage::new(serialized_msg))
                .map_err(|e| e.to_string())?;
        }
        self.response_time = Instant::now();
        Ok(())
    }
}

impl StreamHandler<Result<String, std::io::Error>> for ServerConnection {
    /// Recibe un mensaje TCP, lo deserializa en un mensaje de actix y se lo envia
    /// al server.
    fn handle(&mut self, read: Result<String, std::io::Error>, _ctx: &mut Self::Context) {
        debug!("ServerConnection::StreamHandler");

        let tiempo = self.response_time.elapsed();
        debug!("Tiempo transcurrido: {:?}", tiempo);

        self.response_time = Instant::now();

        if let Ok(line) = read {
            debug!(" Receive '{}' from Replica", line.trim());

            let (message_name, payload) = split_message(&line);

            match message_name.as_str() {
                "pong" => {
                    debug!("ServerConnection::pong");
                    info!("{}", "Recibo pong del lider\n".green());

                    let Ok(ids_count) = deserialize_field(&payload, "ids_count") else {
                        error!("Error al deserializar el campo ids_count");
                        return;
                    };
                    let Ok(drivers) = deserialize_field(&payload, "drivers") else {
                        error!("Error al deserializar el campo drivers");
                        return;
                    };
                    let Ok(busy_drivers) = deserialize_field(&payload, "busy_drivers") else {
                        error!("Error al deserializar el campo drivers");
                        return;
                    };
                    let Ok(passengers) = deserialize_field(&payload, "passengers") else {
                        error!("Error al deserializar el campo passengers");
                        return;
                    };
                    let Ok(udp_sockets_replicas) =
                        deserialize_field(&payload, "udp_sockets_replicas")
                    else {
                        error!("Error al deserializar el campo udp_sockets_replicas");
                        return;
                    };
                    let Ok(disconnected_servers) =
                        deserialize_field(&payload, "disconnected_servers")
                    else {
                        error!("Error al deserializar el campo disconnected_servers");
                        return;
                    };

                    let update = UpdateNetworkState {
                        ids_count,
                        drivers,
                        busy_drivers,
                        passengers,
                        udp_sockets_replicas,
                        disconnected_servers,
                    };

                    if let Err(e) = self.addr.try_send(update) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                _ => {
                    error!("Mensaje {message_name} invalido");
                }
            }
        } else {
            error!("Failed to read line {:?}", read);
        }
    }
}
