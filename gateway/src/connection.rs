use crate::functions::authorize_payment;
use crate::models::messages::PaymentAuthorizationStatus;
use actix::{Actor, AsyncContext, Context, Handler, StreamHandler};
use actix_async_handler::async_handler;
use colored::Colorize;
use common::deserialize::deserialize_field;
use common::deserialize::split_message;
use common::deserialize::DeserializationError;
use common::serialize::serialize_message;
use common::tcp_message::TcpMessage;
use log::{debug, error, info};
use std::net::SocketAddr;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

pub struct TcpConnection {
    pub write: Option<WriteHalf<TcpStream>>,
    pub addr: SocketAddr,
}

impl Actor for TcpConnection {
    type Context = Context<Self>;
}

/// Envia al pasajero el contenido del mensaje recibido
#[async_handler]
impl Handler<TcpMessage> for TcpConnection {
    type Result = ();

    async fn handle(&mut self, msg: TcpMessage, _ctx: &mut Self::Context) -> Self::Result {
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


/// TcpReceiver solo representa la conexion del gateway con el pasajero
impl StreamHandler<Result<String, std::io::Error>> for TcpConnection {
    fn handle(&mut self, read: Result<String, std::io::Error>, ctx: &mut Self::Context) {
        debug!("TcpConnection::StreamHandler");

        if let Ok(line) = read {
            debug!("[{:?}] Receive '{}' from Client\n", self.addr, line.trim());

            let (message_name, payload) = split_message(&line);

            match message_name.as_str() {
                "AuthorizePayment" => {
                    debug!("StreamHandler::AuthorizePayment");
                    let is_authorized = authorize_payment();
                    let serialized_msg = serialize_message(
                        "PaymentAuthorizationStatus",
                        PaymentAuthorizationStatus { is_authorized },
                    );

                    if let Err(e) = ctx.address().try_send(TcpMessage::new(serialized_msg)) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "ConfirmPayment" => {
                    debug!("StreamHandler::ConfirmPayment");

                    let Ok(name): Result<String, DeserializationError> =
                        deserialize_field(&payload, "name")
                    else {
                        error!("Error al deserializar el campo name");
                        return;
                    };

                    info!(
                        "{}",
                        format!("Payment recibed from Passenger {}", name).green()
                    );

                    let serialized_msg = serialize_message("pago_confirmado", ());

                    if let Err(e) = ctx.address().try_send(TcpMessage::new(serialized_msg)) {
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
}
