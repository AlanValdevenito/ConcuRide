use crate::models::gateway::{AuthorizePayment, PaymentDeny, SetPassenger};
use actix::{Actor, Addr, Context, Handler, StreamHandler};
use actix_async_handler::async_handler;
use colored::*;
use common::deserialize::{deserialize_field, split_message};
use common::tcp_message::TcpMessage;
use log::{debug, error, info};
use tokio::{
    io::{AsyncWriteExt, WriteHalf},
    net::TcpStream,
};

use crate::models::passenger::{PaymentAuthorized, RequestDrive};
use crate::passenger::Passenger;

pub struct GatewayConnection {
    // Direccion del actor del pasajero
    pub passenger: Option<Addr<Passenger>>,
    /// Escritura asíncrona en la conexión TCP
    pub write: Option<WriteHalf<TcpStream>>,
}

impl Actor for GatewayConnection {
    type Context = Context<Self>;
}

/// Recibe este mensaje cuando el pago fue autorizado por el gateway
#[async_handler]
impl Handler<AuthorizePayment> for GatewayConnection {
    type Result = ();

    fn handle(&mut self, _msg: AuthorizePayment, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::ApprovePayment");
        debug!("Send TcpMessage to TcpSender\n");
    }
}

/// Almacena al actor pasajero dentro del actor que representa la conexion con el Gateway
#[async_handler]
impl Handler<SetPassenger> for GatewayConnection {
    type Result = ();

    fn handle(&mut self, msg: SetPassenger, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Passenger::SetPassenger");
        self.passenger = Some(msg.passenger);
    }
}

/// Envia al Gateway el contenido del mensaje recibido
#[async_handler]
impl Handler<TcpMessage> for GatewayConnection {
    type Result = ();

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

/// TcpReceiver solo representa la conexion del passenger con el gateway
impl StreamHandler<Result<String, std::io::Error>> for GatewayConnection {
    fn handle(&mut self, read: Result<String, std::io::Error>, _ctx: &mut Self::Context) {
        debug!("TcpSender::StreamHandler");

        if let Ok(line) = read {
            debug!(" Receive '{}' from Client\n", line.trim());

            let (message_name, payload) = split_message(&line);

            match message_name.as_str() {
                "PaymentAuthorizationStatus" => {
                    let Ok(is_authorized) = deserialize_field(&payload, "is_authorized") else {
                        error!("Error al deserializar el campo is_authorized");
                        return;
                    };
                    let Some(passenger) = &self.passenger else {
                        error!("GatewayConnection no tiene passenger");
                        return;
                    };

                    if is_authorized {
                        info!("{}", "Autorización de pago aceptada".green());
                        if let Err(e) = passenger.try_send(PaymentAuthorized {}) {
                            error!("Error al enviar el mensaje: {}", e);
                        }

                        if let Err(e) = passenger.try_send(RequestDrive {}) {
                            error!("Error al enviar el mensaje: {}", e);
                        }
                    } else {
                        info!("{}", "Autorización de pago denegada\n".red());
                        if let Err(e) = passenger.try_send(PaymentDeny {}) {
                            error!("Error al enviar el mensaje: {}", e);
                        }
                    }
                }

                "pago_confirmado" => {
                    debug!("StreamHandler::pago_confirmado");
                    info!("{}", "Pago confirmado".green());
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
