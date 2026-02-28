use actix::{Actor, ActorContext, Context, Handler};
use actix_async_handler::async_handler;
use common::tcp_message::TcpMessage;
use log::{debug, error};
use std::net::SocketAddr;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

pub struct TcpSender {
    pub write: Option<WriteHalf<TcpStream>>,
    pub addr: SocketAddr,
}

impl Actor for TcpSender {
    type Context = Context<Self>;
}

/// Envia al servidor el contenido del mensaje recibido
#[async_handler]
impl Handler<TcpMessage> for TcpSender {
    type Result = ();

    async fn handle(&mut self, msg: TcpMessage, _ctx: &mut Self::Context) -> Self::Result {
        debug!("TcpSender::TcpMessage");
        let message = msg.0.clone();
        let mut write = self.write.take().expect(
            "No debería poder llegar otro mensaje antes de que vuelva por usar AtomicResponse",
        );
        let ret_write = async move {
            match write.write_all(msg.0.as_bytes()).await {
                Ok(_) => Ok(write),
                Err(err) => {
                    error!("Unexpected error while sending: {:?}", err);
                    Err(err)
                }
            }
        }
        .await;

        debug!("Send '{}'\n", message.trim());

        match ret_write {
            Ok(write) => self.write = Some(write),
            Err(_) => {
                error!("Connection lost to {}", self.addr);
                self.write = None;

                _ctx.stop();
            }
        }
    }
}
