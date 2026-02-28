use actix::{Actor, Context, Handler};
use actix_async_handler::async_handler;
use common::tcp_message::TcpMessage;
use log::debug;
use std::net::SocketAddr;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

pub struct TcpSender {
    /// Escritura asíncrona en la conexión TCP
    pub write: Option<WriteHalf<TcpStream>>,
    /// Dirección de socket asociada
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
