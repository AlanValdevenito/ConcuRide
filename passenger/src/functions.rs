use crate::passenger::Passenger;
use crate::receiver::TcpReceiver;
use crate::sender::TcpSender;
use actix::{Actor, Addr, StreamHandler};
use colored::Colorize;
use log::info;
use std::io;
use tokio::{
    io::{split, AsyncBufReadExt, BufReader},
    net::TcpStream,
};
use tokio_stream::wrappers::LinesStream;

/// Calcula la distancia entre dos coordenadas
pub fn distance(position: (u64, u64), other_position: (u64, u64)) -> u64 {
    let (x1, y1) = position;
    let (x2, y2) = other_position;

    f64::sqrt((x1 as f64 - x2 as f64).powi(2) + (y1 as f64 - y2 as f64).powi(2)) as u64
}

/// Se conecta al conductor, segun el socket recibido
pub async fn new_connection(
    driver_socket: String,
    client_addr: Addr<Passenger>,
) -> Result<Addr<TcpSender>, io::Error> {
    async {
        let stream = TcpStream::connect(driver_socket).await?;

        // info!("Conectado al conductor");
        info!("{}", "Conectado al conductor\n".green());

        let local_addr = stream
            .local_addr()
            .expect("Error al obtener socket local luego de reconexion con el server");
        let (read, write_half) = split(stream);
        TcpReceiver::create(|ctx| {
            TcpReceiver::add_stream(LinesStream::new(BufReader::new(read).lines()), ctx);
            TcpReceiver {
                client_addr,
                addr: local_addr,
            }
        });
        Ok(TcpSender {
            write: Some(write_half),
            addr: local_addr,
        }
        .start())
    }
    .await
}
