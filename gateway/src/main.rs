use actix::{Actor, StreamHandler};
use colog::{self};
use colored::*;
use common::constants::GATEWAY_ADDRESS;
use connection::TcpConnection;
use gateway::Gateway;
use log::{error, info};
use models::common::NewConnection;
use std::collections::HashMap;
use std::env;
use tokio::io::{split, AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio_stream::wrappers::LinesStream;

mod connection;
mod functions;
mod gateway;
mod models;

#[actix_rt::main]
async fn main() {
    let mut level = log::LevelFilter::Info;
    if let Some(flag) = env::args().nth(1) {
        if flag.parse::<bool>().unwrap_or(false) {
            level = log::LevelFilter::Debug;
        }
    }

    colog::basic_builder().filter_level(level).init();

    info!("{}", "Gateway conectado\n".green());

    let Ok(listener) = TcpListener::bind(GATEWAY_ADDRESS).await else {
        error!("Error al bindear socket en address {}", GATEWAY_ADDRESS);
        return;
    };

    let gateway = Gateway {
        connections: Vec::new(),
        passengers: HashMap::new(),
        ids_count: 0,
    };

    let gateway_addr = gateway.start();

    info!("Esperando conexiones...\n");

    while let Ok((stream, addr)) = listener.accept().await {
        info!("{}", format!("[{:?}] Cliente conectado\n", addr).green());

        let addr = TcpConnection::create(|ctx| {
            let (read, write_half) = split(stream);
            TcpConnection::add_stream(LinesStream::new(BufReader::new(read).lines()), ctx);
            let write = Some(write_half);
            TcpConnection { write, addr }
        });

        if let Err(e) = gateway_addr.try_send(NewConnection { addr }) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
}
