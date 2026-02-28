use ::driver::receiver::TcpReceiver;
use actix::{Actor, StreamHandler, System};
use colog::{self};
use common::connection::connect_to_server_leader;
use driver::driver::Driver;
use driver::models::messages::{NewConnection, Recovery, Register, SetReceiver};
use driver::passenger_connection::PassengerConnection;
use driver::sender::TcpSender;
use log::{error, info};
use serde_json::Value;
use std::time::Duration;
use std::{env, io};
use tokio::io::{split, AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::TcpListener;
use tokio_stream::wrappers::LinesStream;

use colored::*;

fn main() {
    let result = System::new().block_on(app());
    if let Err(err) = result {
        println!("Error: {}", err)
    }
}

async fn app() -> Result<(), io::Error> {
    let name: String = env::args()
        .nth(1)
        .expect("Falta parametro de la posicion inicial X");

    let x = env::args()
        .nth(2)
        .expect("Falta parametro de la posicion X")
        .parse()
        .expect("Se espera que sea un numero entero la posicion X");

    let y = env::args()
        .nth(3)
        .expect("Falta parametro de la posicion Y")
        .parse()
        .expect("Se espera que sea un numero entero la posicion Y");

    let recovery: bool = env::args()
        .nth(4)
        .expect("Falta parametro de recuperacion")
        .parse()
        .expect("Se espera que sea un booleano");

    let mut level = log::LevelFilter::Info;
    if let Some(flag) = env::args().nth(5) {
        if flag.parse::<bool>().unwrap_or(false) {
            level = log::LevelFilter::Debug;
        }
    }

    colog::basic_builder().filter_level(level).init();

    let Ok(stream) = connect_to_server_leader().await else {
        info!("No hay servidores conectados");
        return Ok(());
    };

    // Escucho en un nuevo puerto por conexiones de pasajeros
    let driver_socket: String = if recovery {
        let mut status = String::new();
        let mut driver_skt: String = "127.0.0.1:0".into();
        if let Ok(mut status_file) = tokio::fs::File::open(format!("driver_{name}.json")).await {
            status_file.read_to_string(&mut status).await?;
            let status: Value = serde_json::from_str(&status)?;
            if let Some(socket) = status["driver_socket"].as_str() {
                driver_skt = socket.into();
            } else {
                driver_skt = "127.0.0.1".into()
            }
        }
        driver_skt
    } else {
        "127.0.0.1:0".into()
    };

    let listener = TcpListener::bind(driver_socket.clone()).await?;
    let driver_socket = listener
        .local_addr()
        .expect("Error al obtener el socket del conductor.")
        .to_string();
    let position = (x, y);

    println!("{}", "Conectado\n".green());

    let addr = stream
        .local_addr()
        .expect("Error al obtener el socket del server.");
    let (read, write_half) = split(stream);

    let sender = TcpSender {
        write: Some(write_half),
        addr,
    }
    .start();

    let client = Driver {
        server_receiver: None,
        server_sender: sender.clone(),
        connections: Vec::new(),
        // drivers: Vec::new(),
        id_current_passenger: 0,
        is_registered: false,
        is_available: true,
        position,
        name: name.clone(),
        passenger_position: None,
        destination: None,
        current_passenger_connection: None,
        passenger_picked_up: false,
        driver_socket: driver_socket.clone(),
    }
    .start();

    let receiver = TcpReceiver::create(|ctx| {
        TcpReceiver::add_stream(LinesStream::new(BufReader::new(read).lines()), ctx);
        TcpReceiver {
            client_addr: client.clone(),
            addr,
        }
    });

    // le pasamos el receiver al driver
    if let Err(e) = client.try_send(SetReceiver { receiver }) {
        error!("Error al enviar el mensaje: {}", e);
    }

    if recovery {
        let mut status = String::new();

        if let Ok(mut status_file) = tokio::fs::File::open(format!("driver_{name}.json")).await {
            status_file.read_to_string(&mut status).await?;

            let status: Value = serde_json::from_str(&status)?;
            if let Err(e) = client.try_send(Recovery {
                status,
                socket: driver_socket.clone(),
            }) {
                error!("Error al enviar el mensaje: {}", e);
            };
        }
    }
    if let Err(e) = client.clone().try_send(Register {
        socket: driver_socket,
        name: name.clone(),
        position,
    }) {
        error!("Error al enviar el mensaje: {}", e);
    };

    println!("\nEsperando pasajeros...\n");

    // escucho por conexiones de pasajeros en otro puerto
    while let Ok((stream, addr)) = listener.accept().await {
        println!("{}", format!("[{:?}] Pasajero conectado\n", addr).green());

        let (read, write_half) = split(stream);
        let client_addr = client.clone();
        let connection = PassengerConnection::create(|ctx| {
            PassengerConnection::add_stream(LinesStream::new(BufReader::new(read).lines()), ctx);
            PassengerConnection {
                driver_addr: client_addr,
                addr,
                write: Some(write_half),
            }
        });
        if let Err(e) = client.try_send(NewConnection { addr: connection }) {
            error!("Error al enviar el mensaje: {}", e);
        };
    }

    actix::clock::sleep(Duration::from_secs(5)).await;
    Ok(())
}
