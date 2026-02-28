use ::passenger::receiver::TcpReceiver;
use actix::{Actor, StreamHandler, System};
use colog::{self};
use common::connection::connect_to_server_leader;
use common::constants::GATEWAY_ADDRESS;
use log::{error, info};
use passenger::gateway_connection::GatewayConnection;
use passenger::models::gateway::SetPassenger;
use passenger::models::passenger::{GetDrivers, Login, PassengerInfo, Recover, SetReceiver};
use passenger::passenger::Passenger;
use passenger::sender::TcpSender;
use serde_json::Value;
use std::sync::Arc;
use std::{env, io};
use tokio::io::{split, AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::LinesStream;

use colored::*;

fn main() {
    let result = System::new().block_on(app());
    if let Err(err) = result {
        println!("Error: {}", err)
    }
}

async fn app() -> Result<(), io::Error> {
    let name = env::args().nth(1).expect("Falta parametro del nombre");

    let src_x = env::args()
        .nth(2)
        .expect("Falta parametro de la posicion inicial X")
        .parse()
        .expect("Se espera que sea un numero entero la posicion origen X");

    let src_y = env::args()
        .nth(3)
        .expect("Falta parametro de la posicion inicial Y")
        .parse()
        .expect("Se espera que sea un numero entero la posicion origen Y");

    let src_position = (src_x, src_y);

    let dst_x = env::args()
        .nth(4)
        .expect("Falta parametro de la posicion destino X")
        .parse()
        .expect("Se espera que sea un numero entero la posicion destino X");

    let dst_y = env::args()
        .nth(5)
        .expect("Falta parametro de la posicion destino Y")
        .parse()
        .expect("Se espera que sea un numero entero la posicion destino Y");

    let recovery: bool = env::args()
        .nth(6)
        .expect("Falta parametro de recuperacion")
        .parse()
        .expect("Se espera que sea un booleano");

    let mut level = log::LevelFilter::Info;
    if let Some(flag) = env::args().nth(7) {
        if flag.parse::<bool>().unwrap_or(false) {
            level = log::LevelFilter::Debug;
        }
    }

    colog::basic_builder().filter_level(level).init();

    let dst_position = (dst_x, dst_y);

    let passenger_info = PassengerInfo {
        id: 0,
        position: src_position,
        destination: dst_position,
    };

    // CONEXION CON EL SERVIDOR

    let Ok(stream) = connect_to_server_leader().await else {
        info!("No hay servidores conectados");
        return Ok(());
    };

    info!("{}", "Conectado\n".green());

    let addr = stream
        .local_addr()
        .expect("Error al obtener el socket del servidor lider");
    let (read, write_half) = split(stream);
    let sender = TcpSender {
        write: Some(write_half),
        addr,
    }
    .start();

    // CONEXION CON EL GATEWAY

    let Ok(stream_gateway) = TcpStream::connect(GATEWAY_ADDRESS).await else {
        error!("Error al conectarse al gateway.");
        return Ok(());
    };

    info!("{}", "Conectado al Gateway\n".green());

    let (read_gateway, write_half_gateway) = split(stream_gateway);

    // Inicializacion del gateway
    let gateway = GatewayConnection::create(|ctx| {
        GatewayConnection::add_stream(LinesStream::new(BufReader::new(read_gateway).lines()), ctx);
        let write = Some(write_half_gateway);
        GatewayConnection {
            write,
            passenger: None,
        }
    });
    // Semaforo para señalizar la finalizacion del programa
    let finished_signal = Arc::new(Semaphore::new(0));

    // CREACION DEL PASAJERO
    let passenger = Passenger {
        name: name.clone(),
        server_receiver: None,
        server_sender: sender.clone(),
        gateway: gateway.clone(),
        connections: Vec::new(),
        info: passenger_info,
        drivers_info: Vec::new(),
        driver_socket: None,
        recovery,
        payment_authorized: false,
        payment_confirmed: false,
        finished_signal: finished_signal.clone(),
    }
    .start();

    if let Err(e) = gateway.try_send(SetPassenger {
        passenger: passenger.clone(),
    }) {
        error!("Error al enviar el mensaje SetPassenger: {}", e);
        return Ok(());
    }

    let server_receiver = TcpReceiver::create(|ctx| {
        TcpReceiver::add_stream(LinesStream::new(BufReader::new(read).lines()), ctx);
        TcpReceiver {
            client_addr: passenger.clone(),
            addr,
        }
    });

    if let Err(e) = passenger.try_send(SetReceiver {
        receiver: server_receiver,
    }) {
        error!("Error al enviar el mensaje SetReceiver: {}", e);
        return Ok(());
    }

    if recovery {
        // Leo el archivo con el estado
        let mut status = String::new();

        if let Ok(mut status_file) = tokio::fs::File::open(format!("passenger_{name}.json")).await {
            if recovery {
                status_file.read_to_string(&mut status).await?;

                let status: Value = serde_json::from_str(&status)?;
                if let Err(e) = passenger.try_send(Recover { status }) {
                    error!("Error al enviar el mensaje: {}", e);
                };
            }
        } else {
            // Al iniciar la aplicacion el pasajero de logguea

            if let Err(e) = passenger.try_send(Login { name }) {
                error!("Error al enviar el mensaje: {}", e);
            }
            // Si soy pasajero, pido los drivers
            info!("{}", "Buscando viaje... ⏳\n".yellow());
            if let Err(e) = passenger.try_send(GetDrivers {}) {
                error!("Error al enviar el mensaje: {}", e);
            }
        }
    } else {
        // Al iniciar la aplicacion el pasajero de logguea

        if let Err(e) = passenger.try_send(Login { name }) {
            error!("Error al enviar el mensaje: {}", e);
        }
        // Si soy pasajero, pido los drivers
        info!("{}", "Buscando viaje... ⏳\n".yellow());
        if let Err(e) = passenger.try_send(GetDrivers {}) {
            error!("Error al enviar el mensaje: {}", e);
        }
    }
    if let Err(err) = finished_signal.acquire().await {
        error!("Error esperando la señal de finalizacion: {}", err);
    }
    Ok(())
}
