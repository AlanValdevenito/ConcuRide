use actix::{Actor, StreamHandler};
use colog::{self};
use colored::*;
use common::constants::MAX_SERVERS;
use common::constants::SERVER_UDP_PORT_RANGE;
use common::utils::id_to_tcp_address;
use log::debug;
use log::error;
use log::info;
use models::common::{Ping, SetLeader, MAX_RESPONSE_TIME};
use sender::TcpSender;
use serde_json::json;
use server::get_neighbor_id;
use server::Server;
use server_connection::ServerConnection;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::io::{split, AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_stream::wrappers::LinesStream;
use udp_receiver::Listen;
use udp_receiver::SetServer;

use tokio::time::{Duration, Instant};

mod connection_listener;
mod models;
mod sender;
mod server;
mod server_connection;
mod udp_receiver;
mod udp_sender;

use common::{serialize::serialize_message, tcp_message::TcpMessage};

/// Inicializa un servidor líder cuya ID es id.
async fn servidor_lider(id: u64) {
    let address = id_to_tcp_address(id);
    let Ok(listener) = TcpListener::bind(address.clone()).await else {
        error!("Error al bindear socket en address {}", address);
        return;
    };

    let server = Server {
        id,
        id_leader: id,
        im_leader: true,
        drivers: HashMap::new(),
        busy_drivers: HashMap::new(),
        passengers: HashMap::new(),
        passengers_address: HashMap::new(),
        servers: HashMap::new(),
        ids_count: 0,
        udp_sockets_replicas: HashMap::new(),
        udp_sender: None,
        leader_connection: None,
        disconnected_servers: Vec::new(),
        received_neighbor_ack: false,
        election_on_course: false,
        actual_neighbor_id: get_neighbor_id(id),
        sequence_number: 0,
        message_received_ack: HashMap::new(),
    };

    let server_addr = server.start();
    let server_addr_cloned = server_addr.clone();

    info!("{}", "Inicia el servidor lider\n".green());

    info!("Esperando conexiones...\n");
    while let Ok((stream, addr)) = listener.accept().await {
        info!("{}", format!("[{:?}] Nueva conexion\n", addr).green());

        let _ = TcpSender::create(|ctx| {
            let (read, write_half) = split(stream);
            TcpSender::add_stream(LinesStream::new(BufReader::new(read).lines()), ctx);
            let write = Some(write_half);
            TcpSender {
                write,
                addr,
                server_addr: server_addr_cloned.clone(),
            }
        });
    }
}

/// Inicializa un servidor réplica cuya id es ID.
async fn servidor_replica(id: u64) {
    let port = SERVER_UDP_PORT_RANGE + id;
    let udp_socket_addr = format!("127.0.0.1:{port}");
    let Ok(udp_skt) = UdpSocket::bind(udp_socket_addr.clone()).await else {
        error!(
            "Error al intentar bindear socket en address {}",
            udp_socket_addr
        );
        return;
    };
    let udp_socket = Arc::new(udp_skt);
    let udp_socket_sender = udp_socket.clone();
    let udp_receiver = udp_receiver::UdpReceiver {
        socket: udp_socket,
        server: None,
    }
    .start();
    let udp_sender = udp_sender::UdpSender {
        socket: udp_socket_sender,
    }
    .start();
    let id_leader = 1;
    let server = Server {
        id,
        id_leader,
        im_leader: false,
        // connections: Vec::new(),
        drivers: HashMap::new(),
        busy_drivers: HashMap::new(),
        passengers: HashMap::new(),
        passengers_address: HashMap::new(),
        servers: HashMap::new(),
        ids_count: 0,
        udp_sockets_replicas: HashMap::new(),
        udp_sender: Some(udp_sender),
        leader_connection: None,
        disconnected_servers: Vec::new(),
        received_neighbor_ack: false,
        election_on_course: false,
        actual_neighbor_id: get_neighbor_id(id),
        sequence_number: 0,
        message_received_ack: HashMap::new(),
    };

    let server_addr = server.start();

    if let Err(e) = udp_receiver.try_send(SetServer {
        server_addr: server_addr.clone(),
    }) {
        error!("Error al enviar el mensaje: {}", e);
    }

    if let Err(e) = udp_receiver.try_send(Listen {}) {
        error!("Error al enviar el mensaje: {}", e);
    }

    info!("{}", "Inicia el replica\n".green());

    let mut leader_id = 1;
    // CONEXION CON OTRO SERVIDOR
    let mut leader_address = id_to_tcp_address(leader_id);
    if leader_id == id {
        leader_address = id_to_tcp_address(leader_id + 1);
    }
    debug!("Intentando conectarme al server {}", leader_id);
    // Pruebo conectarme al server 1
    let mut connection = TcpStream::connect(leader_address).await;
    // Si falla, me conecto al server de mayor id, despues al que le sigue, y asi sucesivamente
    if connection.is_err() {
        debug!("Fallo la conexion al server {}", leader_id);
        for server_id in (1..=MAX_SERVERS).rev() {
            if server_id == id {
                debug!("Server id {} es igual a mi id {}, salgo", server_id, id);
                continue;
            }
            debug!("Intentando conectarme al server {}", server_id);
            connection = TcpStream::connect(id_to_tcp_address(server_id)).await;
            if connection.is_ok() {
                leader_id = server_id;
                debug!("Conectado a {}!!!", server_id);
                info!("{}", "Conectado al servidor lider\n".green());
                break;
            }
        }
    }

    let Ok(connection) = connection else {
        error!("Error al conectarse al servidor lider. No hay servidor escuchando en TCP");
        return;
    };

    let (read_server_r, write_half_server_r) = split(connection);

    // Inicializacion del server
    let server_connection = ServerConnection::create(|ctx| {
        ServerConnection::add_stream(LinesStream::new(BufReader::new(read_server_r).lines()), ctx);
        let write = Some(write_half_server_r);
        ServerConnection {
            addr: server_addr.clone(),
            write,
            response_time: Instant::now(),
        }
    });

    let serialized_msg =
        serialize_message("new_replica", json!({"id": id, "socket": udp_socket_addr}));

    if let Err(e) = server_connection.try_send(TcpMessage::new(serialized_msg)) {
        error!("Error al enviar el mensaje: {}", e);
    }

    if let Err(e) = server_addr.try_send(SetLeader {
        id_leader: leader_id,
        leader_connection: server_connection.clone(),
    }) {
        error!("Error al enviar el mensaje: {}", e);
    }

    // Duermo para que el servidor me mande el primer pong con todo el estado luego de conectarme
    actix::clock::sleep(Duration::from_secs(1)).await;

    loop {
        let ping_result = server_addr.try_send(Ping {});
        if ping_result.is_err() {
            info!("No hay respuesta de ping, iniciando algoritmo de eleccion");
        }
        actix::clock::sleep(Duration::from_secs(MAX_RESPONSE_TIME - 1)).await;
    }
}

/// Inicializa el server, dependiendo de si es líder o réplica.
async fn multiserver_connection(id: u64, leader: bool) {
    if leader {
        servidor_lider(id).await;
    } else {
        servidor_replica(id).await;
    }
}

#[actix_rt::main]
async fn main() {
    let id = env::args()
        .nth(1)
        .expect("Falta parametro del ID del servidor")
        .parse()
        .expect("Se espera que el ID sea un entero");

    let leader = env::args()
        .nth(2)
        .expect("Falta parametro de liderazgo")
        .parse()
        .expect("Se espera que sea un booleano");

    let mut level = log::LevelFilter::Info;
    if let Some(flag) = env::args().nth(3) {
        if flag.parse::<bool>().unwrap_or(false) {
            level = log::LevelFilter::Debug;
        }
    }

    colog::basic_builder().filter_level(level).init();

    multiserver_connection(id, leader).await;
}
