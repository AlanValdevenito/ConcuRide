use log::debug;
use std::io;
use tokio::net::TcpStream;

use crate::{constants::MAX_SERVERS, utils::id_to_tcp_address};

/// Se conecta con el servidor lider
/// Intento conetarme al servidor con id mas alto, en caso de error intento decrementando
/// Hasta que me pueda conectar a un servidor
pub async fn connect_to_server_leader() -> Result<TcpStream, io::Error> {
    let highest_server_id = MAX_SERVERS;
   
    debug!("Intentando conectarme al server {}", highest_server_id);
    
    let server_address = id_to_tcp_address(highest_server_id);
    
    // Pruebo conectarme al server con id mayor
    let mut server_connection = TcpStream::connect(server_address).await;
    
    // Si falla, me conecto al server de mayor id que le sigue, despues al que le sigue, y asi sucesivamente
    if server_connection.is_err() {
        debug!("Fallo la conexion al server {}", highest_server_id);
        for server_id in (1..MAX_SERVERS).rev() {
            debug!("Intentando conectarme al server {}", server_id);
            server_connection = TcpStream::connect(id_to_tcp_address(server_id)).await;
            if server_connection.is_ok() {
                debug!("Conectado a {}!", server_id);
                break;
            }
        }
    }
    server_connection
}
