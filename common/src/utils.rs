use crate::constants::SERVER_TCP_PORT_RANGE;

/// Dado un numero de id, devuelvo el puerto del servidor donde se deberia encontrar el servidor con dicho id
pub fn id_to_tcp_address(id: u64) -> String {
    let tcp_port = SERVER_TCP_PORT_RANGE + id;
    format!("127.0.0.1:{tcp_port}")
}
