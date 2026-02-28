use actix::{Actor, Addr, Context, StreamHandler};
use colored::*;
use common::deserialize::{deserialize_field, split_message};
use log::{debug, error, info};
use std::net::SocketAddr;

use crate::{
    models::passenger::{
        CompleteLogin, DriverArrived, Drivers, KeepLooking, ReconnectDriver, RecoverResponse,
        RideAccepted,
    },
    passenger::Passenger,
};

pub struct TcpReceiver {
    /// Dirección de socket asociada
    pub addr: SocketAddr,
    /// Direccion del actor del pasajero
    pub client_addr: Addr<Passenger>,
}

impl Actor for TcpReceiver {
    type Context = Context<Self>;
}

/// TcpReceiver solo representa la conexion del passenger con el server
impl StreamHandler<Result<String, std::io::Error>> for TcpReceiver {
    fn handle(&mut self, read: Result<String, std::io::Error>, _ctx: &mut Self::Context) {
        debug!("TcpReceiver::StreamHandler");
        if let Ok(line) = read {
            debug!("Receive {line}\n");
            let (message_name, payload) = split_message(&line);
            match message_name.as_str() {
                "drivers" => {
                    debug!("StreamHandler::drivers");
                    let Ok(drivers_info) = deserialize_field(&payload, "drivers_info") else {
                        error!("Error al deserializar el campo drivers_info");
                        return;
                    };
                    debug!("Driver info: {:?}", drivers_info);
                    let drivers = Drivers { drivers_info };

                    if let Err(e) = self.client_addr.try_send(drivers) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "identificador" => {
                    debug!("StreamHandler::identificador\n");
                    let Ok(id) = deserialize_field(&payload, "id") else {
                        error!("Error al deserializar el campo id");
                        return;
                    };
                    let complete_login = CompleteLogin { id };

                    if let Err(e) = self.client_addr.try_send(complete_login) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "ok" => {
                    debug!("StreamHandler::ok");
                    info!("{}", "Viaje aceptado\n".blue());

                    let Ok(driver_socket) = deserialize_field(&payload, "driver_socket") else {
                        error!("Error al deserializar el campo driver_socket");
                        return;
                    };

                    if let Err(e) = self.client_addr.try_send(RideAccepted { driver_socket }) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "driver_arrived" => {
                    debug!("StreamHandler::driver_arrived\n");

                    if let Err(e) = self.client_addr.try_send(DriverArrived {}) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "no" => {
                    debug!("StreamHandler::no\n");
                    info!("{}", "Viaje rechazado por conductor\n".red());

                    if let Err(e) = self.client_addr.try_send(KeepLooking {}) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }
                "recover_response" => {
                    debug!("StreamHandler::recover_response\n");
                    let Ok(picked_up) = deserialize_field(&payload, "picked_up") else {
                        error!("Error al deserializar el campo picked_up");
                        return;
                    };
                    let Ok(position) = deserialize_field(&payload, "position") else {
                        error!("Error al deserializar el campo position");
                        return;
                    };
                    let Ok(end_ride) = deserialize_field(&payload, "end_ride") else {
                        error!("Error al deserializar el campo end_ride");
                        return;
                    };
                    let recover_response_msg = RecoverResponse {
                        picked_up,
                        position,
                        end_ride,
                    };

                    if let Err(e) = self.client_addr.try_send(recover_response_msg) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }

                "reconnect_driver" => {
                    debug!("StreamHandler::reconnect_driver\n");
                    let Ok(socket) = deserialize_field(&payload, "socket") else {
                        error!("Error al deserializar el campo socket");
                        return;
                    };
                    let reconnect = ReconnectDriver { socket };

                    if let Err(e) = self.client_addr.try_send(reconnect) {
                        error!("Error al enviar el mensaje: {}", e);
                    }
                }

                _ => {
                    debug!("Mensaje {message_name} invalido");
                }
            }
        } else {
            debug!("[{:?}] Failed to read line {:?}", self.addr, read);
        }
    }

    // fn finished(&mut self, _ctx: &mut Self::Context) {
    //     debug!("Stream finished, stopping actor.");
    // }
}
