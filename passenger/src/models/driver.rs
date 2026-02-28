use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverInfo {
    /// Socket del conductor
    pub socket: String,
    /// Posicion origen
    pub position: (u64, u64),
}
