use log::error;
use serde::Serialize;

#[derive(Serialize)]
pub struct SerializableMessage {
    pub name: String,
    pub payload: serde_json::Value,
}


/// Se utiliza para serializar el mensaje que enviare en un formato json
pub fn serialize_message<T: Serialize>(name: &str, message: T) -> String {
    let Ok(payload) = serde_json::to_value(&message) else {
        error!("Fallo al serializar mensaje {}", name);
        return String::default();
    };
    serde_json::to_string(&SerializableMessage {
        name: name.into(),
        payload,
    })
    .expect("No deberia fallar al serializar mensaje")
}
