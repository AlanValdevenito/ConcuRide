use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

/// Recibe un String en formato json que representa un mensaje.
/// Separa el mensaje en el nombre del mensaje, y el payload del mensaje.
/// Si falla al deserializar, devuelve un mensaje con nombre string vacio.
pub fn split_message(message_string: &str) -> (String, Value) {
    // convierto el string json a un Value
    let Ok(message): Result<Value, serde_json::Error> = serde_json::from_str(message_string) else {
        return (String::default(), Value::default());
    };

    // del Value extraigo el name y lo convierto a String
    let Some(name) = message["name"].as_str() else {
        return (String::default(), Value::default());
    };

    // del Value extraigo el payload
    let Some(payload) = message.get("payload") else {
        return (String::default(), Value::default());
    };
    (name.to_owned(), payload.to_owned())
}

/// Deserializa un campo de un mensaje
///
/// # Argumentos
///
/// * `message` - El mensaje que contiene el campo a deserializar.
/// * `field` - El campo que a deserializar
///
/// # Devuelve
///
/// El campo deserializado.
pub fn deserialize_field<T: DeserializeOwned>(
    message: &Value,
    field: &str,
) -> Result<T, DeserializationError> {
    let Some(field_value) = message.get(field) else {
        return Err(DeserializationError::MissingField(field.into()));
    };
    serde_json::from_value(field_value.to_owned())
        .map_err(|err| DeserializationError::SerdeJsonError(field.into(), err))
}

#[derive(Error, Debug)]
pub enum DeserializationError {
    #[error("Error al deserializar el campo. El mensaje no contiene el campo {0}")]
    MissingField(String),
    #[error("Error al deserializar el campo {0}. Error: {1}")]
    SerdeJsonError(String, serde_json::error::Error),
}
