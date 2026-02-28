use rand::Rng;

/// Genera un numero random con la probabilidad que se se autorice el viaje o no
/// Utilizamos una probabilidad de rechazo 10%
pub fn authorize_payment() -> bool {
    let mut rng = rand::thread_rng();
    // probabilidad de rechazo = 10%
    rng.gen_range(0..=10) >= 1
}
