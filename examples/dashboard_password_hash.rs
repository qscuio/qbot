use argon2::{password_hash::SaltString, Argon2, PasswordHasher};
use rand::{rngs::OsRng, RngCore};
use std::io::{self, Read};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut password = String::new();
    io::stdin().read_to_string(&mut password)?;
    let password = password.trim_end_matches(['\r', '\n']);
    if password.is_empty() {
        return Err("read a non-empty password from stdin".into());
    }

    let mut salt_bytes = [0u8; 16];
    OsRng.fill_bytes(&mut salt_bytes);
    let salt =
        SaltString::encode_b64(&salt_bytes).map_err(|error| io::Error::other(error.to_string()))?;
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|error| io::Error::other(error.to_string()))?;
    println!("{hash}");
    Ok(())
}
