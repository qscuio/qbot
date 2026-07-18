use argon2::{Argon2, PasswordHash, PasswordVerifier};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use rand::{rngs::OsRng, RngCore};
use redis::{aio::ConnectionManager, AsyncCommands};
use sha2::Sha256;

use crate::config::Config;
use crate::error::{AppError, Result};

pub const SESSION_COOKIE_NAME: &str = "__Host-qbot_dashboard_session";
pub const SESSION_TTL_SECONDS: u64 = 12 * 60 * 60;
const LOGIN_FAILURE_TTL_SECONDS: u64 = 15 * 60;
const MAX_LOGIN_FAILURES: i64 = 5;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct DashboardAuth {
    redis: ConnectionManager,
    username: String,
    password_hash: String,
    session_secret: String,
}

impl DashboardAuth {
    pub fn from_config(config: &Config, redis: ConnectionManager) -> Option<Self> {
        if !config.dashboard_enabled() {
            return None;
        }
        Some(Self {
            redis,
            username: config.dashboard_username.clone()?,
            password_hash: config.dashboard_password_hash.clone()?,
            session_secret: config.dashboard_session_secret.clone()?,
        })
    }

    pub fn verify_credentials(&self, username: &str, password: &str) -> bool {
        let password_valid = PasswordHash::new(&self.password_hash)
            .ok()
            .is_some_and(|hash| {
                Argon2::default()
                    .verify_password(password.as_bytes(), &hash)
                    .is_ok()
            });
        username == self.username && password_valid
    }

    pub async fn is_throttled(&self, client_key: &str) -> Result<bool> {
        let mut redis = self.redis.clone();
        let value: Option<i64> = redis
            .get(login_failure_key(client_key))
            .await
            .map_err(AppError::Redis)?;
        Ok(value.unwrap_or(0) >= MAX_LOGIN_FAILURES)
    }

    pub async fn record_failure(&self, client_key: &str) -> Result<()> {
        let mut redis = self.redis.clone();
        let key = login_failure_key(client_key);
        let count: i64 = redis.incr(&key, 1).await.map_err(AppError::Redis)?;
        if count == 1 {
            redis
                .expire::<_, ()>(&key, LOGIN_FAILURE_TTL_SECONDS as i64)
                .await
                .map_err(AppError::Redis)?;
        }
        Ok(())
    }

    pub async fn clear_failures(&self, client_key: &str) -> Result<()> {
        let mut redis = self.redis.clone();
        redis
            .del::<_, ()>(login_failure_key(client_key))
            .await
            .map_err(AppError::Redis)?;
        Ok(())
    }

    pub async fn create_session(&self) -> Result<String> {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        let token = URL_SAFE_NO_PAD.encode(bytes);
        let digest = session_digest(&self.session_secret, &token)?;
        let mut redis = self.redis.clone();
        redis
            .set_ex::<_, _, ()>(session_key(&digest), &self.username, SESSION_TTL_SECONDS)
            .await
            .map_err(AppError::Redis)?;
        Ok(token)
    }

    pub async fn authenticate(&self, token: &str) -> Result<bool> {
        let digest = session_digest(&self.session_secret, token)?;
        let mut redis = self.redis.clone();
        let username: Option<String> = redis
            .get(session_key(&digest))
            .await
            .map_err(AppError::Redis)?;
        Ok(username.as_deref() == Some(self.username.as_str()))
    }

    pub async fn logout(&self, token: &str) -> Result<()> {
        let digest = session_digest(&self.session_secret, token)?;
        let mut redis = self.redis.clone();
        redis
            .del::<_, ()>(session_key(&digest))
            .await
            .map_err(AppError::Redis)?;
        Ok(())
    }
}

fn login_failure_key(client_key: &str) -> String {
    let clean: String = client_key
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | ':' | '-' | '_'))
        .take(128)
        .collect();
    format!("dashboard:login-fail:{clean}")
}

fn session_key(digest: &str) -> String {
    format!("dashboard:session:{digest}")
}

fn session_digest(secret: &str, token: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| AppError::Config("invalid dashboard session secret".to_string()))?;
    mac.update(token.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

pub fn session_token_from_cookie(cookie_header: &str) -> Option<&str> {
    cookie_header.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        (name == SESSION_COOKIE_NAME && !value.is_empty()).then_some(value)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::{password_hash::SaltString, PasswordHasher};
    use uuid::Uuid;

    #[test]
    fn session_digest_is_stable_and_does_not_contain_token() {
        let digest = session_digest("secret", "raw-token").unwrap();

        assert_eq!(digest, session_digest("secret", "raw-token").unwrap());
        assert!(!digest.contains("raw-token"));
    }

    #[test]
    fn cookie_parser_reads_only_the_dashboard_cookie() {
        assert_eq!(
            session_token_from_cookie("theme=dark; __Host-qbot_dashboard_session=abc123; x=y"),
            Some("abc123")
        );
        assert_eq!(session_token_from_cookie("theme=dark"), None);
    }

    async fn test_auth() -> DashboardAuth {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let client = redis::Client::open(redis_url).unwrap();
        let redis = ConnectionManager::new(client).await.unwrap();
        let salt = SaltString::encode_b64(b"qbot-dashboard-1").unwrap();
        let password_hash = Argon2::default()
            .hash_password(b"correct horse", &salt)
            .unwrap()
            .to_string();
        DashboardAuth {
            redis,
            username: "analyst".to_string(),
            password_hash,
            session_secret: format!("test-secret-{}", Uuid::new_v4()),
        }
    }

    #[tokio::test]
    async fn credentials_sessions_revocation_expiry_and_throttling() {
        let auth = test_auth().await;
        assert!(auth.verify_credentials("analyst", "correct horse"));
        assert!(!auth.verify_credentials("analyst", "wrong"));
        assert!(!auth.verify_credentials("someone-else", "correct horse"));

        let token = auth.create_session().await.unwrap();
        assert!(auth.authenticate(&token).await.unwrap());
        auth.logout(&token).await.unwrap();
        assert!(!auth.authenticate(&token).await.unwrap());

        let expired_token = format!("expired-{}", Uuid::new_v4());
        let digest = session_digest(&auth.session_secret, &expired_token).unwrap();
        let mut redis = auth.redis.clone();
        redis
            .set_ex::<_, _, ()>(session_key(&digest), "analyst", 1)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        assert!(!auth.authenticate(&expired_token).await.unwrap());

        let client_key = format!("test-{}", Uuid::new_v4());
        for _ in 0..MAX_LOGIN_FAILURES {
            auth.record_failure(&client_key).await.unwrap();
        }
        assert!(auth.is_throttled(&client_key).await.unwrap());
        auth.clear_failures(&client_key).await.unwrap();
        assert!(!auth.is_throttled(&client_key).await.unwrap());
    }
}
