use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::time::Duration;

use crate::error::{AppError, Result};

pub struct RedisCache {
    conn: ConnectionManager,
}

impl RedisCache {
    pub fn new(conn: ConnectionManager) -> Self {
        RedisCache { conn }
    }

    pub async fn set_json<T: serde::Serialize>(
        &mut self,
        key: &str,
        value: &T,
        ttl: Duration,
    ) -> Result<()> {
        let json = serde_json::to_string(value)?;
        self.conn
            .set_ex::<_, _, ()>(key, json, ttl.as_secs())
            .await
            .map_err(AppError::Redis)?;
        Ok(())
    }

    pub async fn get_json<T: serde::de::DeserializeOwned>(
        &mut self,
        key: &str,
    ) -> Result<Option<T>> {
        let val: Option<String> = self.conn.get(key).await.map_err(AppError::Redis)?;
        match val {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    pub async fn set_flag(&mut self, key: &str, ttl: Duration) -> Result<()> {
        self.conn
            .set_ex::<_, _, ()>(key, "1", ttl.as_secs())
            .await
            .map_err(AppError::Redis)?;
        Ok(())
    }

    pub async fn has_flag(&mut self, key: &str) -> Result<bool> {
        let exists: bool = self.conn.exists(key).await.map_err(AppError::Redis)?;
        Ok(exists)
    }

    pub async fn delete(&mut self, key: &str) -> Result<()> {
        self.conn.del::<_, ()>(key).await.map_err(AppError::Redis)?;
        Ok(())
    }

    /// Cache scan results until next trading day (TTL: 24h)
    pub async fn cache_scan_results(&mut self, results: &serde_json::Value) -> Result<()> {
        self.set_json("scan:latest", results, Duration::from_secs(86400)).await
    }

    pub async fn get_scan_results(&mut self) -> Result<Option<serde_json::Value>> {
        self.get_json("scan:latest").await
    }

    /// Cache stock universe (TTL: 24h)
    pub async fn cache_stock_universe(&mut self, stocks: &serde_json::Value) -> Result<()> {
        self.set_json("stocks:universe", stocks, Duration::from_secs(86400)).await
    }

    pub async fn get_stock_universe(&mut self) -> Result<Option<serde_json::Value>> {
        self.get_json("stocks:universe").await
    }

    /// Burst monitor cooldown (TTL: 5min)
    pub async fn set_burst_alerted(&mut self, code: &str) -> Result<()> {
        self.set_flag(&format!("burst:alerted:{}", code), Duration::from_secs(300)).await
    }

    pub async fn is_burst_alerted(&mut self, code: &str) -> Result<bool> {
        self.has_flag(&format!("burst:alerted:{}", code)).await
    }
}
