use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

use crate::error::{AppError, Result};

const TG_API: &str = "https://api.telegram.org/bot";
const MAX_MESSAGE_LEN: usize = 4096;

pub struct TelegramPusher {
    token: String,
    client: Client,
}

impl TelegramPusher {
    pub fn new(token: String) -> Self {
        TelegramPusher {
            token,
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    pub async fn push(&self, channel: &str, text: &str) -> Result<()> {
        for chunk in Self::split_message(text) {
            self.send_message(channel, &chunk).await?;
            if chunk.len() > 100 {
                sleep(Duration::from_millis(500)).await;
            }
        }
        Ok(())
    }

    pub async fn set_webhook(&self, webhook_url: &str, secret_token: Option<&str>) -> Result<()> {
        let url = format!("{}{}/setWebhook", TG_API, self.token);
        let mut body = json!({
            "url": webhook_url,
            "drop_pending_updates": false,
            "allowed_updates": ["message", "edited_message"],
        });

        if let Some(secret) = secret_token.filter(|s| !s.trim().is_empty()) {
            body["secret_token"] = json!(secret);
        }

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(AppError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_text = resp.text().await.unwrap_or_default();
            return Err(AppError::Internal(format!(
                "setWebhook {}: {}",
                status, err_text
            )));
        }

        Ok(())
    }

    async fn send_message(&self, chat_id: &str, text: &str) -> Result<()> {
        let url = format!("{}{}/sendMessage", TG_API, self.token);
        let body = json!({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(AppError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_text = resp.text().await.unwrap_or_default();
            if status.as_u16() == 429 {
                warn!("Telegram rate limit hit, waiting 5s...");
                sleep(Duration::from_secs(5)).await;
                let retry = self
                    .client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(AppError::Http)?;
                if !retry.status().is_success() {
                    return Err(AppError::Internal(format!(
                        "Telegram error: {}",
                        retry.status()
                    )));
                }
            } else {
                return Err(AppError::Internal(format!(
                    "Telegram {}: {}",
                    status, err_text
                )));
            }
        }

        Ok(())
    }

    fn split_message(text: &str) -> Vec<String> {
        if text.len() <= MAX_MESSAGE_LEN {
            return vec![text.to_string()];
        }
        text.chars()
            .collect::<Vec<char>>()
            .chunks(MAX_MESSAGE_LEN)
            .map(|c| c.iter().collect())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_short_message() {
        let msg = "Hello";
        assert_eq!(TelegramPusher::split_message(msg), vec!["Hello"]);
    }

    #[test]
    fn test_split_long_message() {
        let msg = "x".repeat(5000);
        let chunks = TelegramPusher::split_message(&msg);
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.len() <= MAX_MESSAGE_LEN));
    }
}
