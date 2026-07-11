use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::error::{AppError, Result};
use crate::services::scan_ranker::{
    ranked_pool_evidence, POOL_LONG_A_ID, POOL_LONG_B_ID, POOL_MID_A_ID, POOL_MID_B_ID,
    POOL_SHORT_A_ID, POOL_SHORT_B_ID,
};
use crate::services::scanner::SignalHit;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaselineCandidate {
    pub code: String,
    pub name: String,
    pub horizon: String,
    pub line_type: String,
    pub pool_id: String,
    pub pool_name: String,
    pub tier: String,
    pub base_source: String,
    pub base_score: f64,
    pub trigger_id: String,
    pub trigger_name: String,
    pub reasons: Vec<String>,
    pub risk_flags: Vec<String>,
    pub factor_breakdown: Vec<(String, f64)>,
}

pub async fn load_scan_ranker_baseline(
    pool: &PgPool,
    trade_date: NaiveDate,
) -> Result<Vec<BaselineCandidate>> {
    let rows: Vec<(String, String, String, String, String, serde_json::Value)> = sqlx::query_as(
        r#"WITH latest_ranked_pool_run AS (
               SELECT run_id
               FROM daily_signal_scan_results
               WHERE scan_date = $1
                 AND signal_id IN ($2, $3, $4, $5, $6, $7)
               GROUP BY run_id
               ORDER BY MAX(scanned_at) DESC, run_id DESC
               LIMIT 1
           )
           SELECT code, name, signal_id, signal_name, icon, metadata
           FROM daily_signal_scan_results
           WHERE scan_date = $1
             AND run_id = (SELECT run_id FROM latest_ranked_pool_run)
             AND signal_id IN ($2, $3, $4, $5, $6, $7)
           ORDER BY signal_id ASC, code ASC"#,
    )
    .bind(trade_date)
    .bind(POOL_SHORT_A_ID)
    .bind(POOL_SHORT_B_ID)
    .bind(POOL_MID_A_ID)
    .bind(POOL_MID_B_ID)
    .bind(POOL_LONG_A_ID)
    .bind(POOL_LONG_B_ID)
    .fetch_all(pool)
    .await?;

    let mut candidates = Vec::with_capacity(rows.len());
    for (code, name, signal_id, signal_name, icon, metadata) in rows {
        let hit = SignalHit {
            code,
            name,
            signal_id,
            signal_name,
            icon,
            metadata,
        };
        let evidence = ranked_pool_evidence(&hit).ok_or_else(|| {
            AppError::Internal(format!(
                "invalid ranked pool evidence for {} {}",
                hit.signal_id, hit.code
            ))
        })?;
        candidates.push(BaselineCandidate {
            code: hit.code,
            name: hit.name,
            horizon: evidence.line_type.clone(),
            line_type: evidence.line_type,
            pool_id: evidence.pool_id,
            pool_name: hit.signal_name,
            tier: evidence.tier,
            base_source: "scan_ranker".to_string(),
            base_score: evidence.score,
            trigger_id: evidence.trigger_id,
            trigger_name: evidence.trigger_name,
            reasons: evidence.reasons,
            risk_flags: evidence.risk_flags,
            factor_breakdown: evidence.factor_breakdown,
        });
    }

    candidates.sort_by(|left, right| {
        right
            .base_score
            .partial_cmp(&left.base_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.pool_id.cmp(&right.pool_id))
            .then_with(|| left.trigger_id.cmp(&right.trigger_id))
    });

    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::load_scan_ranker_baseline;
    use crate::services::scan_ranker::{ranked_pool_evidence, POOL_MID_A_ID, POOL_SHORT_A_ID};
    use crate::services::scanner::SignalHit;
    use crate::storage::postgres::{save_daily_signal_scan_results, DailySignalScanRow};
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    fn d(value: &str) -> NaiveDate {
        NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap()
    }

    fn ranked_hit(
        pool_id: &str,
        code: &str,
        name: &str,
        score: f64,
        reasons: &[&str],
    ) -> SignalHit {
        SignalHit {
            code: code.to_string(),
            name: name.to_string(),
            signal_id: pool_id.to_string(),
            signal_name: format!("{pool_id}-name"),
            icon: "•".to_string(),
            metadata: json!({
                "line_type": if pool_id.contains("short") { "short" } else { "mid" },
                "tier": "A",
                "trigger_id": "breakout",
                "trigger_name": "突破信号",
                "score": score,
                "reasons": reasons,
                "risk_flags": ["量能不足"],
                "factor_breakdown": [
                    {"name": "trend", "score": 18.5},
                    {"name": "volume", "score": 11.2}
                ],
                "supporting_signals": ["breakout"],
                "matched_setups": [{"id": "breakout", "name": "突破信号"}]
            }),
        }
    }

    #[test]
    fn scan_ranker_adapter_parses_ranked_pool_metadata_without_rescoring() {
        let hit = ranked_hit(
            POOL_SHORT_A_ID,
            "600000.SH",
            "浦发银行",
            91.4,
            &["原始理由", "第二理由"],
        );

        let evidence = ranked_pool_evidence(&hit).expect("expected ranked pool evidence");

        assert_eq!(evidence.pool_id, POOL_SHORT_A_ID);
        assert_eq!(evidence.score, 91.4);
        assert_eq!(
            evidence.reasons,
            vec!["原始理由".to_string(), "第二理由".to_string()]
        );
        assert_eq!(evidence.risk_flags, vec!["量能不足".to_string()]);
        assert_eq!(
            evidence.factor_breakdown,
            vec![("trend".to_string(), 18.5), ("volume".to_string(), 11.2)]
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn scan_ranker_adapter_loads_archived_ranked_pool_rows_without_rescoring(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let scan_date = d("2026-03-09");
        let rows = vec![
            DailySignalScanRow {
                code: "000001.SZ".to_string(),
                name: "平安银行".to_string(),
                signal_id: POOL_MID_A_ID.to_string(),
                signal_name: "中线A档".to_string(),
                icon: "📈".to_string(),
                metadata: ranked_hit(
                    POOL_MID_A_ID,
                    "000001.SZ",
                    "平安银行",
                    88.2,
                    &["中线趋势完整"],
                )
                .metadata,
            },
            DailySignalScanRow {
                code: "600000.SH".to_string(),
                name: "浦发银行".to_string(),
                signal_id: POOL_SHORT_A_ID.to_string(),
                signal_name: "短线A档".to_string(),
                icon: "🔥".to_string(),
                metadata: ranked_hit(
                    POOL_SHORT_A_ID,
                    "600000.SH",
                    "浦发银行",
                    91.4,
                    &["原始理由", "第二理由"],
                )
                .metadata,
            },
        ];

        save_daily_signal_scan_results(&pool, scan_date, Uuid::new_v4(), &rows)
            .await
            .unwrap();

        let baseline = load_scan_ranker_baseline(&pool, scan_date).await.unwrap();

        assert_eq!(baseline.len(), 2);
        assert_eq!(baseline[0].code, "600000.SH");
        assert_eq!(baseline[0].base_score, 91.4);
        assert_eq!(baseline[0].base_source, "scan_ranker");
        assert_eq!(baseline[0].pool_id, POOL_SHORT_A_ID);
        assert_eq!(baseline[0].horizon, "short");
        assert_eq!(
            baseline[0].reasons,
            vec!["原始理由".to_string(), "第二理由".to_string()]
        );
        assert_eq!(baseline[1].code, "000001.SZ");
        assert_eq!(baseline[1].base_score, 88.2);
        assert_eq!(baseline[1].horizon, "mid");
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn scan_ranker_adapter_loads_only_latest_archived_ranked_pool_run(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let scan_date = d("2026-03-09");
        let older_run = Uuid::new_v4();
        let newer_run = Uuid::new_v4();

        let older_rows = vec![
            DailySignalScanRow {
                code: "000001.SZ".to_string(),
                name: "平安银行".to_string(),
                signal_id: POOL_MID_A_ID.to_string(),
                signal_name: "中线A档".to_string(),
                icon: "📈".to_string(),
                metadata: ranked_hit(
                    POOL_MID_A_ID,
                    "000001.SZ",
                    "平安银行",
                    84.0,
                    &["旧中线候选"],
                )
                .metadata,
            },
            DailySignalScanRow {
                code: "300001.SZ".to_string(),
                name: "特锐德".to_string(),
                signal_id: POOL_SHORT_A_ID.to_string(),
                signal_name: "短线A档".to_string(),
                icon: "🔥".to_string(),
                metadata: ranked_hit(
                    POOL_SHORT_A_ID,
                    "300001.SZ",
                    "特锐德",
                    89.5,
                    &["旧短线候选"],
                )
                .metadata,
            },
            DailySignalScanRow {
                code: "600111.SH".to_string(),
                name: "北方稀土".to_string(),
                signal_id: POOL_SHORT_A_ID.to_string(),
                signal_name: "短线A档".to_string(),
                icon: "🔥".to_string(),
                metadata: ranked_hit(
                    POOL_SHORT_A_ID,
                    "600111.SH",
                    "北方稀土",
                    83.4,
                    &["旧额外候选"],
                )
                .metadata,
            },
        ];
        save_daily_signal_scan_results(&pool, scan_date, older_run, &older_rows)
            .await
            .unwrap();

        let newer_rows = vec![
            DailySignalScanRow {
                code: "000001.SZ".to_string(),
                name: "平安银行".to_string(),
                signal_id: POOL_MID_A_ID.to_string(),
                signal_name: "中线A档".to_string(),
                icon: "📈".to_string(),
                metadata: ranked_hit(
                    POOL_MID_A_ID,
                    "000001.SZ",
                    "平安银行",
                    88.8,
                    &["新中线候选"],
                )
                .metadata,
            },
            DailySignalScanRow {
                code: "600010.SH".to_string(),
                name: "包钢股份".to_string(),
                signal_id: POOL_SHORT_A_ID.to_string(),
                signal_name: "短线A档".to_string(),
                icon: "🔥".to_string(),
                metadata: ranked_hit(
                    POOL_SHORT_A_ID,
                    "600010.SH",
                    "包钢股份",
                    93.1,
                    &["新短线候选"],
                )
                .metadata,
            },
        ];
        save_daily_signal_scan_results(&pool, scan_date, newer_run, &newer_rows)
            .await
            .unwrap();

        sqlx::query("UPDATE daily_signal_scan_results SET scanned_at = $1 WHERE run_id = $2")
            .bind(Utc.with_ymd_and_hms(2026, 3, 9, 9, 30, 0).unwrap())
            .bind(older_run)
            .execute(&pool)
            .await?;
        sqlx::query("UPDATE daily_signal_scan_results SET scanned_at = $1 WHERE run_id = $2")
            .bind(Utc.with_ymd_and_hms(2026, 3, 9, 15, 45, 0).unwrap())
            .bind(newer_run)
            .execute(&pool)
            .await?;

        let baseline = load_scan_ranker_baseline(&pool, scan_date).await.unwrap();

        assert_eq!(baseline.len(), 2);
        assert_eq!(
            baseline
                .iter()
                .map(|candidate| candidate.code.as_str())
                .collect::<Vec<_>>(),
            vec!["600010.SH", "000001.SZ"]
        );
        assert_eq!(baseline[0].base_score, 93.1);
        assert_eq!(baseline[1].base_score, 88.8);
        assert_eq!(baseline[0].reasons, vec!["新短线候选".to_string()]);
        assert_eq!(baseline[1].reasons, vec!["新中线候选".to_string()]);
        assert!(baseline
            .iter()
            .all(|candidate| { candidate.code != "300001.SZ" && candidate.code != "600111.SH" }));
        Ok(())
    }
}
