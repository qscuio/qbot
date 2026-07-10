#[cfg(test)]
mod tests {
    use sqlx::PgPool;

    #[sqlx::test(migrations = "./migrations")]
    async fn point_in_time_tables_exist(pool: PgPool) -> sqlx::Result<()> {
        let tables: Vec<(String,)> = sqlx::query_as(
            r#"SELECT table_name
               FROM information_schema.tables
               WHERE table_schema = 'public'
                 AND table_name = ANY($1)
               ORDER BY table_name"#,
        )
        .bind(vec![
            "analysis_data_runs".to_string(),
            "corporate_action_versions".to_string(),
            "index_daily_bars".to_string(),
            "limit_up_stock_versions".to_string(),
            "market_daily_snapshots".to_string(),
            "security_daily_status".to_string(),
            "security_master_versions".to_string(),
            "sector_daily_versions".to_string(),
            "stock_adjustment_factors".to_string(),
            "stock_daily_bar_versions".to_string(),
            "stock_daily_basic_versions".to_string(),
            "stock_sector_membership".to_string(),
        ])
        .fetch_all(&pool)
        .await?;

        assert_eq!(tables.len(), 12);
        Ok(())
    }
}
