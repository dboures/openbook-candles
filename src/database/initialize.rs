use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::time::Duration;

use crate::utils::{AnyhowWrap, Config};

pub async fn connect_to_database(config: &Config) -> anyhow::Result<Pool<Postgres>> {
    loop {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_pg_pool_connections)
            .connect(&config.database_url)
            .await;
        if pool.is_ok() {
            println!("Database connected");
            return pool.map_err_anyhow();
        }
        println!("Failed to connect to database, retrying");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub async fn setup_database(pool: &Pool<Postgres>) -> anyhow::Result<()> {
    let candles_table_fut = create_candles_table(pool);
    let fills_table_fut = create_fills_table(pool);
    let result = tokio::try_join!(candles_table_fut, fills_table_fut);
    match result {
        Ok(_) => {
            println!("Successfully configured database");
            Ok(())
        }
        Err(e) => {
            println!("Failed to configure database: {e}");
            Err(e)
        }
    }
}

pub async fn create_candles_table(pool: &Pool<Postgres>) -> anyhow::Result<()> {
    let mut tx = pool.begin().await.map_err_anyhow()?;

    sqlx::query!(
        "CREATE TABLE IF NOT EXISTS candles (
            id serial,
            market_name text,
            start_time timestamptz,
            end_time timestamptz,
            resolution text,
            open numeric,
            close numeric,
            high numeric,
            low numeric,
            volume numeric,
            complete bool
        )",
    )
    .execute(&mut tx)
    .await?;

    sqlx::query!(
        "CREATE INDEX IF NOT EXISTS idx_market_time_resolution ON candles (market_name, start_time, resolution)"
    ).execute(&mut tx).await?;

    sqlx::query!(
        "DO $$
            BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_candles') THEN
                ALTER TABLE candles ADD CONSTRAINT unique_candles UNIQUE (market_name, start_time, resolution);
            END IF;
        END $$"
    )
    .execute(&mut tx)
    .await?;

    tx.commit().await.map_err_anyhow()
}

pub async fn create_fills_table(pool: &Pool<Postgres>) -> anyhow::Result<()> {
    let mut tx = pool.begin().await.map_err_anyhow()?;

    sqlx::query!(
        "CREATE TABLE IF NOT EXISTS fills (
            id numeric PRIMARY KEY,
            time timestamptz not null,
            market text not null,
            open_orders text not null,
            open_orders_owner text not null,
            bid bool not null,
            maker bool not null,
            native_qty_paid numeric not null,
            native_qty_received numeric not null,
            native_fee_or_rebate numeric not null,
            fee_tier text not null,
            order_id text not null
        )",
    )
    .execute(&mut tx)
    .await?;

    sqlx::query!("CREATE INDEX IF NOT EXISTS idx_id_market ON fills (id, market)")
        .execute(&mut tx)
        .await?;

    sqlx::query!("CREATE INDEX IF NOT EXISTS idx_market_time ON fills (market, time)")
        .execute(&mut tx)
        .await?;

    tx.commit().await.map_err_anyhow()
}
