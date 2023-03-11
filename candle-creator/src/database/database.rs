use chrono::Utc;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{error::TryRecvError, Receiver};

use crate::{
    trade_fetching::parsing::OpenBookFillEventLog,
    utils::{AnyhowWrap, Config},
};

use super::MarketInfo;

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

pub async fn setup_database(pool: &Pool<Postgres>, markets: Vec<MarketInfo>) -> anyhow::Result<()> {
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

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS candles (
            id serial,
            market text,
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

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_market_time_resolution ON candles (market, start_time, resolution)"
    ).execute(&mut tx).await?;

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

pub async fn save_candles() {
    unimplemented!("TODO");
}

pub async fn handle_fill_events(
    pool: &Pool<Postgres>,
    mut fill_receiver: Receiver<OpenBookFillEventLog>,
) {
    loop {
        let start = Instant::now();
        let mut write_batch = Vec::new();
        while write_batch.len() < 10 || start.elapsed().as_secs() > 10 {
            match fill_receiver.try_recv() {
                Ok(event) => {
                    if !write_batch.contains(&event) {
                        // O(n)
                        write_batch.push(event)
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    panic!("sender must stay alive")
                }
            };
        }

        if write_batch.len() > 0 {
            print!("writing: {:?} events to DB\n", write_batch.len());
            let upsert_statement = build_fills_upsert_statement(write_batch);
            sqlx::query(&upsert_statement)
                .execute(pool)
                .await
                .map_err_anyhow()
                .unwrap();
        }
    }
}

fn build_fills_upsert_statement(events: Vec<OpenBookFillEventLog>) -> String {
    let mut stmt = String::from("INSERT INTO fills (id, time, market, open_orders, open_orders_owner, bid, maker, native_qty_paid, native_qty_received, native_fee_or_rebate, fee_tier, order_id) VALUES");
    for (idx, event) in events.iter().enumerate() {
        let mut hasher = DefaultHasher::new();
        event.hash(&mut hasher);
        let val_str = format!(
            "({}, \'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {})",
            hasher.finish(),
            Utc::now().to_rfc3339(),
            event.market,
            event.open_orders,
            event.open_orders_owner,
            event.bid,
            event.maker,
            event.native_qty_paid,
            event.native_qty_received,
            event.native_fee_or_rebate,
            event.fee_tier,
            event.order_id,
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT (id) DO UPDATE SET market=excluded.market";

    stmt = format!("{} {}", stmt, handle_conflict);
    print!("{}", stmt);
    stmt
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    #[test]
    fn test_event_hashing() {
        let event_1 = OpenBookFillEventLog {
            market: Pubkey::from_str("8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6").unwrap(),
            open_orders: Pubkey::from_str("CKo9nGfgekYYfjHw4K22qMAtVeqBXET3pSGm8k5DSJi7").unwrap(),
            open_orders_owner: Pubkey::from_str("JCNCMFXo5M5qwUPg2Utu1u6YWp3MbygxqBsBeXXJfrw")
                .unwrap(),
            bid: false,
            maker: false,
            native_qty_paid: 200000000,
            native_qty_received: 4204317,
            native_fee_or_rebate: 1683,
            order_id: 387898134381964481824213,
            owner_slot: 0,
            fee_tier: 0,
            client_order_id: None,
            referrer_rebate: Some(841),
        };

        let event_2 = OpenBookFillEventLog {
            market: Pubkey::from_str("8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6").unwrap(),
            open_orders: Pubkey::from_str("CKo9nGfgekYYfjHw4K22qMAtVeqBXET3pSGm8k5DSJi7").unwrap(),
            open_orders_owner: Pubkey::from_str("JCNCMFXo5M5qwUPg2Utu1u6YWp3MbygxqBsBeXXJfrw")
                .unwrap(),
            bid: false,
            maker: false,
            native_qty_paid: 200000001,
            native_qty_received: 4204317,
            native_fee_or_rebate: 1683,
            order_id: 387898134381964481824213,
            owner_slot: 0,
            fee_tier: 0,
            client_order_id: None,
            referrer_rebate: Some(841),
        };

        let mut h1 = DefaultHasher::new();
        event_1.hash(&mut h1);

        let mut h2 = DefaultHasher::new();
        event_2.hash(&mut h2);

        assert_ne!(h1.finish(), h2.finish());
    }
}
