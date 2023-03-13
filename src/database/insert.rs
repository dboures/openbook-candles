use chrono::Utc;
use sqlx::{Pool, Postgres};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    time::Instant,
};
use tokio::sync::mpsc::{error::TryRecvError, Receiver};

use crate::{candle_creation::trade_fetching::parsing::OpenBookFillEventLog, utils::AnyhowWrap};

use super::Candle;

pub async fn persist_fill_events(
    pool: &Pool<Postgres>,
    mut fill_receiver: Receiver<OpenBookFillEventLog>,
) {
    loop {
        let start = Instant::now();
        let mut write_batch = HashMap::new();
        while write_batch.len() < 10 || start.elapsed().as_secs() > 10 {
            match fill_receiver.try_recv() {
                Ok(event) => {
                    if !write_batch.contains_key(&event) {
                        write_batch.insert(event, 0);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    panic!("Fills sender must stay alive")
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

fn build_fills_upsert_statement(events: HashMap<OpenBookFillEventLog, u8>) -> String {
    let mut stmt = String::from("INSERT INTO fills (id, time, market, open_orders, open_orders_owner, bid, maker, native_qty_paid, native_qty_received, native_fee_or_rebate, fee_tier, order_id) VALUES");
    for (idx, event) in events.keys().enumerate() {
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
    stmt
}

pub async fn persist_candles(pool: Pool<Postgres>, mut candles_receiver: Receiver<Vec<Candle>>) {
    loop {
        match candles_receiver.try_recv() {
            Ok(candles) => {
                if candles.len() == 0 {
                    continue;
                }
                print!("writing: {:?} candles to DB\n", candles.len());
                match candles.last() {
                    Some(c) => {
                        println!("{:?}\n\n", c.end_time)
                    }
                    None => {}
                }
                let upsert_statement = build_candes_upsert_statement(candles);
                sqlx::query(&upsert_statement)
                    .execute(&pool)
                    .await
                    .map_err_anyhow()
                    .unwrap();
            }
            Err(TryRecvError::Empty) => continue,
            Err(TryRecvError::Disconnected) => {
                panic!("Candles sender must stay alive")
            }
        };
    }
}

fn build_candes_upsert_statement(candles: Vec<Candle>) -> String {
    let mut stmt = String::from("INSERT INTO candles (market, start_time, end_time, resolution, open, close, high, low, volume, complete) VALUES");
    for (idx, candle) in candles.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {})",
            candle.market,
            candle.start_time.to_rfc3339(),
            candle.end_time.to_rfc3339(),
            candle.resolution,
            candle.open,
            candle.close,
            candle.high,
            candle.low,
            candle.volume,
            candle.complete,
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT (market, start_time, resolution) 
    DO UPDATE SET 
    open=excluded.open, 
    close=excluded.close, 
    high=excluded.high, 
    low=excluded.low,
    volume=excluded.volume,
    complete=excluded.complete
    ";

    stmt = format!("{} {}", stmt, handle_conflict);
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
