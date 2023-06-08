use deadpool_postgres::Pool;
use log::debug;
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
};
use tokio::sync::mpsc::{error::TryRecvError, Receiver};

use crate::{
    structs::{candle::Candle, openbook::OpenBookFillEvent, transaction::PgTransaction},
    utils::{to_timestampz, AnyhowWrap},
};

pub async fn add_fills_atomically(
    pool: &Pool,
    worker_id: i32,
    fills: Vec<OpenBookFillEvent>,
    signatures: Vec<String>,
) -> anyhow::Result<()> {
    let mut client = pool.get().await?;

    let db_txn = client.build_transaction().start().await?;

    // 1. Insert fills
    if fills.len() > 0 {
        let fills_statement = build_fills_upsert_statement_not_crazy(fills);
        db_txn
            .execute(&fills_statement, &[])
            .await
            .map_err_anyhow()
            .unwrap();
    }

    // 2. Update txns table as processed
    let transactions_statement =
        build_transactions_processed_update_statement(worker_id, signatures);
    db_txn
        .execute(&transactions_statement, &[])
        .await
        .map_err_anyhow()
        .unwrap();

    db_txn.commit().await?;

    Ok(())
}

pub async fn persist_fill_events(
    pool: &Pool,
    fill_receiver: &mut Receiver<OpenBookFillEvent>,
) -> anyhow::Result<()> {
    loop {
        let mut write_batch = HashMap::new();
        while write_batch.len() < 10 {
            match fill_receiver.try_recv() {
                Ok(event) => {
                    write_batch.entry(event).or_insert(0);
                }
                Err(TryRecvError::Empty) => {
                    if !write_batch.is_empty() {
                        break;
                    } else {
                        continue;
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    panic!("Fills sender must stay alive")
                }
            };
        }

        if !write_batch.is_empty() {
            debug!("writing: {:?} events to DB\n", write_batch.len());
            let upsert_statement = build_fills_upsert_statement(write_batch);
            let client = pool.get().await?;
            client
                .execute(&upsert_statement, &[])
                .await
                .map_err_anyhow()
                .unwrap();
        }
    }
}

#[allow(deprecated)]
fn build_fills_upsert_statement(events: HashMap<OpenBookFillEvent, u8>) -> String {
    let mut stmt = String::from("INSERT INTO fills (id, time, market, open_orders, open_orders_owner, bid, maker, native_qty_paid, native_qty_received, native_fee_or_rebate, fee_tier, order_id, log_index) VALUES");
    for (idx, event) in events.keys().enumerate() {
        let mut hasher = DefaultHasher::new();
        event.hash(&mut hasher);
        let val_str = format!(
            "({}, \'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {}, {})",
            hasher.finish(),
            to_timestampz(event.block_time as u64).to_rfc3339(),
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
            event.log_index,
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT DO NOTHING";

    stmt = format!("{} {}", stmt, handle_conflict);
    stmt
}

fn build_fills_upsert_statement_not_crazy(fills: Vec<OpenBookFillEvent>) -> String {
    let mut stmt = String::from("INSERT INTO fills (id, time, market, open_orders, open_orders_owner, bid, maker, native_qty_paid, native_qty_received, native_fee_or_rebate, fee_tier, order_id, log_index) VALUES");
    for (idx, fill) in fills.iter().enumerate() {
        let mut hasher = DefaultHasher::new();
        fill.hash(&mut hasher);
        let val_str = format!(
            "({}, \'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {}, {})",
            hasher.finish(),
            to_timestampz(fill.block_time as u64).to_rfc3339(),
            fill.market,
            fill.open_orders,
            fill.open_orders_owner,
            fill.bid,
            fill.maker,
            fill.native_qty_paid,
            fill.native_qty_received,
            fill.native_fee_or_rebate,
            fill.fee_tier,
            fill.order_id,
            fill.log_index,
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT DO NOTHING";

    stmt = format!("{} {}", stmt, handle_conflict);
    stmt
}

pub fn build_candles_upsert_statement(candles: &Vec<Candle>) -> String {
    let mut stmt = String::from("INSERT INTO candles (market_name, start_time, end_time, resolution, open, close, high, low, volume, complete) VALUES");
    for (idx, candle) in candles.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {})",
            candle.market_name,
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

    let handle_conflict = "ON CONFLICT (market_name, start_time, resolution) 
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

pub fn build_transactions_insert_statement(transactions: Vec<PgTransaction>) -> String {
    let mut stmt = String::from("INSERT INTO transactions (signature, program_pk, block_datetime, slot, err, processed, worker_partition) VALUES");
    for (idx, txn) in transactions.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {})",
            txn.signature,
            txn.program_pk,
            txn.block_datetime.to_rfc3339(),
            txn.slot,
            txn.err,
            txn.processed,
            txn.worker_partition,
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT DO NOTHING";

    stmt = format!("{} {}", stmt, handle_conflict);
    stmt
}

pub fn build_transactions_processed_update_statement(
    worker_id: i32,
    processed_signatures: Vec<String>,
) -> String {
    let mut stmt = String::from(
        "UPDATE transactions
    SET processed = true
    WHERE transactions.signature IN (",
    );
    for (idx, sig) in processed_signatures.iter().enumerate() {
        let val_str = if idx == processed_signatures.len() - 1 {
            format!("\'{}\'", sig,)
        } else {
            format!("\'{}\',", sig,)
        };
        stmt = format!("{} {}", &stmt, val_str);
    }

    let worker_stmt = format!(") AND worker_partition = {} ", worker_id);

    stmt = format!("{} {}", stmt, worker_stmt);
    stmt
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    #[test]
    fn test_event_hashing() {
        let event_1 = OpenBookFillEvent {
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
            block_time: 0,
            log_index: 1,
        };

        let event_2 = OpenBookFillEvent {
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
            block_time: 0,
            log_index: 1,
        };

        let mut h1 = DefaultHasher::new();
        event_1.hash(&mut h1);

        let mut h2 = DefaultHasher::new();
        event_2.hash(&mut h2);

        assert_ne!(h1.finish(), h2.finish());
    }
}
