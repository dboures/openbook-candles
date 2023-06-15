use deadpool_postgres::Pool;

use crate::{
    structs::{candle::Candle, openbook::OpenBookFillEvent, transaction::PgTransaction},
    utils::{to_timestampz, AnyhowWrap},
};

pub async fn insert_fills_atomically(
    pool: &Pool,
    worker_id: i32,
    fills: Vec<OpenBookFillEvent>,
    signatures: Vec<String>,
) -> anyhow::Result<()> {
    let mut client = pool.get().await?;

    let db_txn = client.build_transaction().start().await?;

    // 1. Insert fills
    if !fills.is_empty() {
        let fills_statement = build_fills_upsert_statement(fills);
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

fn build_fills_upsert_statement(fills: Vec<OpenBookFillEvent>) -> String {
    let mut stmt = String::from("INSERT INTO fills (signature, time, market, open_orders, open_orders_owner, bid, maker, native_qty_paid, native_qty_received, native_fee_or_rebate, fee_tier, order_id, log_index) VALUES");
    for (idx, fill) in fills.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', \'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {}, {})",
            fill.signature,
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
