use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres};

use crate::{database::PgOpenBookFill, utils::AnyhowWrap};

use super::{Candle, Resolution};

// use super::PgMarketInfo;

pub async fn fetch_earliest_fill(
    pool: &Pool<Postgres>,
    market_address_string: &str,
) -> anyhow::Result<Option<PgOpenBookFill>> {
    sqlx::query_as!(
        PgOpenBookFill,
        r#"SELECT 
         time as "time!",
         bid as "bid!",
         maker as "maker!",
         native_qty_paid as "native_qty_paid!",
         native_qty_received as "native_qty_received!",
         native_fee_or_rebate as "native_fee_or_rebate!" 
         from fills 
         where market = $1 
         ORDER BY time asc LIMIT 1"#,
        market_address_string
    )
    .fetch_optional(pool)
    .await
    .map_err_anyhow()
}

pub async fn fetch_fills_from(
    pool: &Pool<Postgres>,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgOpenBookFill>> {
    sqlx::query_as!(
        PgOpenBookFill,
        r#"SELECT 
         time as "time!",
         bid as "bid!",
         maker as "maker!",
         native_qty_paid as "native_qty_paid!",
         native_qty_received as "native_qty_received!",
         native_fee_or_rebate as "native_fee_or_rebate!" 
         from fills 
         where market = $1
         and time >= $2
         and time < $3 
         ORDER BY time asc"#,
        market_address_string,
        start_time,
        end_time
    )
    .fetch_all(pool)
    .await
    .map_err_anyhow()
}

pub async fn fetch_latest_finished_candle(
    pool: &Pool<Postgres>,
    market_address_string: &str,
    resolution: Resolution,
) -> anyhow::Result<Option<Candle>> {
    sqlx::query_as!(
        Candle,
        r#"SELECT 
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        market as "market!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market = $1
        and resolution = $2
        and complete = true
        ORDER BY start_time desc LIMIT 1"#,
        market_address_string,
        resolution.to_string()
    )
    .fetch_optional(pool)
    .await
    .map_err_anyhow()
}

// fetch_candles
