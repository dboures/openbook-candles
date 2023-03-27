use chrono::{DateTime, Utc};
use sqlx::{pool::PoolConnection, Postgres};

use crate::{
    structs::{candle::Candle, openbook::PgOpenBookFill, resolution::Resolution, trader::PgTrader},
    utils::AnyhowWrap,
};

pub async fn fetch_earliest_fill(
    conn: &mut PoolConnection<Postgres>,
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
         and maker = true
         ORDER BY time asc LIMIT 1"#,
        market_address_string
    )
    .fetch_optional(conn)
    .await
    .map_err_anyhow()
}

pub async fn fetch_fills_from(
    conn: &mut PoolConnection<Postgres>,
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
         and maker = true
         ORDER BY time asc"#,
        market_address_string,
        start_time,
        end_time
    )
    .fetch_all(conn)
    .await
    .map_err_anyhow()
}

pub async fn fetch_latest_finished_candle(
    conn: &mut PoolConnection<Postgres>,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Option<Candle>> {
    sqlx::query_as!(
        Candle,
        r#"SELECT 
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        market_name as "market_name!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market_name = $1
        and resolution = $2
        and complete = true
        ORDER BY start_time desc LIMIT 1"#,
        market_name,
        resolution.to_string()
    )
    .fetch_optional(conn)
    .await
    .map_err_anyhow()
}

pub async fn fetch_earliest_candles(
    conn: &mut PoolConnection<Postgres>,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Vec<Candle>> {
    sqlx::query_as!(
        Candle,
        r#"SELECT 
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        market_name as "market_name!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market_name = $1
        and resolution = $2
        ORDER BY start_time asc"#,
        market_name,
        resolution.to_string()
    )
    .fetch_all(conn)
    .await
    .map_err_anyhow()
}

pub async fn fetch_candles_from(
    conn: &mut PoolConnection<Postgres>,
    market_name: &str,
    resolution: Resolution,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    sqlx::query_as!(
        Candle,
        r#"SELECT 
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        market_name as "market_name!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market_name = $1
        and resolution = $2
        and start_time >= $3
        and end_time <= $4
        ORDER BY start_time asc"#,
        market_name,
        resolution.to_string(),
        start_time,
        end_time
    )
    .fetch_all(conn)
    .await
    .map_err_anyhow()
}

pub async fn fetch_tradingview_candles(
    conn: &mut PoolConnection<Postgres>,
    market_name: &str,
    resolution: Resolution,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    sqlx::query_as!(
        Candle,
        r#"SELECT 
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        market_name as "market_name!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market_name = $1
        and resolution = $2
        and start_time >= $3
        and end_time <= $4
        ORDER BY start_time asc"#, // TODO: order?
        market_name,
        resolution.to_string(),
        start_time,
        end_time
    )
    .fetch_all(conn)
    .await
    .map_err_anyhow()
}

pub async fn fetch_top_traders_by_base_volume_from(
    conn: &mut PoolConnection<Postgres>,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgTrader>> {
    sqlx::query_as!(
        PgTrader,
        r#"SELECT 
        open_orders_owner, 
        sum(
          native_qty_paid * CASE bid WHEN true THEN 0 WHEN false THEN 1 END
        ) as "raw_ask_size!",
        sum(
          native_qty_received * CASE bid WHEN true THEN 1 WHEN false THEN 0 END
        ) as "raw_bid_size!"
      FROM fills
 WHERE  market = $1
        AND time >= $2
        AND time < $3
 GROUP  BY open_orders_owner
 ORDER  BY 
    sum(native_qty_paid * CASE bid WHEN true THEN 0 WHEN false THEN 1 END) 
    + 
    sum(native_qty_received * CASE bid WHEN true THEN 1 WHEN false THEN 0 END) 
DESC 
LIMIT 10000"#,
        market_address_string,
        start_time,
        end_time
    )
    .fetch_all(conn)
    .await
    .map_err_anyhow()
}

pub async fn fetch_top_traders_by_quote_volume_from(
    conn: &mut PoolConnection<Postgres>,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgTrader>> {
    sqlx::query_as!(
        PgTrader,
        r#"SELECT 
        open_orders_owner, 
        sum(
            native_qty_received * CASE bid WHEN true THEN 0 WHEN false THEN 1 END
        ) as "raw_ask_size!",
        sum(
            native_qty_paid * CASE bid WHEN true THEN 1 WHEN false THEN 0 END
        ) as "raw_bid_size!"
      FROM fills
 WHERE  market = $1
        AND time >= $2
        AND time < $3
 GROUP  BY open_orders_owner
 ORDER  BY 
    sum(native_qty_received * CASE bid WHEN true THEN 0 WHEN false THEN 1 END) 
    + 
    sum(native_qty_paid * CASE bid WHEN true THEN 1 WHEN false THEN 0 END) 
DESC  
LIMIT 10000"#,
        market_address_string,
        start_time,
        end_time
    )
    .fetch_all(conn)
    .await
    .map_err_anyhow()
}
