use crate::structs::{
    candle::Candle,
    coingecko::{PgCoinGecko24HighLow, PgCoinGecko24HourVolume},
    openbook::PgOpenBookFill,
    resolution::Resolution,
    trader::PgTrader,
};
use chrono::{DateTime, Utc};
use deadpool_postgres::{GenericClient, Pool};

pub async fn fetch_earliest_fill(
    pool: &Pool,
    market_address_string: &str,
) -> anyhow::Result<Option<PgOpenBookFill>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        time as "time!",
        bid as "bid!",
        maker as "maker!",
        native_qty_paid as "native_qty_paid!",
        native_qty_received as "native_qty_received!",
        native_fee_or_rebate as "native_fee_or_rebate!" 
        from fills 
        where market = $1 
        and maker = true
        ORDER BY time asc LIMIT 1"#;

    let row = client.query_opt(stmt, &[&market_address_string]).await?;

    match row {
        Some(r) => Ok(Some(PgOpenBookFill::from_row(r))),
        None => Ok(None),
    }
}

pub async fn fetch_fills_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgOpenBookFill>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
         time as "time!",
         bid as "bid!",
         maker as "maker!",
         native_qty_paid as "native_qty_paid!",
         native_qty_received as "native_qty_received!",
         native_fee_or_rebate as "native_fee_or_rebate!" 
         from fills 
         where market = $1
         and time >= $2::timestamptz
         and time < $3::timestamptz
         and maker = true
         ORDER BY time asc"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;
    Ok(rows.into_iter().map(PgOpenBookFill::from_row).collect())
}

pub async fn fetch_latest_finished_candle(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Option<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name!",
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
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
        ORDER BY start_time desc LIMIT 1"#;

    let row = client
        .query_opt(stmt, &[&market_name, &resolution.to_string()])
        .await?;

    match row {
        Some(r) => Ok(Some(Candle::from_row(r))),
        None => Ok(None),
    }
}

/// Fetches all of the candles for the given market and resoultion, starting from the earliest.
/// Note that this function will fetch ALL candles.
pub async fn fetch_earliest_candles(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Vec<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name!",
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market_name = $1
        and resolution = $2
        ORDER BY start_time asc"#;

    let rows = client
        .query(stmt, &[&market_name, &resolution.to_string()])
        .await?;

    Ok(rows.into_iter().map(Candle::from_row).collect())
}

pub async fn fetch_candles_from(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name!",
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
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
        ORDER BY start_time asc"#;

    let rows = client
        .query(
            stmt,
            &[
                &market_name,
                &resolution.to_string(),
                &start_time,
                &end_time,
            ],
        )
        .await?;

    Ok(rows.into_iter().map(Candle::from_row).collect())
}

pub async fn fetch_top_traders_by_base_volume_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgTrader>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
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
    LIMIT 10000"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;

    Ok(rows.into_iter().map(PgTrader::from_row).collect())
}

pub async fn fetch_top_traders_by_quote_volume_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgTrader>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
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
    LIMIT 10000"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;

    Ok(rows.into_iter().map(PgTrader::from_row).collect())
}

pub async fn fetch_coingecko_24h_volume(
    pool: &Pool,
    market_address_strings: &Vec<&str>,
) -> anyhow::Result<Vec<PgCoinGecko24HourVolume>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
            t1.market, 
            COALESCE(t2.native_qty_received, 0) as "raw_base_size!",
            COALESCE(t2.native_qty_paid, 0) as "raw_quote_size!"
        FROM (
            SELECT distinct on (market) *
            FROM fills f
            where bid = true
            and market = any($1) 
        order by market, "time" desc
        ) t1
        LEFT JOIN (
            select market,
            sum(native_qty_received) as "native_qty_received",
            sum(native_qty_paid) as "native_qty_paid"
            from fills 
            where "time" >= current_timestamp - interval '1 day' 
            and bid = true
            group by market
        ) t2 ON t1.market = t2.market"#;

    let rows = client.query(stmt, &[&market_address_strings]).await?;

    Ok(rows
        .into_iter()
        .map(PgCoinGecko24HourVolume::from_row)
        .collect())
}

pub async fn fetch_coingecko_24h_high_low(
    pool: &Pool,
    market_names: &Vec<&str>,
) -> anyhow::Result<Vec<PgCoinGecko24HighLow>> {
    let client = pool.get().await?;

    let stmt = r#"select 
            r.market_name as "market_name!", 
            coalesce(c.high, r.high) as "high!", 
            coalesce(c.low, r.low) as "low!", 
            r."close" as "close!"
          from 
            (
              SELECT *
              from 
                candles 
               where (market_name, start_time, resolution) in (
                select market_name, max(start_time), resolution 
                from candles 
                where "resolution" = '1M' 
                and market_name = any($1)
                group by market_name, resolution
            )
            ) as r 
            left join (
            SELECT 
                market_name, 
                max(start_time) as "start_time", 
                max(high) as "high", 
                min(low) as "low"
              from 
                candles 
              where 
                "resolution" = '1M' 
                and "start_time" >= current_timestamp - interval '1 day'
                group by market_name
            ) c on r.market_name = c.market_name"#;

    let rows = client.query(stmt, &[&market_names]).await?;

    Ok(rows
        .into_iter()
        .map(PgCoinGecko24HighLow::from_row)
        .collect())
}
