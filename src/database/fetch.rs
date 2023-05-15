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

    let stmt = client
        .prepare(
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
        )
        .await?;

    let row = client.query_opt(&stmt, &[&market_address_string]).await?;

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

    let stmt = client
        .prepare(
            r#"SELECT 
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
         ORDER BY time asc"#,
        )
        .await?;

    let rows = client
        .query(&stmt, &[&market_address_string, &start_time, &end_time])
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| PgOpenBookFill::from_row(r))
        .collect())
}

pub async fn fetch_latest_finished_candle(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Option<Candle>> {
    let client = pool.get().await?;

    let stmt = client
        .prepare(
            r#"SELECT 
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
        ORDER BY start_time desc LIMIT 1"#,
        )
        .await?;

    let row = client
        .query_opt(&stmt, &[&market_name, &resolution.to_string()])
        .await?;

    match row {
        Some(r) => Ok(Some(Candle::from_row(r))),
        None => Ok(None),
    }
}

pub async fn fetch_earliest_candles(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Vec<Candle>> {
    let client = pool.get().await?;

    let stmt = client
        .prepare(
            r#"SELECT 
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
        ORDER BY start_time asc"#,
        )
        .await?;

    let rows = client
        .query(&stmt, &[&market_name, &resolution.to_string()])
        .await?;

    Ok(rows.into_iter().map(|r| Candle::from_row(r)).collect())
}

pub async fn fetch_candles_from(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    let client = pool.get().await?;

    let stmt = client
        .prepare(
            r#"SELECT 
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
        ORDER BY start_time asc"#,
        )
        .await?;

    let rows = client
        .query(
            &stmt,
            &[
                &market_name,
                &resolution.to_string(),
                &start_time,
                &end_time,
            ],
        )
        .await?;

    Ok(rows.into_iter().map(|r| Candle::from_row(r)).collect())
}

pub async fn fetch_top_traders_by_base_volume_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgTrader>> {
    let client = pool.get().await?;

    let stmt = client
        .prepare(
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
        )
        .await?;

    let rows = client
        .query(&stmt, &[&market_address_string, &start_time, &end_time])
        .await?;

    Ok(rows.into_iter().map(|r| PgTrader::from_row(r)).collect())
}

pub async fn fetch_top_traders_by_quote_volume_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgTrader>> {
    let client = pool.get().await?;

    let stmt = client
        .prepare(
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
        )
        .await?;

    let rows = client
        .query(&stmt, &[&market_address_string, &start_time, &end_time])
        .await?;

    Ok(rows.into_iter().map(|r| PgTrader::from_row(r)).collect())
}

pub async fn fetch_coingecko_24h_volume(
    pool: &Pool,
) -> anyhow::Result<Vec<PgCoinGecko24HourVolume>> {
    let client = pool.get().await?;

    let stmt = client
        .prepare(
            r#"select market as "address!",
        sum(native_qty_paid) as "raw_quote_size!",
        sum(native_qty_received) as "raw_base_size!"
        from fills 
        where "time" >= current_timestamp - interval '1 day' 
        and bid = true
        group by market"#,
        )
        .await?;

    let rows = client.query(&stmt, &[]).await?;

    Ok(rows
        .into_iter()
        .map(|r| PgCoinGecko24HourVolume::from_row(r))
        .collect())
}

pub async fn fetch_coingecko_24h_high_low(
    pool: &Pool,
) -> anyhow::Result<Vec<PgCoinGecko24HighLow>> {
    let client = pool.get().await?;

    let stmt = client
        .prepare(
            r#"select 
        g.market_name as "market_name!", 
        g.high as "high!", 
        g.low as "low!", 
        c."close" as "close!"
      from 
        (
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
          group by 
            market_name
        ) as g 
        join candles c on g.market_name = c.market_name 
        and g.start_time = c.start_time 
      where 
        c.resolution = '1M'"#,
        )
        .await?;

    let rows = client.query(&stmt, &[]).await?;

    Ok(rows
        .into_iter()
        .map(|r| PgCoinGecko24HighLow::from_row(r))
        .collect())
}
