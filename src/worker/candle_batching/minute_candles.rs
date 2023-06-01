use std::cmp::min;

use chrono::{DateTime, Duration, DurationRound, Utc};
use deadpool_postgres::Pool;
use log::debug;

use crate::{
    database::fetch::{fetch_earliest_fill, fetch_fills_from, fetch_latest_finished_candle, fetch_candles_from},
    structs::{
        candle::Candle,
        markets::MarketInfo,
        openbook::{calculate_fill_price_and_size, PgOpenBookFill},
        resolution::{day, Resolution},
    },
    utils::{f64_max, f64_min},
};

pub async fn batch_1m_candles(pool: &Pool, market: &MarketInfo) -> anyhow::Result<Vec<Candle>> {
    let market_name = &market.name;
    let market_address = &market.address;
    let latest_candle = fetch_latest_finished_candle(pool, market_name, Resolution::R1m).await?;

    match latest_candle {
        Some(candle) => {
            let start_time = candle.end_time;
            let end_time = min(
                start_time + day(),
                Utc::now().duration_trunc(Duration::minutes(1))?,
            );
            let mut fills = fetch_fills_from(pool, market_address, start_time, end_time).await?;
            let existing_candles = fetch_candles_from(pool, market_name, Resolution::R1m, candle.start_time, end_time).await?;

            let candles = combine_fills_into_1m_candles(
                &mut fills,
                market,
                start_time,
                end_time,
                Some(candle.close),
            );
            Ok(filter_redundant_candles(existing_candles, candles.clone()))
        }
        None => {
            let earliest_fill = fetch_earliest_fill(pool, market_address).await?;

            if earliest_fill.is_none() {
                debug!("No fills found for: {:?}", market_name);
                return Ok(Vec::new());
            }

            let start_time = earliest_fill
                .unwrap()
                .time
                .duration_trunc(Duration::minutes(1))?;
            let end_time = min(
                start_time + day(),
                Utc::now().duration_trunc(Duration::minutes(1))?,
            );
            let mut fills = fetch_fills_from(pool, market_address, start_time, end_time).await?;
            if !fills.is_empty() {
                let candles =
                    combine_fills_into_1m_candles(&mut fills, market, start_time, end_time, None);
                Ok(candles)
            } else {
                Ok(Vec::new())
            }
        }
    }
}

fn combine_fills_into_1m_candles(
    fills: &mut Vec<PgOpenBookFill>,
    market: &MarketInfo,
    st: DateTime<Utc>,
    et: DateTime<Utc>,
    maybe_last_price: Option<f64>,
) -> Vec<Candle> {
    let empty_candle = Candle::create_empty_candle(market.name.clone(), Resolution::R1m);

    let minutes = (et - st).num_minutes();
    let mut candles = vec![empty_candle; minutes as usize];

    let mut fills_iter = fills.iter_mut().peekable();
    let mut start_time = st;
    let mut end_time = start_time + Duration::minutes(1);

    let mut last_price = match maybe_last_price {
        Some(p) => p,
        None => {
            let first = fills_iter.peek().unwrap();
            let (price, _) =
                calculate_fill_price_and_size(**first, market.base_decimals, market.quote_decimals);
            price
        }
    };

    for i in 0..candles.len() {
        candles[i].open = last_price;
        candles[i].close = last_price;
        candles[i].low = last_price;
        candles[i].high = last_price;

        while matches!(fills_iter.peek(), Some(f) if f.time < end_time) {
            let fill = fills_iter.next().unwrap();

            let (price, volume) =
                calculate_fill_price_and_size(*fill, market.base_decimals, market.quote_decimals);

            candles[i].close = price;
            candles[i].low = f64_min(price, candles[i].low);
            candles[i].high = f64_max(price, candles[i].high);
            candles[i].volume += volume;

            last_price = price;
        }

        candles[i].start_time = start_time;
        candles[i].end_time = end_time;
        candles[i].complete = matches!(fills_iter.peek(), Some(f) if f.time > end_time);

        start_time = end_time;
        end_time += Duration::minutes(1);
    }

    candles
}

fn filter_redundant_candles(existing_candles: Vec<Candle>, mut candles: Vec<Candle>) -> Vec<Candle> {
    candles.retain(|c| {
        !existing_candles.contains(c)
    });
    println!("trimmed: {:?}", candles.len());
    // println!("{:?}", candles.last());
    println!("candles: {:?}", existing_candles.len());
    // println!("{:?}", existing_candles.last());
    candles
}

/// Goes from the earliest fill to the most recent. Will mark candles as complete if there are missing gaps of fills between the start and end.
pub async fn backfill_batch_1m_candles(
    pool: &Pool,
    market: &MarketInfo,
) -> anyhow::Result<Vec<Candle>> {
    let market_name = &market.name;
    let market_address = &market.address;
    let mut candles = vec![];

    let earliest_fill = fetch_earliest_fill(pool, &market.address).await?;
    if earliest_fill.is_none() {
        debug!("No fills found for: {:?}", &market_name);
        return Ok(candles);
    }

    let mut start_time = earliest_fill
        .unwrap()
        .time
        .duration_trunc(Duration::minutes(1))?;
    while start_time < Utc::now() {
        let end_time = min(
            start_time + day(),
            Utc::now().duration_trunc(Duration::minutes(1))?,
        );
        let mut fills = fetch_fills_from(pool, market_address, start_time, end_time).await?;
        if !fills.is_empty() {
            let mut minute_candles =
                combine_fills_into_1m_candles(&mut fills, market, start_time, end_time, None);
            candles.append(&mut minute_candles);
        }
        start_time += day()
    }
    Ok(candles)
}
