use std::cmp::min;

use chrono::{DateTime, Duration, DurationRound, Utc};
use deadpool_postgres::Pool;

use crate::{
    database::fetch::{fetch_earliest_fill, fetch_fills_from, fetch_latest_finished_candle},
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
            let candles = combine_fills_into_1m_candles(
                &mut fills,
                market,
                start_time,
                end_time,
                Some(candle.close),
            );
            Ok(candles)
        }
        None => {
            let earliest_fill = fetch_earliest_fill(pool, market_address).await?;

            if earliest_fill.is_none() {
                println!("No fills found for: {:?}", market_name);
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
            if fills.len() > 0 {
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
    let mut start_time = st.clone();
    let mut end_time = start_time + Duration::minutes(1);

    let mut last_price = match maybe_last_price {
        Some(p) => p,
        None => {
            let first = fills_iter.peek().clone().unwrap();
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
        end_time = end_time + Duration::minutes(1);
    }

    candles
}
