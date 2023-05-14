use chrono::{DateTime, Duration, DurationRound, Utc};
use deadpool_postgres::Pool;
use std::cmp::{max};

use crate::{
    database::fetch::{fetch_candles_from, fetch_earliest_candles, fetch_latest_finished_candle},
    structs::{
        candle::Candle,
        resolution::{day, Resolution},
    },
    utils::{f64_max, f64_min},
};

pub async fn batch_higher_order_candles(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Vec<Candle>> {
    let latest_candle = fetch_latest_finished_candle(pool, market_name, resolution).await?;

    match latest_candle {
        Some(candle) => {
            let start_time = candle.end_time;
            let end_time = start_time + day();
            let mut constituent_candles = fetch_candles_from(
                pool,
                market_name,
                resolution.get_constituent_resolution(),
                start_time,
                end_time,
            )
            .await?;
            if constituent_candles.len() == 0 {
                return Ok(Vec::new());
            }
            let combined_candles = combine_into_higher_order_candles(
                &mut constituent_candles,
                resolution,
                start_time,
                candle,
            );
            Ok(combined_candles)
        }
        None => {
            let mut constituent_candles =
                fetch_earliest_candles(pool, market_name, resolution.get_constituent_resolution())
                    .await?;
            if constituent_candles.len() == 0 {
                // println!(
                //     "Batching {}, but no candles found for: {:?}, {}",
                //     resolution,
                //     market_name,
                //     resolution.get_constituent_resolution()
                // );
                return Ok(Vec::new());
            }
            let start_time = constituent_candles[0].start_time.duration_trunc(day())?;

            if constituent_candles.len() == 0 {
                return Ok(Vec::new());
            }

            let seed_candle = constituent_candles[0].clone();
            let combined_candles = combine_into_higher_order_candles(
                &mut constituent_candles,
                resolution,
                start_time,
                seed_candle,
            );

            Ok(trim_candles(
                combined_candles,
                constituent_candles[0].start_time,
            ))
        }
    }
}

fn combine_into_higher_order_candles(
    constituent_candles: &mut Vec<Candle>,
    target_resolution: Resolution,
    st: DateTime<Utc>,
    seed_candle: Candle,
) -> Vec<Candle> {
    // println!("target_resolution: {}", target_resolution);

    let duration = target_resolution.get_duration();

    let empty_candle = Candle::create_empty_candle(
        constituent_candles[0].market_name.clone(),
        target_resolution,
    );
    let now = Utc::now().duration_trunc(Duration::minutes(1)).unwrap();
    let candle_window = now - st;
    let num_candles = max(
        1,
        (candle_window.num_minutes() / duration.num_minutes()) as usize + 1,
    );

    let mut combined_candles = vec![empty_candle; num_candles];

    let mut con_iter = constituent_candles.iter_mut().peekable();
    let mut start_time = st.clone();
    let mut end_time = start_time + duration;

    let mut last_candle = seed_candle;

    for i in 0..combined_candles.len() {
        combined_candles[i].open = last_candle.close;
        combined_candles[i].low = last_candle.close;
        combined_candles[i].close = last_candle.close;
        combined_candles[i].high = last_candle.close;

        while matches!(con_iter.peek(), Some(c) if c.end_time <= end_time) {
            let unit_candle = con_iter.next().unwrap();
            combined_candles[i].high = f64_max(combined_candles[i].high, unit_candle.high);
            combined_candles[i].low = f64_min(combined_candles[i].low, unit_candle.low);
            combined_candles[i].close = unit_candle.close;
            combined_candles[i].volume += unit_candle.volume;
            combined_candles[i].complete = unit_candle.complete;
            combined_candles[i].end_time = unit_candle.end_time;
        }

        combined_candles[i].start_time = start_time;
        combined_candles[i].end_time = end_time;

        start_time = end_time;
        end_time = end_time + duration;

        last_candle = combined_candles[i].clone();
    }

    combined_candles
}

fn trim_candles(mut c: Vec<Candle>, start_time: DateTime<Utc>) -> Vec<Candle> {
    let mut i = 0;
    while i < c.len() {
        if c[i].end_time <= start_time {
            c.remove(i);
        } else {
            i += 1;
        }
    }
    c
}
