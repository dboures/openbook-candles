use std::cmp::{max, min};

use chrono::{DateTime, Duration, DurationRound, SubsecRound, Utc};
use num_traits::{FromPrimitive, Zero};
use sqlx::{types::Decimal, Pool, Postgres};

use crate::database::{
    fetchers::{fetch_earliest_fill, fetch_fills_from, fetch_latest_finished_candle},
    Candle, MarketInfo, PgOpenBookFill, Resolution,
};

pub async fn batch_candles(pool: &Pool<Postgres>, markets: Vec<MarketInfo>) {
    let m = MarketInfo {
        name: "BTC/USDC".to_owned(),
        address: "A8YFbxQYFVqKZaoYJLLUVcQiWP7G2MeEgW5wsAQgMvFw".to_owned(),
        base_decimals: 6,
        quote_decimals: 6,
        base_mint_key: "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU".to_owned(),
        quote_mint_key: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_owned(),
        base_lot_size: 10_000,
        quote_lot_size: 1,
    };
    batch_for_market(&pool.clone(), m).await.unwrap();

    // for market in markets.iter() {
    //     tokio::spawn(async move {
    //         loop {
    //             batch_for_market(&pool.clone(), &market.address.clone()).await;
    //         }
    //         });
    // }
}

async fn batch_for_market(pool: &Pool<Postgres>, market: MarketInfo) -> anyhow::Result<()> {
    let candles = batch_1m_candles(pool, market).await?;
    // println!("candles {:?}", candles[10]);
    // database::insert_candles(pool, candles)

    // for resolution in Resolution.iter

    Ok(())
}

async fn batch_1m_candles(
    pool: &Pool<Postgres>,
    market: MarketInfo,
) -> anyhow::Result<Vec<Candle>> {
    let market_address_string = &market.address;
    let latest_candle =
        fetch_latest_finished_candle(pool, market_address_string, Resolution::R1m).await?;

    match latest_candle {
        Some(candle) => {
            let start_time = candle.end_time;
            let end_time = min(
                start_time + Duration::hours(6),
                Utc::now().duration_trunc(Duration::minutes(1))?,
            );
            let mut fills =
                fetch_fills_from(pool, market_address_string, start_time, end_time).await?;
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
            let earliest_fill = fetch_earliest_fill(pool, market_address_string).await?;

            if earliest_fill.is_none() {
                println!("No fills found for: {:?}", market_address_string);
                return Ok(Vec::new());
            }

            let start_time = earliest_fill
                .unwrap()
                .time
                .duration_trunc(Duration::minutes(1))?;
            let end_time = min(
                start_time + Duration::hours(6),
                Utc::now().duration_trunc(Duration::minutes(1))?,
            );
            let mut fills =
                fetch_fills_from(pool, market_address_string, start_time, end_time).await?;
            let candles =
                combine_fills_into_1m_candles(&mut fills, market, start_time, end_time, None);
            Ok(candles)
        }
    }
}

fn combine_fills_into_1m_candles(
    fills: &mut Vec<PgOpenBookFill>,
    market: MarketInfo,
    st: DateTime<Utc>,
    et: DateTime<Utc>,
    maybe_last_price: Option<Decimal>,
) -> Vec<Candle> {
    let empty_candle = Candle::create_empty_candle(market.address, Resolution::R1m);

    let minutes = (et - st).num_minutes();
    let mut candles = vec![empty_candle; minutes as usize];

    let mut fills_iter = fills.iter_mut().peekable();
    let mut start_time = st.clone();
    let mut end_time = start_time + Duration::minutes(1);

    let mut last_price = maybe_last_price.unwrap_or(Decimal::zero());

    for i in 0..candles.len() {
        candles[i].open = last_price;
        candles[i].close = last_price;
        candles[i].low = last_price;
        candles[i].high = last_price;

        while matches!(fills_iter.peek(), Some(f) if f.time < end_time) {
            let fill = fills_iter.next().unwrap();

            let (price, volume) =
                calculate_fill_price_and_size(*fill, market.base_decimals, market.quote_decimals);

            println!("{:?}", price);

            candles[i].close = price;
            candles[i].low = min(price, candles[i].low);
            candles[i].high = max(price, candles[i].high);
            candles[i].volume += volume;

            last_price = price;
        }

        candles[i].start_time = start_time;
        candles[i].end_time = end_time;
        candles[i].complete = matches!(fills_iter.peek(), Some(f) if f.time > end_time);

        println!("{:?}", candles[i]);

        start_time = end_time;
        end_time = end_time + Duration::minutes(1);
    }

    candles
}

fn calculate_fill_price_and_size(
    fill: PgOpenBookFill,
    base_decimals: u8,
    quote_decimals: u8,
) -> (Decimal, Decimal) {
    if fill.bid {
        let price_before_fees = if fill.maker {
            fill.native_qty_paid + fill.native_fee_or_rebate
        } else {
            fill.native_qty_paid - fill.native_fee_or_rebate
        };
        let price = (price_before_fees * token_factor(base_decimals))
            / (token_factor(quote_decimals) * fill.native_qty_received);
        let size = fill.native_qty_received / token_factor(base_decimals);
        (price, size)
    } else {
        let price_before_fees = if fill.maker {
            fill.native_qty_received - fill.native_fee_or_rebate
        } else {
            fill.native_qty_received + fill.native_fee_or_rebate
        };
        let price = (price_before_fees * token_factor(base_decimals))
            / (token_factor(quote_decimals) * fill.native_qty_paid);
        let size = fill.native_qty_paid / token_factor(base_decimals);
        (price, size)
    }
}

fn token_factor(decimals: u8) -> Decimal {
    Decimal::from_u64(10u64.pow(decimals as u32)).unwrap()
}
