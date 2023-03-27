pub mod higher_order_candles;
pub mod minute_candles;

use chrono::Duration;
use sqlx::{pool::PoolConnection, Pool, Postgres};
use strum::IntoEnumIterator;
use tokio::{sync::mpsc::Sender, time::sleep};

use crate::{
    structs::{candle::Candle, markets::MarketInfo, resolution::Resolution},
    worker::candle_batching::minute_candles::batch_1m_candles,
};

use self::higher_order_candles::batch_higher_order_candles;

pub async fn batch_for_market(
    pool: Pool<Postgres>,
    candles_sender: &Sender<Vec<Candle>>,
    market: &MarketInfo,
) -> anyhow::Result<()> {
    loop {
        let sender = candles_sender.clone();
        let market_clone = market.clone();
        let mut conn = pool.acquire().await?;
        loop {
            sleep(Duration::milliseconds(2000).to_std()?).await;
            match batch_inner(&mut conn, &sender, &market_clone).await {
                Ok(_) => {}
                Err(e) => {
                    println!(
                        "Batching thread failed for {:?} with error: {:?}",
                        market_clone.name.clone(),
                        e
                    );
                    break;
                }
            };
        }
        println!("Restarting {:?} batching thread", market.name);
    }
}

async fn batch_inner(
    conn: &mut PoolConnection<Postgres>,
    candles_sender: &Sender<Vec<Candle>>,
    market: &MarketInfo,
) -> anyhow::Result<()> {
    let market_name = &market.name.clone();
    let candles = batch_1m_candles(conn, market).await?;
    send_candles(candles, candles_sender).await;

    for resolution in Resolution::iter() {
        if resolution == Resolution::R1m {
            continue;
        }
        let candles = batch_higher_order_candles(conn, market_name, resolution).await?;
        send_candles(candles, candles_sender).await;
    }
    Ok(())
}

async fn send_candles(candles: Vec<Candle>, candles_sender: &Sender<Vec<Candle>>) {
    if candles.len() > 0 {
        if let Err(_) = candles_sender.send(candles).await {
            panic!("candles receiver dropped");
        }
    }
}
