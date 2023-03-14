pub mod higher_order_candles;
pub mod minute_candles;

use chrono::Duration;
use sqlx::{Pool, Postgres};
use strum::IntoEnumIterator;
use tokio::{sync::mpsc::Sender, time::sleep};

use crate::{
    candle_creation::candle_batching::minute_candles::batch_1m_candles,
    structs::{candle::Candle, markets::MarketInfo, resolution::Resolution},
};

use self::higher_order_candles::batch_higher_order_candles;

pub async fn batch_candles(
    pool: Pool<Postgres>,
    candles_sender: &Sender<Vec<Candle>>,
    markets: Vec<MarketInfo>,
) {
    let mut handles = vec![];
    for market in markets.into_iter() {
        let sender = candles_sender.clone();
        let pool_clone = pool.clone();
        let market_clone = market.clone();
        handles.push(tokio::spawn(async move {
            loop {
                batch_for_market(&pool_clone, &sender, &market_clone)
                    .await
                    .unwrap();

                sleep(Duration::milliseconds(2000).to_std().unwrap()).await;
            }
        }));
    }

    futures::future::join_all(handles).await;
}

async fn batch_for_market(
    pool: &Pool<Postgres>,
    candles_sender: &Sender<Vec<Candle>>,
    market: &MarketInfo,
) -> anyhow::Result<()> {
    let market_name = &market.name.clone();
    let candles = batch_1m_candles(pool, market).await?;
    send_candles(candles, candles_sender).await;

    for resolution in Resolution::iter() {
        if resolution == Resolution::R1m {
            continue;
        }
        let candles = batch_higher_order_candles(pool, market_name, resolution).await?;
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
