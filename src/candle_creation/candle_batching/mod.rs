pub mod higher_order_candles;
pub mod minute_candles;

use chrono::Duration;
use sqlx::{Pool, Postgres};
use strum::IntoEnumIterator;
use tokio::{sync::mpsc::Sender, time::sleep};

use crate::{
    candle_creation::candle_batching::minute_candles::batch_1m_candles,
    database::{Candle, MarketInfo, Resolution},
};

use self::higher_order_candles::batch_higher_order_candles;

pub fn day() -> Duration {
    Duration::days(1)
}

pub async fn batch_candles(
    pool: Pool<Postgres>,
    candles_sender: &Sender<Vec<Candle>>,
    markets: Vec<MarketInfo>,
) {
    // TODO: tokio spawn a taks for every market

    loop {
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

        batch_for_market(&pool.clone(), candles_sender, m)
            .await
            .unwrap();

        sleep(Duration::milliseconds(500).to_std().unwrap()).await;
    }

    //loop
}

async fn batch_for_market(
    pool: &Pool<Postgres>,
    candles_sender: &Sender<Vec<Candle>>,
    market: MarketInfo,
) -> anyhow::Result<()> {
    let market_address = &market.address.clone();
    let candles = batch_1m_candles(pool, market).await?;
    send_candles(candles, candles_sender).await;

    for resolution in Resolution::iter() {
        if resolution == Resolution::R1m {
            continue;
        }
        let candles = batch_higher_order_candles(pool, market_address, resolution).await?;
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
