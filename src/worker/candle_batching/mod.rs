pub mod higher_order_candles;
pub mod minute_candles;

use chrono::Duration;
use deadpool_postgres::Pool;
use log::{error, warn};
use strum::IntoEnumIterator;
use tokio::time::sleep;

use crate::{
    database::insert::build_candles_upsert_statement,
    structs::{candle::Candle, markets::MarketInfo, resolution::Resolution},
    utils::AnyhowWrap,
    worker::candle_batching::minute_candles::batch_1m_candles,
};

use self::higher_order_candles::batch_higher_order_candles;

pub async fn batch_for_market(pool: &Pool, market: &MarketInfo) -> anyhow::Result<()> {
    loop {
        let market_clone = market.clone();

        loop {
            sleep(Duration::milliseconds(5000).to_std()?).await;
            match batch_inner(pool, &market_clone).await {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Batching thread failed for {:?} with error: {:?}",
                        market_clone.name.clone(),
                        e
                    );
                    break;
                }
            };
        }
        warn!("Restarting {:?} batching thread", market.name);
    }
}

async fn batch_inner(pool: &Pool, market: &MarketInfo) -> anyhow::Result<()> {
    let market_name = &market.name.clone();
    let candles = batch_1m_candles(pool, market).await?;
    save_candles(pool, candles).await?;
    for resolution in Resolution::iter() {
        if resolution == Resolution::R1m {
            continue;
        }
        let candles = batch_higher_order_candles(pool, market_name, resolution).await?;
        save_candles(pool, candles).await?;
    }
    Ok(())
}

async fn save_candles(pool: &Pool, candles: Vec<Candle>) -> anyhow::Result<()> {
    if candles.is_empty() {
        return Ok(());
    }
    let upsert_statement = build_candles_upsert_statement(&candles);
    let client = pool.get().await.unwrap();
    client
        .execute(&upsert_statement, &[])
        .await
        .map_err_anyhow()?;
    Ok(())
}
