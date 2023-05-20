use anchor_lang::prelude::Pubkey;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use deadpool_postgres::Object;
use futures::future::join_all;
use openbook_candles::{
    database::{
        initialize::connect_to_database,
        insert::{build_candles_upsert_statement, persist_candles},
    },
    structs::{
        candle::Candle,
        markets::{fetch_market_infos, load_markets},
        openbook::OpenBookFillEvent,
        resolution::Resolution,
    },
    utils::{AnyhowWrap, Config},
    worker::candle_batching::{
        higher_order_candles::backfill_batch_higher_order_candles,
        minute_candles::backfill_batch_1m_candles,
    },
};
use std::{collections::HashMap, env, str::FromStr};
use strum::IntoEnumIterator;
use tokio::sync::mpsc::{self, Sender};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 2);

    let path_to_markets_json = &args[1];
    let rpc_url: String = dotenv::var("RPC_URL").unwrap();

    let config = Config {
        rpc_url: rpc_url.clone(),
    };
    let markets = load_markets(&path_to_markets_json);
    let market_infos = fetch_market_infos(&config, markets.clone()).await?;
    println!("Backfilling candles for {:?}", markets);

    let pool = connect_to_database().await?;
    for market in market_infos.into_iter() {
        let client = pool.get().await?;
        let minute_candles = backfill_batch_1m_candles(&pool, &market).await?;
        save_candles(minute_candles, client).await?;

        for resolution in Resolution::iter() {
            if resolution == Resolution::R1m {
                continue;
            }
            let higher_order_candles =
                backfill_batch_higher_order_candles(&pool, &market.name, resolution).await?;
            let client = pool.get().await?;
            save_candles(higher_order_candles, client).await?;
        }
    }
    Ok(())
}

async fn save_candles(candles: Vec<Candle>, client: Object) -> anyhow::Result<()> {
    if candles.len() > 0 {
        let upsert_statement = build_candles_upsert_statement(candles);
        client
            .execute(&upsert_statement, &[])
            .await
            .map_err_anyhow()?;
    }
    Ok(())
}
