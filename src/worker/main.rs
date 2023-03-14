use dotenv;
use openbook_candles::worker::candle_batching::batch_candles;
use openbook_candles::worker::trade_fetching::scrape::scrape;
use openbook_candles::database::{
    initialize::{connect_to_database, setup_database},
    insert::{persist_candles, persist_fill_events},
};
use openbook_candles::structs::candle::Candle;
use openbook_candles::structs::markets::{fetch_market_infos, load_markets};
use openbook_candles::structs::openbook::OpenBookFillEventLog;
use openbook_candles::utils::Config;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let path_to_markets_json: String = dotenv::var("PATH_TO_MARKETS_JSON").unwrap();
    let rpc_url: String = dotenv::var("RPC_URL").unwrap();
    let database_url: String = dotenv::var("DATABASE_URL").unwrap();
    let max_pg_pool_connections: u32 = dotenv::var("MAX_PG_POOL_CONNS_WORKER")
        .unwrap()
        .parse::<u32>()
        .unwrap();

    let config = Config {
        rpc_url: rpc_url.clone(),
        database_url,
        max_pg_pool_connections,
    };

    let markets = load_markets(&path_to_markets_json);
    let market_infos = fetch_market_infos(&config, markets).await?;
    let mut target_markets = HashMap::new();
    for m in market_infos.clone() {
        target_markets.insert(Pubkey::from_str(&m.address)?, 0);
    }
    println!("{:?}", target_markets);

    let pool = connect_to_database(&config).await?;
    setup_database(&pool).await?;
    let mut handles = vec![];

    let (fill_sender, fill_receiver) = mpsc::channel::<OpenBookFillEventLog>(1000);

    handles.push(tokio::spawn(async move {
        scrape(&config, &fill_sender, &target_markets).await; //TODO: send the vec, it's okay
    }));

    let fills_pool = pool.clone();
    handles.push(tokio::spawn(async move {
        persist_fill_events(&fills_pool, fill_receiver).await;
    }));

    let (candle_sender, candle_receiver) = mpsc::channel::<Vec<Candle>>(1000);

    let batch_pool = pool.clone();
    handles.push(tokio::spawn(async move {
        batch_candles(batch_pool, &candle_sender, market_infos).await;
    }));

    let persist_pool = pool.clone();
    handles.push(tokio::spawn(async move {
        persist_candles(persist_pool, candle_receiver).await;
    }));

    futures::future::join_all(handles).await;

    Ok(())
}
