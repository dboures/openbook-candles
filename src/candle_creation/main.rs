use dotenv;
use openbook_candles::candle_creation::candle_batching::batch_candles;
use openbook_candles::candle_creation::trade_fetching::{
    backfill::backfill,
    scrape::{fetch_market_infos, scrape},
};
use openbook_candles::database::{
    initialize::{connect_to_database, setup_database},
    insert::{persist_candles, persist_fill_events},
};
use openbook_candles::structs::candle::Candle;
use openbook_candles::structs::markets::load_markets;
use openbook_candles::structs::openbook::OpenBookFillEventLog;
use openbook_candles::utils::Config;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

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

    let markets = load_markets("/Users/dboures/dev/openbook-candles/markets.json");
    let market_infos = fetch_market_infos(&config, markets).await?;
    let mut target_markets = HashMap::new();
    for m in market_infos.clone() {
        target_markets.insert(Pubkey::from_str(&m.address)?, 0);
    }
    println!("{:?}", target_markets);

    let pool = connect_to_database(&config).await?;
    setup_database(&pool).await?;

    let (fill_sender, fill_receiver) = mpsc::channel::<OpenBookFillEventLog>(1000);

    // let bf_sender = fill_sender.clone();
    // let targets = target_markets.clone();
    // tokio::spawn(async move {
    //     backfill(&rpc_url.clone(), &bf_sender, &targets).await;
    // });

    tokio::spawn(async move {
        scrape(&config, &fill_sender, &target_markets).await; //TODO: send the vec, it's okay
    });

    let fills_pool = pool.clone();
    tokio::spawn(async move {
        persist_fill_events(&fills_pool, fill_receiver).await;
    });

    // let (candle_sender, candle_receiver) = mpsc::channel::<Vec<Candle>>(1000);

    // let batch_pool = pool.clone();
    // tokio::spawn(async move {
    //     batch_candles(batch_pool, &candle_sender, market_infos).await;
    // });

    // let persist_pool = pool.clone();
    // // tokio::spawn(async move {
    // persist_candles(persist_pool, candle_receiver).await;
    // // });

    loop {} // tokio drop if one thread drops or something

    Ok(())
}

// use getconfirmedsignaturesforaddres2 to scan txns
// find filleventlog events
// parse trade data
// persist the last 3 months on differnet timescales
