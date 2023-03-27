use dotenv;
use openbook_candles::database::{
    initialize::{connect_to_database, setup_database},
    insert::{persist_candles, persist_fill_events},
};
use openbook_candles::structs::candle::Candle;
use openbook_candles::structs::markets::{fetch_market_infos, load_markets};
use openbook_candles::structs::openbook::OpenBookFillEventLog;
use openbook_candles::utils::Config;
use openbook_candles::worker::candle_batching::batch_for_market;
use openbook_candles::worker::trade_fetching::scrape::scrape;
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 2);
    let path_to_markets_json = &args[1];
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
    let market_infos = fetch_market_infos(&config, markets.clone()).await?;
    let mut target_markets = HashMap::new();
    for m in market_infos.clone() {
        target_markets.insert(Pubkey::from_str(&m.address)?, 0);
    }
    println!("{:?}", target_markets);

    let pool = connect_to_database(&config).await?;
    setup_database(&pool).await?;
    let mut handles = vec![];

    let (fill_sender, mut fill_receiver) = mpsc::channel::<OpenBookFillEventLog>(1000);

    handles.push(tokio::spawn(async move {
        scrape(&config, &fill_sender, &target_markets).await; //TODO: send the vec, it's okay
    }));

    let fills_pool = pool.clone();
    handles.push(tokio::spawn(async move {
        loop {
            persist_fill_events(&fills_pool, &mut fill_receiver).await.unwrap();
        }
    }));

    let (candle_sender, mut candle_receiver) = mpsc::channel::<Vec<Candle>>(1000);

    for market in market_infos.into_iter() {
        let sender = candle_sender.clone();
        let batch_pool = pool.clone();
        handles.push(tokio::spawn(async move {
            batch_for_market(batch_pool, &sender, &market).await.unwrap();
            println!("SOMETHING WENT WRONG");
        }));
    }

    let persist_pool = pool.clone();
    handles.push(tokio::spawn(async move {
        loop {
            persist_candles(persist_pool.clone(), &mut candle_receiver).await.unwrap();
        }
    }));

    futures::future::join_all(handles).await;

    Ok(())
}
