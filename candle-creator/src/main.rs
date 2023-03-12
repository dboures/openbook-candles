use crate::{
    candle_batching::batch_candles,
    database::{fetch::fetch_latest_finished_candle, insert::persist_candles, Candle, Resolution},
    trade_fetching::{parsing::OpenBookFillEventLog, scrape::fetch_market_infos},
    utils::Config,
};
use database::{
    fetch::fetch_earliest_fill,
    initialize::{connect_to_database, setup_database},
};
use dotenv;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;

mod candle_batching;
mod database;
mod trade_fetching;
mod utils;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let rpc_url: String = dotenv::var("RPC_URL").unwrap();
    let database_url: String = dotenv::var("DATABASE_URL").unwrap();

    let config = Config {
        rpc_url,
        database_url,
        max_pg_pool_connections: 5,
    };

    let markets = utils::load_markets("/Users/dboures/dev/openbook-candles/markets.json");
    let market_infos = fetch_market_infos(&config, markets).await?;
    println!("{:?}", market_infos);

    let pool = connect_to_database(&config).await?;
    // setup_database(&pool, market_infos).await?;

    // let (fill_sender, fill_receiver) = mpsc::channel::<OpenBookFillEventLog>(1000);

    // tokio::spawn(async move {
    //     trade_fetching::scrape::scrape(&config, fill_sender.clone()).await; TODO: send the vec, it's okay
    // });

    // database::database::handle_fill_events(&pool, fill_receiver).await;

    // trade_fetching::websocket::listen_logs().await?;

    let (candle_sender, candle_receiver) = mpsc::channel::<Vec<Candle>>(1000);

    let batch_pool = pool.clone();
    tokio::spawn(async move {
        batch_candles(batch_pool, &candle_sender, market_infos).await;
    });

    let persist_pool = pool.clone();
    // tokio::spawn(async move {
    persist_candles(persist_pool, candle_receiver).await;
    // });

    loop {}

    Ok(())
}

// use getconfirmedsignaturesforaddres2 to scan txns
// find filleventlog events
// parse trade data
// persist the last 3 months on differnet timescales
