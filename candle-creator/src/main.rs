use crate::{trade_fetching::parsing::OpenBookFillEventLog, utils::Config};
use database::database::{connect_to_database, setup_database};
use dotenv;
use tokio::sync::mpsc;

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
        markets: utils::load_markets("/Users/dboures/dev/openbook-candles/markets.json"),
    };

    println!("{:?}", config);

    let pool = connect_to_database(&config).await?;
    setup_database(&pool).await?;

    let (fill_sender, fill_receiver) = mpsc::channel::<OpenBookFillEventLog>(1000);

    // spawn a thread for each market?
    // what are the memory implications?

    // tokio::spawn(async move {
    //     trade_fetching::scrape::scrape(&config, fill_event_sender.clone()).await;
    // });

    tokio::spawn(async move {
        trade_fetching::scrape::scrape(&config, fill_sender.clone()).await;
    });

    database::database::handle_fill_events(&pool, fill_receiver).await;

    // trade_fetching::websocket::listen_logs().await?;
    Ok(())
}

// use getconfirmedsignaturesforaddres2 to scan txns
// find filleventlog events
// parse trade data
// persist the last 3 months on differnet timescales
