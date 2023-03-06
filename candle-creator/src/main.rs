use crate::{trade_fetching::parsing::FillEventLog, utils::Config};
use database::database::{connect_to_database, setup_database};
use std::{fs::File, io::Read};
use tokio::sync::mpsc;

mod database;
mod trade_fetching;
mod utils;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config: Config = {
        let mut file = File::open("./example-config.toml")?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    println!("{:?}", config);

    let pool = connect_to_database(&config).await?;
    setup_database(&pool).await?;

    let (fill_event_sender, mut fill_event_receiver) = mpsc::channel::<FillEventLog>(1000);

    // spawn a thread for each market?
    // what are the memory implications?

    tokio::spawn(async move {
        trade_fetching::scrape::scrape(&config, fill_event_sender).await;
    });

    database::database::handle_fill_events(&pool, fill_event_receiver).await;

    // trade_fetching::websocket::listen_logs().await?;
    Ok(())
}

// use getconfirmedsignaturesforaddres2 to scan txns
// find filleventlog events
// parse trade data
// persist the last 3 months on differnet timescales
