use log::{error, info};
use openbook_candles::structs::markets::{fetch_market_infos, load_markets};
use openbook_candles::structs::openbook::OpenBookFillEvent;
use openbook_candles::utils::Config;
use openbook_candles::worker::metrics::{
    serve_metrics, METRIC_DB_POOL_AVAILABLE, METRIC_DB_POOL_SIZE, METRIC_FILLS_QUEUE_LENGTH,
};
use openbook_candles::worker::trade_fetching::scrape::scrape;
use openbook_candles::{
    database::{
        initialize::{connect_to_database, setup_database},
        insert::{persist_fill_events},
    },
    worker::candle_batching::batch_for_market,
};
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::{collections::HashMap, str::FromStr, time::Duration as WaitDuration};
use tokio::sync::mpsc;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv::dotenv().ok();

    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 2);
    let path_to_markets_json = &args[1];
    let rpc_url: String = dotenv::var("RPC_URL").unwrap();

    let config = Config {
        rpc_url: rpc_url.clone(),
    };

    let fills_queue_max_size = 10000;

    let markets = load_markets(path_to_markets_json);
    let market_infos = fetch_market_infos(&config, markets.clone()).await?;
    let mut target_markets = HashMap::new();
    for m in market_infos.clone() {
        target_markets.insert(Pubkey::from_str(&m.address)?, m.name);
    }
    info!("{:?}", target_markets);

    let pool = connect_to_database().await?;
    setup_database(&pool).await?;
    let mut handles = vec![];

    let (fill_sender, mut fill_receiver) = mpsc::channel::<OpenBookFillEvent>(fills_queue_max_size);
    let scrape_fill_sender = fill_sender.clone();
    handles.push(tokio::spawn(async move {
        scrape(&config, &scrape_fill_sender, &target_markets).await;
    }));

    let fills_pool = pool.clone();
    handles.push(tokio::spawn(async move {
        loop {
            persist_fill_events(&fills_pool, &mut fill_receiver)
                .await
                .unwrap();
        }
    }));

    for market in market_infos.into_iter() {
        let batch_pool = pool.clone();
        handles.push(tokio::spawn(async move {
            batch_for_market(&batch_pool, &market).await.unwrap();
            error!("batching halted for market {}", &market.name);
        }));
    }

    let monitor_pool = pool.clone();
    let monitor_fill_channel = fill_sender.clone();
    handles.push(tokio::spawn(async move {
        // TODO: maybe break this out into a new function
        loop {
            let pool_status = monitor_pool.status();
            METRIC_DB_POOL_AVAILABLE.set(pool_status.available as i64);
            METRIC_DB_POOL_SIZE.set(pool_status.size as i64);

            METRIC_FILLS_QUEUE_LENGTH
                .set((fills_queue_max_size - monitor_fill_channel.capacity()) as i64);

            tokio::time::sleep(WaitDuration::from_secs(10)).await;
        }
    }));

    handles.push(tokio::spawn(async move {
        // TODO: this is ugly af
        serve_metrics().await.unwrap().await.unwrap();
    }));

    futures::future::join_all(handles).await;

    Ok(())
}
