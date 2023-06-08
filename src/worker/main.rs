use log::{error, info};
use openbook_candles::structs::markets::{fetch_market_infos, load_markets};
use openbook_candles::structs::transaction::NUM_TRANSACTION_PARTITIONS;
use openbook_candles::utils::Config;
use openbook_candles::worker::metrics::{
    serve_metrics, METRIC_DB_POOL_AVAILABLE, METRIC_DB_POOL_SIZE,
};
use openbook_candles::worker::trade_fetching::scrape::{scrape_fills, scrape_signatures};
use openbook_candles::{
    database::initialize::{connect_to_database, setup_database},
    worker::candle_batching::batch_for_market,
};
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::{collections::HashMap, str::FromStr, time::Duration as WaitDuration};

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

    // signature scraping
    let rpc_clone = rpc_url.clone();
    let pool_clone = pool.clone();
    handles.push(tokio::spawn(async move {
        scrape_signatures(rpc_clone, &pool_clone).await.unwrap();
    }));

    // transaction/fill scraping
    for id in 0..NUM_TRANSACTION_PARTITIONS {
        let rpc_clone = rpc_url.clone();
        let pool_clone = pool.clone();
        let markets_clone = target_markets.clone();
        handles.push(tokio::spawn(async move {
            scrape_fills(id as i32, rpc_clone, &pool_clone, &markets_clone)
                .await
                .unwrap();
        }));
    }

    // candle batching
    for market in market_infos.into_iter() {
        let batch_pool = pool.clone();
        handles.push(tokio::spawn(async move {
            batch_for_market(&batch_pool, &market).await.unwrap();
            error!("batching halted for market {}", &market.name);
        }));
    }

    let monitor_pool = pool.clone();
    handles.push(tokio::spawn(async move {
        // TODO: maybe break this out into a new function
        loop {
            let pool_status = monitor_pool.status();
            METRIC_DB_POOL_AVAILABLE.set(pool_status.available as i64);
            METRIC_DB_POOL_SIZE.set(pool_status.size as i64);

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
