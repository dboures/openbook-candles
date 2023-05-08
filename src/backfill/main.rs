use std::{collections::HashMap, str::FromStr, env};
use anchor_lang::prelude::Pubkey;
use chrono::{NaiveDateTime, DateTime, Utc, Duration};
use futures::future::join_all;
use openbook_candles::{structs::{openbook::OpenBookFillEventLog, markets::{load_markets, fetch_market_infos}}, worker::trade_fetching::{scrape::scrape_transactions, parsing::parse_trades_from_openbook_txns}, database::{initialize::connect_to_database, insert::persist_fill_events}, utils::Config};
use solana_client::{rpc_config::RpcTransactionConfig, nonblocking::rpc_client::RpcClient, rpc_client::GetConfirmedSignaturesForAddress2Config, rpc_response::RpcConfirmedTransactionStatusWithSignature};
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use solana_transaction_status::UiTransactionEncoding;
use tokio::sync::mpsc::{Sender, self};

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
    let (fill_sender, mut fill_receiver) = mpsc::channel::<OpenBookFillEventLog>(1000);

   tokio::spawn(async move {
        loop {
            persist_fill_events(&pool, &mut fill_receiver)
                .await
                .unwrap();
        }
    });

    backfill(rpc_url, &fill_sender, &target_markets).await?;
    Ok(())
}

pub async fn backfill(
    rpc_url: String,
    fill_sender: &Sender<OpenBookFillEventLog>,
    target_markets: &HashMap<Pubkey, u8>,
) -> anyhow::Result<()> {

    println!("backfill started");
    let mut before_sig: Option<Signature> = None;
    let mut now_time = Utc::now().timestamp();
    let end_time = (Utc::now() - Duration::days(1)).timestamp();

    let mut handles = vec![];

    while now_time > end_time {
        let rpc_client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
        let maybe_r = get_signatures(&rpc_client, before_sig, None).await;

        match maybe_r {
            Some((last, time, sigs)) => {
                now_time = time;
                before_sig = Some(last);

                let time_left = backfill_time_left(now_time, end_time);
                println!(
                    "{} minutes ~ {} days remaining in the backfill\n",
                    time_left.num_minutes(),
                    time_left.num_days()
                );

                let cloned_markets = target_markets.clone();
                let cloned_sender = fill_sender.clone();
                let handle = tokio::spawn(async move {
                    get_transactions(&rpc_client, sigs, &cloned_sender, &cloned_markets).await;
                });
                handles.push(handle);
            },
            None => {},
        }
    };

    futures::future::join_all(handles).await;

    println!("Backfill complete \n");
    Ok(())
}


pub async fn get_signatures(rpc_client: &RpcClient,
    before_sig: Option<Signature>,
    limit: Option<usize>) -> Option<(Signature, i64, Vec<RpcConfirmedTransactionStatusWithSignature>)> {
        let rpc_config = GetConfirmedSignaturesForAddress2Config {
            before: before_sig,
            until: None,
            limit,
            commitment: Some(CommitmentConfig::confirmed()),
        };
    
        let sigs = match rpc_client
            .get_signatures_for_address_with_config(
                &Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").unwrap(),
                rpc_config,
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                println!("Error in get_signatures_for_address_with_config: {}", e);
                return None;
            }
        };
    
        if sigs.len() == 0 {
            println!("No signatures found");
            return None;
        }
        let last = sigs.last().unwrap();
        return Some((Signature::from_str(&last.signature).unwrap(), last.block_time.unwrap(), sigs));
    }



pub async fn get_transactions(
    rpc_client: &RpcClient,
    mut sigs: Vec<RpcConfirmedTransactionStatusWithSignature>,
    fill_sender: &Sender<OpenBookFillEventLog>,
    target_markets: &HashMap<Pubkey, u8>,
) {
    sigs.retain(|sig| sig.err.is_none());
    if sigs.last().is_none() {
        return;
    }

    let txn_config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Json),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    let signatures: Vec<_> = sigs
        .into_iter()
        .map(|sig| sig.signature.parse::<Signature>().unwrap())
        .collect();

    let txn_futs: Vec<_> = signatures
        .iter()
        .map(|s| rpc_client.get_transaction_with_config(&s, txn_config))
        .collect();

    let mut txns = join_all(txn_futs).await;

    let fills = parse_trades_from_openbook_txns(&mut txns, target_markets);
    if fills.len() > 0 {
        for fill in fills.into_iter() {
            if let Err(_) = fill_sender.send(fill).await {
                panic!("receiver dropped");
            }
        }
    }
}

fn backfill_time_left(current_time: i64, backfill_end: i64) -> Duration {
    let naive_cur = NaiveDateTime::from_timestamp_millis(current_time * 1000).unwrap();
    let naive_bf = NaiveDateTime::from_timestamp_millis(backfill_end * 1000).unwrap();
    let cur_date = DateTime::<Utc>::from_utc(naive_cur, Utc);
    let bf_date = DateTime::<Utc>::from_utc(naive_bf, Utc);
    cur_date - bf_date
}