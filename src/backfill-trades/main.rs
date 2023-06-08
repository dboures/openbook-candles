use anchor_lang::prelude::Pubkey;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use deadpool_postgres::Pool;
use futures::future::join_all;
use log::debug;
use openbook_candles::{
    database::{
        fetch::fetch_worker_transactions,
        initialize::{connect_to_database, setup_database},
        insert::{add_fills_atomically, build_transactions_insert_statement},
    },
    structs::{
        markets::{fetch_market_infos, load_markets},
        transaction::{PgTransaction, NUM_TRANSACTION_PARTITIONS},
    },
    utils::{AnyhowWrap, Config, OPENBOOK_KEY},
    worker::trade_fetching::parsing::parse_trades_from_openbook_txns,
};
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_config::RpcTransactionConfig
};
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use solana_transaction_status::UiTransactionEncoding;
use std::{collections::HashMap, env, str::FromStr,time::Duration as WaitDuration };

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 2);

    let path_to_markets_json = &args[1];
    // let num_days = args[2].parse::<i64>().unwrap(); // TODO: implement
    let num_days = 1;
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
    println!("{:?}", target_markets);

    let pool = connect_to_database().await?;
    setup_database(&pool).await?;

    let mut handles = vec![];

    let rpc_clone = rpc_url.clone();
    let pool_clone = pool.clone();
    handles.push(tokio::spawn(async move {
        fetch_signatures(rpc_clone, &pool_clone, num_days).await.unwrap();
    }));

    for id in 0..NUM_TRANSACTION_PARTITIONS {
        let rpc_clone = rpc_url.clone();
        let pool_clone = pool.clone();
        let markets_clone = target_markets.clone();
        handles.push(tokio::spawn(async move {
            backfill(id as i32, rpc_clone, &pool_clone, &markets_clone)
                .await
                .unwrap();
        }));
    }

    // TODO: spawn status thread

    futures::future::join_all(handles).await;
    Ok(())
}

pub async fn fetch_signatures(rpc_url: String, pool: &Pool, num_days: i64) -> anyhow::Result<()> {
    let mut before_sig: Option<Signature> = None;
    let mut now_time = Utc::now().timestamp();
    let end_time = (Utc::now() - Duration::days(num_days)).timestamp();
    let rpc_client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());

    while now_time > end_time {
        let rpc_config = GetConfirmedSignaturesForAddress2Config {
            before: before_sig,
            until: None,
            limit: None,
            commitment: Some(CommitmentConfig::confirmed()),
        };

        let sigs = match rpc_client
            .get_signatures_for_address_with_config(
                &OPENBOOK_KEY,
                rpc_config,
            )
            .await
        {
            Ok(sigs) => sigs,
            Err(e) => {
                println!("Error fetching signatures: {}", e);
                continue;
            }
        };
        if sigs.is_empty() {
            println!("No signatures found, trying again");
            continue;
        }
        let last = sigs.last().unwrap();
        let last_time = last.block_time.unwrap().clone();
        let last_signature = last.signature.clone();
        let transactions = sigs
            .into_iter()
            .map(|s| PgTransaction::from_rpc_confirmed_transaction(s))
            .collect::<Vec<PgTransaction>>();

        if transactions.is_empty() {
            println!("No transactions found, trying again");   
        }
        debug!("writing: {:?} txns to DB\n", transactions.len());
        let upsert_statement = build_transactions_insert_statement(transactions);
            let client = pool.get().await?;
            client
                .execute(&upsert_statement, &[])
                .await
                .map_err_anyhow()?;

        now_time = last_time;
        before_sig = Some(Signature::from_str(&last_signature)?);
        let time_left = backfill_time_left(now_time, end_time);
        println!(
            "{} minutes ~ {} days remaining in the backfill\n",
            time_left.num_minutes(),
            time_left.num_days()
        );
    }
    Ok(())
}

pub async fn backfill(
    worker_id: i32,
    rpc_url: String,
    pool: &Pool,
    target_markets: &HashMap<Pubkey, String>,
) -> anyhow::Result<()> {
    println!("Worker {} up \n", worker_id);
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    loop {
        let transactions = fetch_worker_transactions(worker_id, pool).await?;
        if transactions.len() == 0 {
            println!("No signatures found by worker {}", worker_id);
            tokio::time::sleep(WaitDuration::from_secs(1)).await;
            continue;
        };

        // for each signature, fetch the transaction
        let txn_config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        let sig_strings = transactions
            .iter()
            .map(|t| t.signature.clone())
            .collect::<Vec<String>>();

        let signatures: Vec<_> = transactions
            .into_iter()
            .map(|t| t.signature.parse::<Signature>().unwrap())
            .collect();

        let txn_futs: Vec<_> = signatures
            .iter()
            .map(|s| rpc_client.get_transaction_with_config(s, txn_config))
            .collect();

        let mut txns = join_all(txn_futs).await;

        let fills = parse_trades_from_openbook_txns(&mut txns, target_markets);

        // Write any fills to the database, and mark the transactions as processed
        add_fills_atomically(pool, worker_id, fills, sig_strings).await?;
    }

    // TODO: graceful shutdown
    // println!("Worker {} down \n", worker_id);
    Ok(())
}

fn backfill_time_left(current_time: i64, backfill_end: i64) -> Duration {
    let naive_cur = NaiveDateTime::from_timestamp_millis(current_time * 1000).unwrap();
    let naive_bf = NaiveDateTime::from_timestamp_millis(backfill_end * 1000).unwrap();
    let cur_date = DateTime::<Utc>::from_utc(naive_cur, Utc);
    let bf_date = DateTime::<Utc>::from_utc(naive_bf, Utc);
    cur_date - bf_date
}
