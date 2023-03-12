use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use solana_client::{rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::UiTransactionEncoding;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

use crate::trade_fetching::scrape::scrape_transactions;

use super::parsing::OpenBookFillEventLog;

pub async fn backfill(
    rpc_url: &String,
    fill_sender: &Sender<OpenBookFillEventLog>,
    target_markets: &HashMap<Pubkey, u8>,
) {
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::processed());

    println!("backfill started");
    let mut before_slot: Option<Signature> = None;
    let end_time = (Utc::now() - Duration::days(1)).timestamp();
    loop {
        let last_sig_option =
            scrape_transactions(&rpc_client, before_slot, None, fill_sender, target_markets).await;

        if last_sig_option.is_none() {
            println!("last sig is none");
            continue;
        }

        let txn_config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        match rpc_client.get_transaction_with_config(&last_sig_option.unwrap(), txn_config) {
            Ok(txn) => {
                let unix_sig_time = rpc_client.get_block_time(txn.slot).unwrap();
                if unix_sig_time < end_time {
                    break;
                }
                let time_left = backfill_time_left(unix_sig_time, end_time);
                println!(
                    "{} minutes ~ {} days remaining in the backfill\n",
                    time_left.num_minutes(),
                    time_left.num_days()
                );
            }
            Err(e) => {
                println!("error: {:?}", e);
                continue;
            }
        }
        before_slot = last_sig_option;
    }

    print!("Backfill complete \n");
}

fn backfill_time_left(current_time: i64, backfill_end: i64) -> Duration {
    let naive_cur = NaiveDateTime::from_timestamp_millis(current_time * 1000).unwrap();
    let naive_bf = NaiveDateTime::from_timestamp_millis(backfill_end * 1000).unwrap();
    let cur_date = DateTime::<Utc>::from_utc(naive_cur, Utc);
    let bf_date = DateTime::<Utc>::from_utc(naive_bf, Utc);
    cur_date - bf_date
}
