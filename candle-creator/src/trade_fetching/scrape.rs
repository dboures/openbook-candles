use solana_client::{
    client_error::Result as ClientResult,
    rpc_client::{GetConfirmedSignaturesForAddress2Config, RpcClient},
    rpc_config::RpcTransactionConfig,
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::{str::FromStr, time::Duration};
use tokio::sync::mpsc::Sender;

use crate::utils::Config;

use super::parsing::{parse_trades_from_openbook_txns, OpenBookFillEventLog};

// use serde::{Deserialize, Serialize};

pub async fn scrape(config: &Config, fill_sender: Sender<OpenBookFillEventLog>) {
    let url = &config.rpc_url;
    let rpc_client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());

    fetch_market(&rpc_client).await;

    let before_slot = None;
    loop {
        scrape_transactions(&rpc_client, before_slot, &fill_sender).await;

        print!("Ding fires are done \n\n");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub async fn backfill(config: &Config, fill_sender: Sender<OpenBookFillEventLog>) {
    let url = &config.rpc_url;
    let rpc_client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());

    let mut before_slot: Option<Signature> = None;

    loop {
        let last_sig = scrape_transactions(&rpc_client, before_slot, &fill_sender).await;

        match rpc_client.get_transaction(&last_sig, UiTransactionEncoding::Json) {
            Ok(txn) => {
                let unix_sig_time = rpc_client.get_block_time(txn.slot).unwrap();
                if unix_sig_time > 0 {
                    // TODO: is 3 months in past
                    break;
                }
                println!("backfill at {}", unix_sig_time);
            }
            Err(_) => continue,
        }
        before_slot = Some(last_sig);
    }

    print!("Backfill complete \n");
}

pub async fn scrape_transactions(
    rpc_client: &RpcClient,
    before_slot: Option<Signature>,
    fill_sender: &Sender<OpenBookFillEventLog>,
) -> Signature {
    let rpc_config = GetConfirmedSignaturesForAddress2Config {
        before: before_slot,
        until: None,
        limit: Some(150),
        commitment: Some(CommitmentConfig::confirmed()),
    };

    let mut sigs = match rpc_client.get_signatures_for_address_with_config(
        &Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").unwrap(),
        rpc_config,
    ) {
        Ok(s) => s,
        Err(_) => return before_slot.unwrap(),
    };

    sigs.retain(|sig| sig.err.is_none());
    let last_sig = sigs.last().unwrap().clone();

    let txn_config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Json),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    let mut txns = sigs
        .into_iter()
        .map(|sig| {
            rpc_client.get_transaction_with_config(
                &sig.signature.parse::<Signature>().unwrap(),
                txn_config,
            )
        })
        .collect::<Vec<ClientResult<EncodedConfirmedTransactionWithStatusMeta>>>(); // TODO: am I actually getting all the txns?

    let fills = parse_trades_from_openbook_txns(&mut txns);
    if fills.len() > 0 {
        for fill in fills.into_iter() {
            if let Err(_) = fill_sender.send(fill).await {
                panic!("receiver dropped");
            }
        }
    }

    Signature::from_str(&last_sig.signature).unwrap()
}

async fn fetch_market(rpc_client: &RpcClient) {
    let data = rpc_client
        .get_account_data(
            &Pubkey::from_str("8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6").unwrap(),
        )
        .unwrap();

    println!("{}", data.len());

    // simply the market object in TS
}
