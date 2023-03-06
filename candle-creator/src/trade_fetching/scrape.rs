use anyhow::Result;
use solana_client::{
    client_error::Result as ClientResult,
    rpc_client::{GetConfirmedSignaturesForAddress2Config, RpcClient},
    rpc_config::RpcTransactionConfig,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
    UiTransactionEncoding,
};
use sqlx::{Pool, Postgres};
use std::{str::FromStr, time::Duration};
use tokio::sync::mpsc::Sender;

use crate::utils::Config;

use super::parsing::{parse_fill_events_from_txns, FillEventLog};

pub async fn scrape(config: &Config, fill_event_sender: Sender<FillEventLog>) {
    let url = &config.rpc_http_url;
    let rpc_client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());

    let openbook_key = Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").unwrap();

    // let start_slot = ; //set the start point at 3 months from now (config above)
    loop {
        let config = GetConfirmedSignaturesForAddress2Config {
            before: None,
            until: None,
            limit: Some(150), // TODO: None
            commitment: Some(CommitmentConfig::confirmed()),
        };

        let mut sigs = rpc_client
            .get_signatures_for_address_with_config(&openbook_key, config)
            .unwrap();

        sigs.retain(|sig| sig.err.is_none());

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

        let fill_events = parse_fill_events_from_txns(&mut txns);
        if fill_events.len() > 0 {
            for event in fill_events.into_iter() {
                if let Err(_) = fill_event_sender.send(event).await {
                    println!("receiver dropped");
                    return;
                }
            }
        }

        print!("Ding fires are done \n\n");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // increment slot somehow (or move forward in time or something)
    }
}
