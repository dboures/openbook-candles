use futures::future::join_all;
use log::{debug, warn};
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_config::RpcTransactionConfig,
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::UiTransactionEncoding;
use std::{collections::HashMap, str::FromStr, time::Duration as WaitDuration};
use tokio::sync::mpsc::Sender;

use crate::{
    structs::openbook::OpenBookFillEvent,
    utils::{Config, OPENBOOK_KEY},
    worker::metrics::{METRIC_FILLS_TOTAL, METRIC_RPC_ERRORS_TOTAL},
};

use super::parsing::parse_trades_from_openbook_txns;

pub async fn scrape(
    config: &Config,
    fill_sender: &Sender<OpenBookFillEvent>,
    target_markets: &HashMap<Pubkey, String>,
) {
    let rpc_client =
        RpcClient::new_with_commitment(config.rpc_url.clone(), CommitmentConfig::processed());

    let before_slot = None;
    loop {
        scrape_transactions(
            &rpc_client,
            before_slot,
            Some(150),
            fill_sender,
            target_markets,
        )
        .await;
        tokio::time::sleep(WaitDuration::from_millis(250)).await;
    }
}

pub async fn scrape_transactions(
    rpc_client: &RpcClient,
    before_sig: Option<Signature>,
    limit: Option<usize>,
    fill_sender: &Sender<OpenBookFillEvent>,
    target_markets: &HashMap<Pubkey, String>,
) -> Option<Signature> {
    let rpc_config = GetConfirmedSignaturesForAddress2Config {
        before: before_sig,
        until: None,
        limit,
        commitment: Some(CommitmentConfig::confirmed()),
    };

    let mut sigs = match rpc_client
        .get_signatures_for_address_with_config(
            &OPENBOOK_KEY,
            rpc_config,
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            warn!("rpc error in get_signatures_for_address_with_config: {}", e);
            METRIC_RPC_ERRORS_TOTAL
                .with_label_values(&["getSignaturesForAddress"])
                .inc();
            return before_sig;
        }
    };

    if sigs.is_empty() {
        debug!("No signatures found");
        return before_sig;
    }

    let last = sigs.last().unwrap();
    let request_last_sig = Signature::from_str(&last.signature).unwrap();

    sigs.retain(|sig| sig.err.is_none());
    if sigs.last().is_none() {
        return Some(request_last_sig);
    }

    let txn_config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Json),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    let sig_strings = sigs
        .iter()
        .map(|t| t.signature.clone())
        .collect::<Vec<String>>();

    let signatures: Vec<_> = sigs
        .into_iter()
        .map(|sig| sig.signature.parse::<Signature>().unwrap())
        .collect();

    let txn_futs: Vec<_> = signatures
        .iter()
        .map(|s| rpc_client.get_transaction_with_config(s, txn_config))
        .collect();

    let mut txns = join_all(txn_futs).await;

    let fills = parse_trades_from_openbook_txns(&mut txns, &sig_strings, target_markets);
    if !fills.is_empty() {
        for fill in fills.into_iter() {
            let market_name = target_markets.get(&fill.market).unwrap();
            if let Err(_) = fill_sender.send(fill).await {
                panic!("receiver dropped");
            }
            METRIC_FILLS_TOTAL.with_label_values(&[market_name]).inc();
        }
    }

    Some(request_last_sig)
}
