use deadpool_postgres::Pool;
use futures::future::join_all;
use log::{debug, warn};
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_config::RpcTransactionConfig,
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::UiTransactionEncoding;
use std::{collections::HashMap, time::Duration as WaitDuration};

use crate::{
    database::{
        fetch::fetch_worker_transactions,
        insert::{build_transactions_insert_statement, insert_fills_atomically},
    },
    structs::transaction::PgTransaction,
    utils::{AnyhowWrap, OPENBOOK_KEY},
    worker::metrics::{METRIC_FILLS_TOTAL, METRIC_RPC_ERRORS_TOTAL, METRIC_TRANSACTIONS_TOTAL},
};

use super::parsing::parse_trades_from_openbook_txns;

pub async fn scrape_signatures(rpc_url: String, pool: &Pool) -> anyhow::Result<()> {
    let rpc_client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());

    loop {
        let rpc_config = GetConfirmedSignaturesForAddress2Config {
            before: None,
            until: None,
            limit: None,
            commitment: Some(CommitmentConfig::confirmed()),
        };

        let sigs = match rpc_client
            .get_signatures_for_address_with_config(&OPENBOOK_KEY, rpc_config)
            .await
        {
            Ok(sigs) => sigs,
            Err(e) => {
                warn!("rpc error in get_signatures_for_address_with_config: {}", e);
                METRIC_RPC_ERRORS_TOTAL
                    .with_label_values(&["getSignaturesForAddress"])
                    .inc();
                continue;
            }
        };
        if sigs.is_empty() {
            debug!("No signatures found, trying again");
            continue;
        }
        let transactions: Vec<PgTransaction> = sigs
            .into_iter()
            .map(PgTransaction::from_rpc_confirmed_transaction)
            .collect();

        debug!("Scraper writing: {:?} txns to DB\n", transactions.len());
        let upsert_statement = build_transactions_insert_statement(transactions);
        let client = pool.get().await?;
        let num_txns = client
            .execute(&upsert_statement, &[])
            .await
            .map_err_anyhow()?;
        METRIC_TRANSACTIONS_TOTAL.inc_by(num_txns);
    }
    // TODO: graceful shutdown
}

pub async fn scrape_fills(
    worker_id: i32,
    rpc_url: String,
    pool: &Pool,
    target_markets: &HashMap<Pubkey, String>,
) -> anyhow::Result<()> {
    debug!("Worker {} started \n", worker_id);
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    loop {
        let transactions = fetch_worker_transactions(worker_id, pool).await?;
        if transactions.is_empty() {
            debug!("No signatures found by worker {}", worker_id);
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

        let (fills, completed_sigs) =
            parse_trades_from_openbook_txns(&mut txns, sig_strings, target_markets);
        for fill in fills.iter() {
            let market_name = target_markets.get(&fill.market).unwrap();
            METRIC_FILLS_TOTAL.with_label_values(&[market_name]).inc();
        }
        // Write fills to the database, and update properly fetched transactions as processed
        insert_fills_atomically(pool, worker_id, fills, completed_sigs).await?;
    }
}
