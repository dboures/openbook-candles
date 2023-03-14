use anchor_lang::AnchorDeserialize;
use futures::future::join_all;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_config::{RpcAccountInfoConfig, RpcTransactionConfig},
};
use solana_sdk::{
    commitment_config::CommitmentConfig, program_pack::Pack, pubkey::Pubkey, signature::Signature,
};
use solana_transaction_status::UiTransactionEncoding;
use spl_token::state::Mint;
use std::{collections::HashMap, str::FromStr, time::Duration as WaitDuration};
use tokio::sync::mpsc::Sender;

use crate::{
    structs::{
        markets::{MarketConfig, MarketInfo},
        openbook::{MarketState, OpenBookFillEventLog},
    },
    utils::Config,
};

use super::parsing::parse_trades_from_openbook_txns;

pub async fn scrape(
    config: &Config,
    fill_sender: &Sender<OpenBookFillEventLog>,
    target_markets: &HashMap<Pubkey, u8>,
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
    fill_sender: &Sender<OpenBookFillEventLog>,
    target_markets: &HashMap<Pubkey, u8>,
) -> Option<Signature> {
    let rpc_config = GetConfirmedSignaturesForAddress2Config {
        before: before_sig,
        until: None,
        limit,
        commitment: Some(CommitmentConfig::confirmed()),
    };

    let mut sigs = match rpc_client
        .get_signatures_for_address_with_config(
            &Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").unwrap(),
            rpc_config,
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            println!("Error in get_signatures_for_address_with_config: {}", e);
            return before_sig;
        }
    };

    if sigs.len() == 0 {
        println!("No signatures found");
        return before_sig;
    }

    let last = sigs.last().clone().unwrap();
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

    Some(request_last_sig)
}

pub async fn fetch_market_infos(
    config: &Config,
    markets: Vec<MarketConfig>,
) -> anyhow::Result<Vec<MarketInfo>> {
    let rpc_client =
        RpcClient::new_with_commitment(config.rpc_url.clone(), CommitmentConfig::processed());

    let rpc_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        data_slice: None,
        commitment: Some(CommitmentConfig::confirmed()),
        min_context_slot: None,
    };

    let market_keys = markets
        .iter()
        .map(|x| Pubkey::from_str(&x.address).unwrap())
        .collect::<Vec<Pubkey>>();
    let mut market_results = rpc_client
        .get_multiple_accounts_with_config(&market_keys, rpc_config.clone())
        .await?
        .value;

    let mut mint_key_map = HashMap::new();

    let mut market_infos = market_results
        .iter_mut()
        .map(|r| {
            let get_account_result = r.as_mut().unwrap();

            let mut market_bytes: &[u8] = &mut get_account_result.data[5..];
            let raw_market: MarketState =
                AnchorDeserialize::deserialize(&mut market_bytes).unwrap();

            let market_address_string = serum_bytes_to_pubkey(raw_market.own_address).to_string();
            let base_mint_key = serum_bytes_to_pubkey(raw_market.coin_mint);
            let quote_mint_key = serum_bytes_to_pubkey(raw_market.pc_mint);
            mint_key_map.insert(base_mint_key, 0);
            mint_key_map.insert(quote_mint_key, 0);

            let market_name = markets
                .iter()
                .find(|x| x.address == market_address_string)
                .unwrap()
                .name
                .clone();

            MarketInfo {
                name: market_name,
                address: market_address_string,
                base_decimals: 0,
                quote_decimals: 0,
                base_mint_key: base_mint_key.to_string(),
                quote_mint_key: quote_mint_key.to_string(),
                base_lot_size: raw_market.coin_lot_size,
                quote_lot_size: raw_market.pc_lot_size,
            }
        })
        .collect::<Vec<MarketInfo>>();

    let mint_keys = mint_key_map.keys().cloned().collect::<Vec<Pubkey>>();

    let mint_results = rpc_client
        .get_multiple_accounts_with_config(&mint_keys, rpc_config)
        .await?
        .value;
    for i in 0..mint_results.len() {
        let mut mint_account = mint_results[i].as_ref().unwrap().clone();
        let mut mint_bytes: &[u8] = &mut mint_account.data[..];
        let mint = Mint::unpack_from_slice(&mut mint_bytes).unwrap();

        mint_key_map.insert(mint_keys[i], mint.decimals);
    }

    for i in 0..market_infos.len() {
        let base_key = Pubkey::from_str(&market_infos[i].base_mint_key).unwrap();
        let quote_key = Pubkey::from_str(&market_infos[i].quote_mint_key).unwrap();
        market_infos[i].base_decimals = *mint_key_map.get(&base_key).unwrap();
        market_infos[i].quote_decimals = *mint_key_map.get(&quote_key).unwrap();
    }

    Ok(market_infos)
}

fn serum_bytes_to_pubkey(data: [u64; 4]) -> Pubkey {
    let mut res = [0; 32];
    for i in 0..4 {
        res[8 * i..][..8].copy_from_slice(&data[i].to_le_bytes());
    }
    Pubkey::new_from_array(res)
}
