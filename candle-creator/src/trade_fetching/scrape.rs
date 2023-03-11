use anchor_lang::AnchorDeserialize;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    client_error::Result as ClientResult,
    rpc_client::{GetConfirmedSignaturesForAddress2Config, RpcClient},
    rpc_config::{RpcAccountInfoConfig, RpcTransactionConfig},
};
use solana_sdk::{
    commitment_config::CommitmentConfig, program_pack::Pack, pubkey::Pubkey, signature::Signature,
};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use spl_token::state::Mint;
use std::{collections::HashMap, str::FromStr, time::Duration};
use tokio::sync::mpsc::Sender;

use crate::{
    database::MarketInfo,
    trade_fetching::parsing::MarketState,
    utils::{Config, MarketConfig},
};

use super::parsing::{parse_trades_from_openbook_txns, OpenBookFillEventLog};

pub async fn scrape(config: &Config, fill_sender: Sender<OpenBookFillEventLog>) {
    let url = &config.rpc_url;
    let rpc_client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());

    let before_slot = None;
    loop {
        scrape_transactions(&rpc_client, before_slot, &fill_sender).await;

        print!("Ding fires are done \n\n");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// pub async fn backfill(config: &Config, fill_sender: Sender<OpenBookFillEventLog>) {
//     let url = &config.rpc_url;
//     let rpc_client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());

//     let mut before_slot: Option<Signature> = None;

//     loop {
//         let last_sig_option = scrape_transactions(&rpc_client, before_slot, &fill_sender).await;

//         if last_sig_option.is_none() {
//             continue;
//         }

//         match rpc_client.get_transaction(&last_sig_option.unwrap(), UiTransactionEncoding::Json) {
//             Ok(txn) => {
//                 let unix_sig_time = rpc_client.get_block_time(txn.slot).unwrap();
//                 if unix_sig_time > 0 {
//                     // TODO: is 3 months in past
//                     break;
//                 }
//                 println!("backfill at {}", unix_sig_time);
//             }
//             Err(_) => continue,
//         }
//         before_slot = last_sig_option;
//     }

//     print!("Backfill complete \n");
// }

pub async fn scrape_transactions(
    rpc_client: &RpcClient,
    before_slot: Option<Signature>,
    fill_sender: &Sender<OpenBookFillEventLog>,
) -> Option<Signature> {
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
        Err(_) => return before_slot, // TODO: add error log
    };

    sigs.retain(|sig| sig.err.is_none());
    let last_sig = sigs.last().unwrap().clone(); // Failed here

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

    Some(Signature::from_str(&last_sig.signature).unwrap())
}

pub async fn fetch_market_infos(
    config: &Config,
    markets: Vec<MarketConfig>,
) -> anyhow::Result<Vec<MarketInfo>> {
    let url = &config.rpc_url;
    let rpc_client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());

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
        .unwrap()
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
        .unwrap()
        .value;
    // println!("{:?}", mint_results);
    // println!("{:?}", mint_keys);
    // println!("{:?}", mint_results.len());
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
