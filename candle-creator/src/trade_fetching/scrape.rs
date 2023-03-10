use anchor_lang::AnchorDeserialize;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    client_error::Result as ClientResult,
    rpc_client::{GetConfirmedSignaturesForAddress2Config, RpcClient},
    rpc_config::{RpcTransactionConfig, RpcAccountInfoConfig},
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature, program_pack::Pack};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use spl_token::state::Mint;
use std::{str::FromStr, time::Duration};
use tokio::sync::mpsc::Sender;

use crate::{utils::{Config, MarketInfo, MarketConfig}, trade_fetching::parsing::{MarketState}};

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

pub async fn fetch_market_infos(config: &Config, markets: Vec<MarketConfig>) -> anyhow::Result<Vec<MarketInfo>> {
    let url = &config.rpc_url;
    let rpc_client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());

    let rpc_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        data_slice: None,
        commitment: Some(CommitmentConfig::confirmed()),
        min_context_slot: None,
    };

    let market_keys = markets.iter().map(|x| Pubkey::from_str(&x.address).unwrap()).collect::<Vec<Pubkey>>();
    let mut market_results = rpc_client.get_multiple_accounts_with_config(&market_keys, rpc_config.clone()).unwrap().value;

    let mut mint_keys = Vec::new();

    let mut market_infos = market_results.iter_mut().map(|mut r| {
        let get_account_result = r.as_mut().unwrap();

        let mut market_bytes: &[u8] = &mut get_account_result.data[5..];
        let raw_market: MarketState = AnchorDeserialize::deserialize(&mut market_bytes).unwrap();

        let base_mint = serum_bytes_to_pubkey(raw_market.coin_mint);
        let quote_mint = serum_bytes_to_pubkey(raw_market.pc_mint);
        mint_keys.push(base_mint);
        mint_keys.push(quote_mint);

        MarketInfo {
            name: "".to_string(),
            address: serum_bytes_to_pubkey(raw_market.own_address).to_string(),
            base_decimals: 0,
            quote_decimals: 0,
            base_lot_size: raw_market.coin_lot_size,
            quote_lot_size: raw_market.pc_lot_size,
        }
    }).collect::<Vec<MarketInfo>>();

    let mint_results = rpc_client.get_multiple_accounts_with_config(&mint_keys, rpc_config).unwrap().value;
    for i in (0..mint_results.len()).step_by(2) {
        let mut base_mint_account = mint_results[i].as_ref().unwrap().clone();
        let mut quote_mint_account = mint_results[i+1].as_ref().unwrap().clone();

        let mut base_mint_bytes: &[u8] = &mut base_mint_account.data[..];
        let mut quote_mint_bytes: &[u8] = &mut quote_mint_account.data[..];

        let base_mint = Mint::unpack_from_slice(&mut base_mint_bytes).unwrap();
        let quote_mint = Mint::unpack_from_slice(&mut quote_mint_bytes).unwrap();

        market_infos[i / 2].name = markets[i / 2].name.clone();
        market_infos[i / 2].base_decimals = base_mint.decimals;
        market_infos[i / 2].quote_decimals = quote_mint.decimals;
    }

    Ok(market_infos)
}

fn serum_bytes_to_pubkey(data: [u64; 4]) -> Pubkey {
    let mut res = [0; 32];
    for i in 0..4 {
        res[8*i..][..8].copy_from_slice(&data[i].to_le_bytes());
    }
    Pubkey::new_from_array(res)
}
