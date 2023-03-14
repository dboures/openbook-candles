use anchor_lang::AnchorDeserialize;
use serde::{Deserialize, Serialize};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcAccountInfoConfig};
use solana_sdk::{commitment_config::CommitmentConfig, program_pack::Pack, pubkey::Pubkey};
use spl_token::state::Mint;
use std::{collections::HashMap, fs::File, str::FromStr};

use crate::utils::Config;

use super::openbook::MarketState;

#[derive(Debug, Clone, Serialize)]
pub struct MarketInfo {
    pub name: String,
    pub address: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_mint_key: String,
    pub quote_mint_key: String,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarketConfig {
    pub name: String,
    pub address: String,
}

pub fn load_markets(path: &str) -> Vec<MarketConfig> {
    let reader = File::open(path).unwrap();
    serde_json::from_reader(reader).unwrap()
}

pub fn valid_market(market_name: &str, markets: &Vec<MarketInfo>) -> bool {
    markets.iter().any(|x| x.name == market_name)
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
