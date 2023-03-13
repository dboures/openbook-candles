use solana_client::client_error::Result as ClientResult;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
};
use std::{collections::HashMap, io::Error};

use anchor_lang::{event, AnchorDeserialize, AnchorSerialize};
use solana_sdk::pubkey::Pubkey;

const PROGRAM_DATA: &str = "Program data: ";

#[event]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OpenBookFillEventLog {
    pub market: Pubkey,
    pub open_orders: Pubkey,
    pub open_orders_owner: Pubkey,
    pub bid: bool,
    pub maker: bool,
    pub native_qty_paid: u64,
    pub native_qty_received: u64,
    pub native_fee_or_rebate: u64,
    pub order_id: u128,
    pub owner_slot: u8,
    pub fee_tier: u8,
    pub client_order_id: Option<u64>,
    pub referrer_rebate: Option<u64>,
}

#[derive(Copy, Clone, AnchorDeserialize)]
#[cfg_attr(target_endian = "little", derive(Debug))]
#[repr(packed)]
pub struct MarketState {
    // 0
    pub account_flags: u64, // Initialized, Market

    // 1
    pub own_address: [u64; 4],

    // 5
    pub vault_signer_nonce: u64,
    // 6
    pub coin_mint: [u64; 4],
    // 10
    pub pc_mint: [u64; 4],

    // 14
    pub coin_vault: [u64; 4],
    // 18
    pub coin_deposits_total: u64,
    // 19
    pub coin_fees_accrued: u64,

    // 20
    pub pc_vault: [u64; 4],
    // 24
    pub pc_deposits_total: u64,
    // 25
    pub pc_fees_accrued: u64,

    // 26
    pub pc_dust_threshold: u64,

    // 27
    pub req_q: [u64; 4],
    // 31
    pub event_q: [u64; 4],

    // 35
    pub bids: [u64; 4],
    // 39
    pub asks: [u64; 4],

    // 43
    pub coin_lot_size: u64,
    // 44
    pub pc_lot_size: u64,

    // 45
    pub fee_rate_bps: u64,
    // 46
    pub referrer_rebates_accrued: u64,
}

pub fn parse_trades_from_openbook_txns(
    txns: &mut Vec<ClientResult<EncodedConfirmedTransactionWithStatusMeta>>,
    target_markets: &HashMap<Pubkey, u8>,
) -> Vec<OpenBookFillEventLog> {
    let mut fills_vector = Vec::<OpenBookFillEventLog>::new();
    for txn in txns.iter_mut() {
        match txn {
            Ok(t) => {
                if let Some(m) = &t.transaction.meta {
                    match &m.log_messages {
                        OptionSerializer::Some(logs) => {
                            match parse_openbook_fills_from_logs(logs, target_markets) {
                                Some(mut events) => fills_vector.append(&mut events),
                                None => {}
                            }
                        }
                        OptionSerializer::None => {}
                        OptionSerializer::Skip => {}
                    }
                }
            }
            Err(_) => {}
        }
    }
    fills_vector
}

fn parse_openbook_fills_from_logs(
    logs: &Vec<String>,
    target_markets: &HashMap<Pubkey, u8>,
) -> Option<Vec<OpenBookFillEventLog>> {
    let mut fills_vector = Vec::<OpenBookFillEventLog>::new();
    for l in logs {
        match l.strip_prefix(PROGRAM_DATA) {
            Some(log) => {
                let borsh_bytes = match anchor_lang::__private::base64::decode(log) {
                    Ok(borsh_bytes) => borsh_bytes,
                    _ => continue,
                };
                let mut slice: &[u8] = &borsh_bytes[8..];
                let event: Result<OpenBookFillEventLog, Error> =
                    anchor_lang::AnchorDeserialize::deserialize(&mut slice);

                match event {
                    Ok(e) => {
                        if target_markets.contains_key(&e.market) {
                            fills_vector.push(e);
                        }
                    }
                    _ => continue,
                }
            }
            _ => (),
        }
    }

    if fills_vector.len() > 0 {
        return Some(fills_vector);
    } else {
        return None;
    }
}
