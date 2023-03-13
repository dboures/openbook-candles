use anchor_lang::{event, AnchorDeserialize, AnchorSerialize};
use chrono::{DateTime, Utc};
use num_traits::{FromPrimitive, ToPrimitive};
use serde::Serialize;
use solana_sdk::pubkey::Pubkey;
use sqlx::types::Decimal;

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

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct PgOpenBookFill {
    pub time: DateTime<Utc>,
    pub bid: bool,
    pub maker: bool,
    pub native_qty_paid: Decimal,
    pub native_qty_received: Decimal,
    pub native_fee_or_rebate: Decimal,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PgTrader {
    pub open_orders_owner: String,
    pub raw_ask_size: Decimal,
    pub raw_bid_size: Decimal,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Trader {
    pub pubkey: String,
    pub volume_base_units: f64,
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

pub fn calculate_fill_price_and_size(
    fill: PgOpenBookFill,
    base_decimals: u8,
    quote_decimals: u8,
) -> (Decimal, Decimal) {
    if fill.bid {
        let price_before_fees = if fill.maker {
            fill.native_qty_paid + fill.native_fee_or_rebate
        } else {
            fill.native_qty_paid - fill.native_fee_or_rebate
        };
        let price = (price_before_fees * token_factor(base_decimals))
            / (token_factor(quote_decimals) * fill.native_qty_received);
        let size = fill.native_qty_received / token_factor(base_decimals);
        (price, size)
    } else {
        let price_before_fees = if fill.maker {
            fill.native_qty_received - fill.native_fee_or_rebate
        } else {
            fill.native_qty_received + fill.native_fee_or_rebate
        };
        let price = (price_before_fees * token_factor(base_decimals))
            / (token_factor(quote_decimals) * fill.native_qty_paid);
        let size = fill.native_qty_paid / token_factor(base_decimals);
        (price, size)
    }
}

pub fn calculate_trader_volume(trader: PgTrader, base_decimals: u8) -> Trader {
    let bid_size = trader.raw_bid_size / token_factor(base_decimals);
    let ask_size = trader.raw_ask_size / token_factor(base_decimals);

    Trader {
        pubkey: trader.open_orders_owner,
        volume_base_units: (bid_size + ask_size).to_f64().unwrap(),
        // TODO: quote volume
    }
}

fn token_factor(decimals: u8) -> Decimal {
    Decimal::from_u64(10u64.pow(decimals as u32)).unwrap()
}
