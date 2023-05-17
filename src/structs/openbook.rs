use anchor_lang::{event, AnchorDeserialize, AnchorSerialize};
use chrono::{DateTime, Utc};
use num_traits::Pow;
use solana_sdk::pubkey::Pubkey;
use tokio_postgres::Row;

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
    pub native_qty_paid: f64,
    pub native_qty_received: f64,
    pub native_fee_or_rebate: f64,
}
impl PgOpenBookFill {
    pub fn from_row(row: Row) -> Self {
        PgOpenBookFill {
            time: row.get(0),
            bid: row.get(1),
            maker: row.get(2),
            native_qty_paid: row.get(3),
            native_qty_received: row.get(4),
            native_fee_or_rebate: row.get(5),
        }
    }
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
) -> (f64, f64) {
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

pub fn token_factor(decimals: u8) -> f64 {
    10f64.pow(decimals as f64)
}
