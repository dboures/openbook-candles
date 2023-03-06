use solana_client::client_error::Result as ClientResult;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
};
use std::io::Error;

use anchor_lang::{event, AnchorDeserialize, AnchorSerialize};
use solana_sdk::pubkey::Pubkey;

const PROGRAM_DATA: &str = "Program data: ";

#[event]
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct FillEventLog {
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

pub fn parse_fill_events_from_txns(
    txns: &mut Vec<ClientResult<EncodedConfirmedTransactionWithStatusMeta>>,
) -> Vec<FillEventLog> {
    let mut fills_vector = Vec::<FillEventLog>::new();
    for txn in txns.iter_mut() {
        // println!("{:#?}\n", txn.as_ref());

        // fugly
        match txn {
            Ok(t) => {
                if let Some(m) = &t.transaction.meta {
                    // println!("{:#?}\n", m.log_messages);

                    match &m.log_messages {
                        OptionSerializer::Some(logs) => match parse_fill_events_from_logs(logs) {
                            Some(mut events) => fills_vector.append(&mut events),
                            None => {}
                        },
                        OptionSerializer::None => {}
                        OptionSerializer::Skip => {}
                    }
                }
            }
            Err(_) => {} //println!("goo: {:?}", e),
        }
    }
    return fills_vector;
}

fn parse_fill_events_from_logs(logs: &Vec<String>) -> Option<Vec<FillEventLog>> {
    let mut fills_vector = Vec::<FillEventLog>::new();
    for l in logs {
        match l.strip_prefix(PROGRAM_DATA) {
            Some(log) => {
                let borsh_bytes = match anchor_lang::__private::base64::decode(log) {
                    Ok(borsh_bytes) => borsh_bytes,
                    _ => continue,
                };
                let mut slice: &[u8] = &borsh_bytes[8..];
                let event: Result<FillEventLog, Error> =
                    anchor_lang::AnchorDeserialize::deserialize(&mut slice);

                match event {
                    Ok(e) => {
                        fills_vector.push(e);
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
