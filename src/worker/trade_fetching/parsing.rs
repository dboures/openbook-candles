use log::warn;
use solana_client::client_error::Result as ClientResult;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
};
use std::{collections::HashMap, io::Error};

use crate::{
    structs::openbook::{OpenBookFillEvent, OpenBookFillEventRaw},
    worker::metrics::METRIC_RPC_ERRORS_TOTAL,
};

const PROGRAM_DATA: &str = "Program data: ";

pub fn parse_trades_from_openbook_txns(
    txns: &mut Vec<ClientResult<EncodedConfirmedTransactionWithStatusMeta>>,
    mut sig_strings: Vec<String>,
    target_markets: &HashMap<Pubkey, String>,
) -> (Vec<OpenBookFillEvent>, Vec<String>) {
    let mut fills_vector = Vec::<OpenBookFillEvent>::new();
    let mut failed_sigs = vec![];
    for (idx, txn) in txns.iter_mut().enumerate() {
        match txn {
            Ok(t) => {
                if let Some(m) = &t.transaction.meta {
                    match &m.log_messages {
                        OptionSerializer::Some(logs) => {
                            match parse_openbook_fills_from_logs(
                                logs,
                                target_markets,
                                sig_strings[idx].clone(),
                                t.block_time.unwrap(),
                            ) {
                                Some(mut events) => fills_vector.append(&mut events),
                                None => {}
                            }
                        }
                        OptionSerializer::None => {}
                        OptionSerializer::Skip => {}
                    }
                }
            }
            Err(e) => {
                warn!("rpc error in get_transaction {}", e);
                failed_sigs.push(sig_strings[idx].clone());
                METRIC_RPC_ERRORS_TOTAL
                    .with_label_values(&["getTransaction"])
                    .inc();
            }
        }
    }
    sig_strings.retain(|s| !failed_sigs.contains(&s));
    (fills_vector, sig_strings)
}

fn parse_openbook_fills_from_logs(
    logs: &Vec<String>,
    target_markets: &HashMap<Pubkey, String>,
    signature: String,
    block_time: i64,
) -> Option<Vec<OpenBookFillEvent>> {
    let mut fills_vector = Vec::<OpenBookFillEvent>::new();
    for (idx, l) in logs.iter().enumerate() {
        match l.strip_prefix(PROGRAM_DATA) {
            Some(log) => {
                let borsh_bytes = match anchor_lang::__private::base64::decode(log) {
                    Ok(borsh_bytes) => borsh_bytes,
                    _ => continue,
                };
                let mut slice: &[u8] = &borsh_bytes[8..];
                let event: Result<OpenBookFillEventRaw, Error> =
                    anchor_lang::AnchorDeserialize::deserialize(&mut slice);

                match event {
                    Ok(e) => {
                        let fill_event = e.into_event(signature.clone(), block_time, idx);
                        if target_markets.contains_key(&fill_event.market) {
                            fills_vector.push(fill_event);
                        }
                    }
                    _ => continue,
                }
            }
            _ => (),
        }
    }

    if !fills_vector.is_empty() {
        Some(fills_vector)
    } else {
        None
    }
}
