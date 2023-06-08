use chrono::{DateTime, Utc};
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use tokio_postgres::Row;

use crate::utils::{to_timestampz, OPENBOOK_KEY};

#[derive(Clone, Debug, PartialEq)]
pub struct PgTransaction {
    pub signature: String,
    pub program_pk: String,
    pub block_datetime: DateTime<Utc>,
    pub slot: u64,
    pub err: bool,
    pub processed: bool,
    pub worker_partition: i32,
}

pub const NUM_TRANSACTION_PARTITIONS: u64 = 10;

impl PgTransaction {
    pub fn from_rpc_confirmed_transaction(
        rpc_confirmed_transaction: RpcConfirmedTransactionStatusWithSignature,
    ) -> Self {
        PgTransaction {
            signature: rpc_confirmed_transaction.signature,
            program_pk: OPENBOOK_KEY.to_string(),
            block_datetime: to_timestampz(rpc_confirmed_transaction.block_time.unwrap() as u64),
            slot: rpc_confirmed_transaction.slot,
            err: rpc_confirmed_transaction.err.is_some(),
            processed: false,
            worker_partition: (rpc_confirmed_transaction.slot % NUM_TRANSACTION_PARTITIONS) as i32,
        }
    }

    pub fn from_row(row: Row) -> Self {
        let slot_raw = row.get::<usize, i64>(3);
        PgTransaction {
            signature: row.get(0),
            program_pk: row.get(1),
            block_datetime: row.get(2),
            slot: slot_raw as u64,
            err: row.get(4),
            processed: row.get(5),
            worker_partition: row.get(6),
        }
    }
}

pub enum ProcessState {
    Processed,
    Unprocessed,
}
