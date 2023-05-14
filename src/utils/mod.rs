use chrono::{NaiveDateTime, Utc};
use serde_derive::Deserialize;
use sqlx::{Pool, Postgres};

use crate::structs::markets::MarketInfo;

pub trait AnyhowWrap {
    type Value;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value>;
}

impl<T, E: std::fmt::Debug> AnyhowWrap for Result<T, E> {
    type Value = T;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value> {
        self.map_err(|err| anyhow::anyhow!("{:?}", err))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub rpc_url: String,
    pub database_url: String,
    pub max_pg_pool_connections: usize,
    pub use_ssl: bool,
    pub ca_cert_path: String,
    pub client_key_path: String,
}

pub struct WebContext {
    pub rpc_url: String,
    pub markets: Vec<MarketInfo>,
    pub pool: Pool<Postgres>,
}

#[allow(deprecated)]
pub fn to_timestampz(seconds: u64) -> chrono::DateTime<Utc> {
    chrono::DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(seconds as i64, 0), Utc)
}

pub(crate) fn f64_max(a: f64, b: f64) -> f64 {
    if a >= b {
        a
    } else {
        b
    }
}

pub(crate) fn f64_min(a: f64, b: f64) -> f64 {
    if a < b {
        a
    } else {
        b
    }
}
