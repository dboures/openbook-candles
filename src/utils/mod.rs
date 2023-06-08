use anchor_lang::prelude::Pubkey;
use chrono::{NaiveDateTime, Utc};
use deadpool_postgres::Pool;
use serde_derive::Deserialize;
use solana_sdk::pubkey;

use crate::structs::markets::MarketInfo;

pub const OPENBOOK_KEY: Pubkey = pubkey!("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX");

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

#[derive(Debug, serde::Deserialize)]
pub struct PgConfig {
    pub pg: deadpool_postgres::Config,
    pub pg_max_pool_connections: usize,
    pub pg_use_ssl: bool,
    pub pg_ca_cert_path: Option<String>,
    pub pg_client_key_path: Option<String>,
}

impl PgConfig {
    pub fn from_env() -> Result<Self, config::ConfigError> {
        config::Config::builder()
            .add_source(config::Environment::default().separator("_"))
            .add_source(config::Environment::default())
            .build()?
            .try_deserialize()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub rpc_url: String,
}

pub struct WebContext {
    pub rpc_url: String,
    pub markets: Vec<MarketInfo>,
    pub pool: Pool,
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
