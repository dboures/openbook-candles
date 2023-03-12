// use jsonrpc_core_client::transports::ws;

// use anchor_client::{
//     anchor_lang::{self, event, AnchorDeserialize, AnchorSerialize, Discriminator},
//     ClientError as AnchorClientError, Cluster,
// };
// use log::*;
// use solana_account_decoder::UiAccountEncoding;
// use solana_client::{
//     pubsub_client::{PubsubClient, PubsubClientSubscription},
//     rpc_config::{
//         RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionLogsConfig,
//         RpcTransactionLogsFilter,
//     },
//     rpc_response::{Response, RpcKeyedAccount, RpcLogsResponse},
// };
// use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
// use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Keypair};
// use std::{io::Error, rc::Rc, str::FromStr, time::Duration};

// use crate::utils::AnyhowWrap;
// use crate::{
//     database::initialize::{connect_to_database, setup_database},
//     utils::Config,
// };

// use super::parsing::parse_and_save_logs;

// const PROGRAM_LOG: &str = "Program log: ";

// pub async fn listen_logs(config: Config) -> anyhow::Result<()> {
//     let ws_url = config.rpc_ws_url;

//     let transaction_logs_config = RpcTransactionLogsConfig {
//         commitment: Some(CommitmentConfig::confirmed()),
//     };

//     let transaction_logs_filter = RpcTransactionLogsFilter::Mentions(vec![String::from(
//         "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
//     )]);

//     let (_log_sub, log_receiver) =
//         PubsubClient::logs_subscribe(&ws_url, transaction_logs_filter, transaction_logs_config)?;

//     loop {
//         let response = log_receiver.recv().map_err_anyhow()?; // TODO: what to do if disconnects
//         if response.value.err.is_none() {
//             parse_and_save_logs(&response.value.logs);
//         }
//     }
// }

pub async fn listen_program_accounts() {
    // let payer = Rc::new(Keypair::new());

    // let connect = ws::try_connect::<RpcSolPubSubClient>(&ws_url).map_err_anyhow()?;

    // let client = connect.await.map_err_anyhow()?;
    // let openbook_key = Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").unwrap();

    // let cluster = Cluster::Custom(rpc_url.to_string(), ws_url.to_string());

    // let client = AnchorClient::new_with_options(cluster, payer, CommitmentConfig::confirmed());

    // let dex_program = client.program(openbook_key);

    // let account_info_config = RpcAccountInfoConfig {
    //     encoding: Some(UiAccountEncoding::Base64),
    //     data_slice: None,
    //     commitment: Some(CommitmentConfig::processed()),
    //     min_context_slot: None,
    // };

    // let program_accounts_config = RpcProgramAccountsConfig {
    //     filters: None, // TODO: add filters for markets we care about
    //     with_context: Some(true),
    //     account_config: account_info_config.clone(),
    // };

    // let (_program_sub, prog_receiver) = PubsubClient::program_subscribe(
    //     &ws_url,
    //     &openbook_key,
    //     Some(program_accounts_config)
    // )?;
}
