use clap::Parser;
use cmd::bins_run;
use tribbler::config::DEFAULT_CONFIG_LOCATION;
use std::{
    process,
    sync::{
        mpsc::{self, Sender},
        Arc,
    },
    time::Duration,
};

use lab::{lab1, lab2};
use log::{error, info, warn, LevelFilter};
use tokio::{join, time};
use tribbler::{addr, config::Config, err::TribResult, storage::MemStorage};

/// starts a number of backend servers using a given bin config file
#[derive(Parser, Debug)]
#[clap(name = "bins-keep")]
struct Args {
    /// log level to use when starting the backends
    #[clap(short, long, default_value = "INFO")]
    log_level: LevelFilter,
    /// bin configuration file
    #[clap(short, long, default_value = DEFAULT_CONFIG_LOCATION)]
    config: String,
    /// addresses to send ready notifications to
    #[clap(short, long)]
    ready_addrs: Vec<String>,

    #[clap(long, default_value = "10")]
    recv_timeout: u64,

    #[clap(short, long, default_value = "127.0.0.1:7799")]
    address: String,
}

// #[derive(Parser, Debug)]
// #[clap(name = "kv-server")]
// struct Options {
//     #[clap(short, long, default_value = "127.0.0.1:7799")]
//     address: String,

//     #[clap(short, long, default_value = "INFO")]
//     log_level: LevelFilter,
// }

#[tokio::main]
async fn main() -> TribResult<()> {
    let pt = bins_run::ProcessType::Keep;
    let args = Args::parse();
    bins_run::main(
        pt,
        args.log_level,
        args.config,
        args.ready_addrs,
        args.recv_timeout,
    )
    .await
    // let cfg = args.config;
    // let config = Arc::new(Config::read(Some(&cfg))?);
    // let (tx, rdy) = mpsc::channel();
    // let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    // let cfg = config.keeper_config(0, Some(tx), Some(shut_rx)).unwrap();
    // info!("starting keeper on {}", cfg.addr());
    // lab2::serve_keeper(cfg).await;
    // Ok(())
}
