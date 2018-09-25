use std::process;

#[macro_use]
extern crate log;
extern crate simplelog;

extern crate pubsub_broker;

use simplelog::{LevelFilter, TermLogger};

use pubsub_broker::config::Config;

fn main() {
    TermLogger::init(LevelFilter::Debug, simplelog::Config::default()).expect("failed to initialize logger");

    if let Err(e) = pubsub_broker::run(Config::default()) {
        error!("{}", e);
        process::exit(1);
    }
}
