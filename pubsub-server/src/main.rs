#![feature(wait_until)]

use std::error::Error as StdError;
use std::sync::{Arc, Mutex, Condvar};
use std::process;
use std::net::{SocketAddr, ToSocketAddrs};

#[macro_use]
extern crate log;
extern crate simplelog;
extern crate clap;
extern crate ctrlc;

extern crate pubsub_broker;

use simplelog::{LevelFilter, TermLogger};

use pubsub_broker::util;
use pubsub_broker::Broker;

pub struct Config {
    log_level: LevelFilter,

    bind_addrs: Vec<SocketAddr>,
    threads: usize,
    enable_keepalive: bool,
    packet_size: u16,
}
impl Default for Config {
    fn default() -> Config {
        Config {
            log_level: LevelFilter::Info,

            bind_addrs: "localhost:26999".to_socket_addrs().unwrap().collect(),
            threads: 0,
            enable_keepalive: false,
            packet_size: 0,
        }
    }
}
impl<'a> From<clap::ArgMatches<'a>> for Config {
    fn from(args: clap::ArgMatches) -> Config {
        Config {
            log_level: util::verbosity_to_log_level(args.occurrences_of("verbose") as usize),

            bind_addrs: args.values_of("bind_addrs").unwrap().map(util::parse_addr).map(|r| r.unwrap()).flatten().collect(),
            threads: args.value_of("threads").unwrap().parse().unwrap(),
            enable_keepalive: !args.is_present("disable_keepalive"),
            packet_size: args.value_of("packet_size").unwrap().parse().unwrap(),
        }
    }
}
impl Config {
    pub fn bind_addrs(&self) -> &Vec<SocketAddr> {
        &self.bind_addrs
    }
}

fn args<'a>() -> clap::ArgMatches<'a> {
    clap::App::new("JQTT broker")
        .version("0.1")
        .author("Jack O'Sullivan <jackos1998@gmail.com>")
        .arg(clap::Arg::with_name("verbose")
             .short("v")
             .long("verbose")
             .multiple(true)
             .help("Print extra log messages"))
        .arg(clap::Arg::with_name("bind_addrs")
             .short("b")
             .long("bind-addresses")
             .value_name("address[:port],...")
             .help("Set bind addresses")
             .use_delimiter(true)
             .default_value("localhost:26999")
             .takes_value(true)
             .validator(|val| util::parse_addr(&val).map(|_| ()).map_err(|e| format!("{}", e))))
        .arg(clap::Arg::with_name("threads")
             .short("t")
             .long("threads")
             .value_name("number of threads")
             .help("Set number of worker threads")
             .default_value("0")
             .hide_default_value(true)
             .takes_value(true)
             .validator(util::validate_parse::<u16, _>))
        .arg(clap::Arg::with_name("disable_keepalive")
             .long("disable-keepalive")
             .help("Disable keepalive"))
        .arg(clap::Arg::with_name("packet_size")
             .long("packet-size")
             .value_name("size in bytes")
             .help("Packet size (set to 0 for default based on IPv4 / IPv6)")
             .default_value("0")
             .hide_default_value(true)
             .takes_value(true)
             .validator(util::validate_parse::<u16, _>))
        .get_matches()
}
fn run(config: Config) -> Result<(), Box<dyn StdError>> {
    info!("starting broker");

    let stop = Arc::new((Mutex::new(false), Condvar::new()));
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            let &(ref stop_lock, ref stop_cond) = &*stop;

            let mut stop = stop_lock.lock().unwrap();
            if *stop {
                return;
            }

            info!("shutting down...");
            *stop = true;
            stop_cond.notify_one();
        })?;
    }

    let broker = Broker::bind(config.bind_addrs(), config.threads, config.enable_keepalive, config.packet_size, config.packet_size)?;
    {
        let &(ref lock, ref cvar) = &*stop;
        let _guard = cvar.wait_until(lock.lock().unwrap(), |stop| *stop).unwrap();
    }

    broker.stop();
    Ok(())
}
fn main() {
    let config: Config = args().into();
    TermLogger::init(config.log_level, simplelog::Config::default()).expect("failed to initialize logger");

    if let Err(e) = run(config) {
        error!("{}", e);
        process::exit(1);
    }
}
