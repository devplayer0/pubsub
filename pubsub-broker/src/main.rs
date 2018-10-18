use std::error::Error as StdError;
use std::sync::{Arc, Mutex, Condvar};
use std::process;
use std::net::{SocketAddr, ToSocketAddrs};

#[macro_use]
extern crate log;
extern crate simplelog;
extern crate ctrlc;

extern crate pubsub_broker;

use simplelog::{LevelFilter, TermLogger};

use pubsub_broker::Broker;

pub struct Config {
    bind_addrs: Vec<SocketAddr>,
}
impl Default for Config {
    fn default() -> Config {
        Config {
            bind_addrs: "localhost:26999".to_socket_addrs().unwrap().collect(),
        }
    }
}
impl Config {
    pub fn bind_addrs(&self) -> &Vec<SocketAddr> {
        &self.bind_addrs
    }
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

    let broker = Broker::bind(config.bind_addrs(), true)?;

    let &(ref stop_lock, ref stop_cond) = &*stop;
    let mut stop = stop_lock.lock().unwrap();
    while !*stop {
        stop = stop_cond.wait(stop).unwrap();
    }

    broker.stop();
    Ok(())
}
fn main() {
    TermLogger::init(LevelFilter::Debug, simplelog::Config::default()).expect("failed to initialize logger");

    if let Err(e) = run(Config::default()) {
        error!("{}", e);
        process::exit(1);
    }
}
