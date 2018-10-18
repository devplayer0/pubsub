use std::time::Duration;
use std::thread;

extern crate bytes;
#[macro_use]
extern crate log;
extern crate simplelog;
extern crate pubsub_client;

use bytes::{Buf, Reader};
use simplelog::{LevelFilter, TermLogger};
use pubsub_client::{Error, Client, Message, MessageCollector, CompleteMessageListener};

struct TestListener;
impl CompleteMessageListener for TestListener {
    fn recv_message<'a>(&mut self, _message: Message<Reader<Box<Buf + Send + Sync + 'a>>>) -> Result<(), Error> {
        info!("got message");
        Ok(())
    }
    fn on_error(&mut self, error: Error) {
        error!("error: {}", error);
    }
}

fn run() -> Result<(), Error> {
    let client = Client::connect("localhost:26999", MessageCollector::new(TestListener))?;
    client.subscribe("memes")?;
    thread::sleep(Duration::from_secs(1));
    client.unsubscribe("memes")?;
    thread::sleep(Duration::from_secs(3));
    client.stop();

    Ok(())
}
fn main() {
    TermLogger::init(LevelFilter::Debug, simplelog::Config::default()).expect("failed to initialize logger");

    if let Err(e) = run() {
        error!("{}", e);
    }
}
