#![feature(duration_float)]

use std::time::Instant;
use std::fs::File;

use log::{info, error};
use simplelog::{LevelFilter, TermLogger};

use pubsub_client::bytes::{Reader, Buf};
use pubsub_client::{Message, CompleteMessageListener, MessageCollector, Client};

// 512MB
const DUMMY_SIZE: u32 = 512 * 1024 * 1024;

struct DummyListener;
impl CompleteMessageListener for DummyListener {
    fn recv_message<'a>(&mut self, _msg: Message<Reader<Box<Buf + Send + Sync + 'a>>>) -> Result<(), pubsub_client::Error> {
        unimplemented!();
    }
    fn on_error(&mut self, err: pubsub_client::Error) {
        error!("{}", err);
    }
}

fn dummy_message() -> Result<Message<File>, pubsub_client::Error> {
    Ok(Message::new(DUMMY_SIZE, "/dev/zero", File::open("/dev/zero")?))
}
fn run() -> Result<(), pubsub_client::Error> {
    let mut client = Client::connect("localhost:26999", MessageCollector::new(DummyListener))?;

    // send zeroes
    let msg = dummy_message()?;
    client.queue_message(msg)?;

    let start = Instant::now();
    client.drain_messages()?;
    info!("took {:.3?} to send {} byte message (~{:.2}MB/s)", start.elapsed(), DUMMY_SIZE, (DUMMY_SIZE as f64 / 1024. / 1024.) / start.elapsed().as_float_secs());

    // send 2 lots of zeroes in parallel
    let msg_a = dummy_message()?;
    let msg_b = dummy_message()?;
    client.queue_message(msg_a)?;
    client.queue_message(msg_b)?;

    let start = Instant::now();
    client.drain_messages()?;
    info!("took {:.3?} to send 2 {} byte messages (~{:.2}MB/s)", start.elapsed(), DUMMY_SIZE, (DUMMY_SIZE as f64 / 1024. / 1024.) / start.elapsed().as_float_secs());

    client.stop();
    Ok(())
}
fn main() {
    TermLogger::init(LevelFilter::Debug, simplelog::Config::default()).unwrap();

    if let Err(e) = run() {
        error!("{}", e);
    }
}
