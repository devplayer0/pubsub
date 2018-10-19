#![feature(box_syntax)]

use std::time::Duration;
use std::thread;
use std::env;
use std::io::Read;

extern crate bytes;
#[macro_use]
extern crate log;
extern crate simplelog;
extern crate pubsub_client;

use bytes::{Buf, Reader};
use simplelog::{LevelFilter, TermLogger};
use pubsub_client::{Error, Client, Message, MessageCollector, CompleteMessageListener};

const LIPSUM: &'static str = include_str!("lipsum.txt");

struct TestListener;
impl CompleteMessageListener for TestListener {
    fn recv_message<'a>(&mut self, mut msg: Message<Reader<Box<Buf + Send + Sync + 'a>>>) -> Result<(), Error> {
        let mut text = String::with_capacity(msg.size() as usize);
        msg.read_to_string(&mut text).unwrap();
        assert!(text == LIPSUM);
        info!("got message");
        Ok(())
    }
    fn on_error(&mut self, error: Error) {
        error!("error: {}", error);
    }
}

fn run() -> Result<(), Error> {
    let wait = {
        let args: Vec<_> = env::args().collect();
        if args.len() != 2 {
            return Err(Error::Custom(format!("usage: {} <wait | publish>", args[0])));
        }

        match &*args[1] {
            "wait" => true,
            "publish" => false,
            _ => return Err(Error::Custom(format!("usage: {} <wait | publish>", args[0]))),
        }
    };

    let mut client = Client::connect("localhost:26999", MessageCollector::new(TestListener))?;
    if wait {
        client.subscribe("memes")?;
        thread::sleep(Duration::from_secs(30));
    } else {
        for _ in 0..3 {
            let data = LIPSUM.as_bytes();
            let msg = Message::new(data.len() as u32, "memes", data);
            client.queue_message(msg)?;
        }

        client.drain_messages()?;
    }

    client.stop();

    Ok(())
}
fn main() {
    TermLogger::init(LevelFilter::Debug, simplelog::Config::default()).expect("failed to initialize logger");

    if let Err(e) = run() {
        error!("{}", e);
    }
}
