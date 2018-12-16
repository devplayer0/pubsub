#![feature(wait_until)]

use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;
use std::thread;
use std::io::Cursor;

use log::{trace, info, debug, error};
use simplelog::{LevelFilter, TermLogger};
use rand::prelude::*;

use pubsub_client::bytes::{Reader, IntoBuf, Buf, Bytes};
use pubsub_client::{Error, TimerManager, Message, CompleteMessageListener, MessageCollector, Client};

const MESSAGE_CONTENT: &'static [u8] = include_bytes!("../lipsum.txt");
const TOPICS: &'static [&'static str] = &[ "a", "b", "c", "d" ];
const MESSAGE_COUNT: usize = 100;
const MIN_INTERVAL: Duration = Duration::from_millis(200);
const MAX_INTERVAL: Duration = Duration::from_millis(300);
const PUBLISHER_COUNT: usize = 10;
const SUBSCRIBER_COUNT: usize = 100;

struct DummyListener;
impl CompleteMessageListener for DummyListener {
    fn recv_message<'a>(&mut self, _msg: Message<Reader<Box<Buf + Send + Sync + 'a>>>) -> Result<(), Error> {
        unimplemented!();
    }
    fn on_error(&mut self, err: pubsub_client::Error) {
        error!("{}", err);
    }
}

struct TracingListener(Arc<(Mutex<usize>, Condvar)>);
impl TracingListener {
    fn new(received: &Arc<(Mutex<usize>, Condvar)>) -> TracingListener {
        TracingListener(Arc::clone(received))
    }
}
impl CompleteMessageListener for TracingListener {
    fn recv_message<'a>(&mut self, msg: Message<Reader<Box<Buf + Send + Sync + 'a>>>) -> Result<(), Error> {
        trace!("received message on topic {}, size {}", msg.topic(), msg.size());
        let &(ref lock, ref cvar) = &*self.0;
        let mut received = lock.lock().unwrap();

        *received += 1;
        cvar.notify_one();

        Ok(())
    }
    fn on_error(&mut self, err: pubsub_client::Error) {
        error!("{}", err);
    }
}

fn dummy_message(topic: &str) -> Message<Reader<Cursor<Bytes>>> {
    Message::new(MESSAGE_CONTENT.len() as u32, topic, Bytes::from(MESSAGE_CONTENT).into_buf().reader())
}
fn run() -> Result<(), pubsub_client::Error> {
    let timers = TimerManager::new();

    let received = Arc::new((Mutex::new(0), Condvar::new()));
    let mut subscribers = Vec::with_capacity(TOPICS.len() * SUBSCRIBER_COUNT);
    for topic in TOPICS {
        for _ in 0..SUBSCRIBER_COUNT {
            let mut client = Client::connect_timers(&timers, "localhost:26999", MessageCollector::new(TracingListener::new(&received)))?;
            client.subscribe(topic)?;

            subscribers.push(client);
        }
    }

    let mut publishers = Vec::with_capacity(TOPICS.len() * PUBLISHER_COUNT);
    for topic in TOPICS {
        for i in 0..PUBLISHER_COUNT {
            let mut client = Client::connect_timers(&timers, "localhost:26999", MessageCollector::new(DummyListener))?;
            let thread = thread::spawn(move || {
                let mut rng = thread_rng();
                for _ in 0..MESSAGE_COUNT {
                    let msg = dummy_message(topic);
                    client.queue_message(msg).expect("failed to queue message");
                    client.drain_messages().expect("failed to send message");

                    let interval = rng.gen_range(MIN_INTERVAL, MAX_INTERVAL);
                    thread::sleep(interval);
                }

                client.stop();
                debug!("publisher {} for topic {} sent {} messages", i, topic, MESSAGE_COUNT);
            });

            publishers.push(thread);
        }
    }

    let total_messages = TOPICS.len() * SUBSCRIBER_COUNT * PUBLISHER_COUNT * MESSAGE_COUNT;
    {
        let &(ref lock, ref cvar) = &*received;
        let _guard = cvar.wait_until(lock.lock().unwrap(), |c| *c == total_messages).unwrap();
    }

    // needed purely because each client is on a poll with a timeout
    let stop_threads: Vec<_> = subscribers
        .into_iter()
        .map(|s| thread::spawn(move || s.stop()))
        .collect();
    for thread in stop_threads {
        thread.join().unwrap();
    }

    for thread in publishers {
        thread.join().unwrap();
    }

    info!("received {} messages across {} subscribers", total_messages, TOPICS.len() * SUBSCRIBER_COUNT);

    Ok(())
}
fn main() {
    TermLogger::init(LevelFilter::Debug, simplelog::Config::default()).unwrap();

    if let Err(e) = run() {
        error!("{}", e);
    }
}
