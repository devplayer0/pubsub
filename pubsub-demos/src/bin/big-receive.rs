#![feature(wait_until)]
#![feature(duration_float)]

use std::sync::{Arc, Mutex, Condvar};
use std::time::Instant;

use log::{debug, info, error};
use simplelog::{LevelFilter, TermLogger};

use pubsub_client::{Error, MessageListener, Client};
use pubsub_client::messaging::{MessageStart, MessageSegment};

struct DummyListener {
    start: Option<(MessageStart, Instant)>,
    received: u32,

    completed: Arc<(Mutex<bool>, Condvar)>,
}
impl DummyListener {
    fn new(completed: &Arc<(Mutex<bool>, Condvar)>) -> DummyListener {
        DummyListener {
            completed: Arc::clone(completed),

            start: None,
            received: 0,
        }
    }
}
impl MessageListener for DummyListener {
    fn recv_start(&mut self, start: MessageStart) -> Result<(), Error> {
        match self.start {
            Some((ref s, _)) if start.id() == s.id() => return Err(Error::Custom(format!("received start for already started message {}", s.id()))),
            Some(_) => debug!("ignoring new message with id {}", start.id()),
            None => {
                info!("receiving {} byte message (id {})", start.size(), start.id());
                self.start = Some((start, Instant::now()))
            },
        }

        Ok(())
    }
    fn recv_data(&mut self, data: MessageSegment) -> Result<(), Error> {
        match self.start.as_ref() {
            Some((start, started)) => {
                if data.id() != start.id() {
                    return Ok(());
                }

                self.received += data.size();
                if self.received == start.size() {
                    let &(ref lock, ref cvar) = &*self.completed;
                    let mut completed = lock.lock().unwrap();

                    info!("took {:.3?} to receive {} byte message (~{:.2}MB/s)", started.elapsed(), start.size(), (start.size() as f64 / 1024. / 1024.) / started.elapsed().as_float_secs());
                    *completed = true;
                    cvar.notify_one();
                }
                Ok(())
            },
            None => Err(Error::CustomStatic("haven't started a message!")),
        }
    }
    fn on_error(&mut self, err: pubsub_client::Error) {
        error!("{}", err);
    }
}

fn run() -> Result<(), pubsub_client::Error> {
    let completed = Arc::new((Mutex::new(false), Condvar::new()));
    let mut client = Client::connect("localhost:26999", DummyListener::new(&completed))?;

    client.subscribe("/dev/zero")?;
    info!("connected, waiting for message...");
    {
        let &(ref lock, ref cvar) = &*completed;
        let _guard = cvar.wait_until(lock.lock().unwrap(), |c| *c).unwrap();
    }

    client.stop();
    Ok(())
}
fn main() {
    TermLogger::init(LevelFilter::Debug, simplelog::Config::default()).unwrap();

    if let Err(e) = run() {
        error!("{}", e);
    }
}
