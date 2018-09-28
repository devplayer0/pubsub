use std::error::Error as StdError;
use std::any::Any;
use std::ops::Deref;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use std::collections::HashMap;
use std::thread::{self, JoinHandle};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Condvar};
use std::io;
use std::net::{SocketAddr, UdpSocket};

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;
extern crate ctrlc;
extern crate num_cpus;
extern crate crossbeam;

extern crate pubsub_common as common;

use crossbeam::channel::{self, Sender};
use crossbeam::queue::MsQueue;

pub mod config;

use config::Config;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        ThreadPanic(err: Box<dyn Any + Send>) {
            display("thread panicked: {:?}", err)
        }
        Io(err: io::Error) {
            from()
            display("io error: {}", err)
            description(err.description())
            cause(err)
        }
    }
}

struct Data {
    buffer: Vec<u8>,
    size: usize,
}
impl Data {
    pub fn from_buffer(buffer: Vec<u8>, size: usize) -> Data {
        Data {
            buffer,
            size,
        }
    }
    pub fn take(self) -> Vec<u8> {
        self.buffer
    }
}
impl Deref for Data {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buffer[..self.size]
    }
}
enum WorkerMessage {
    Packet(Data),
    Shutdown,
}

#[derive(Debug)]
struct Client {
    addr: SocketAddr,
    tx: Sender<WorkerMessage>,
}
impl Hash for Client {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}
impl PartialEq for Client {
    fn eq(&self, other: &Client) -> bool {
        other.addr == self.addr
    }
}
impl Client {
    pub fn new(addr: SocketAddr, tx: Sender<WorkerMessage>) -> Client {
        Client {
            addr,
            tx
        }
    }
    pub fn dispatch(&self, data: Data) {
        self.tx.send(WorkerMessage::Packet(data));
    }
}
impl Eq for Client {}

#[derive(Debug)]
struct Worker {
    tx: Sender<WorkerMessage>,
    thread: JoinHandle<()>,
}
impl Worker {
    pub fn new(buffers: Arc<MsQueue<Vec<u8>>>) -> Worker {
        let (tx, rx) = channel::unbounded();
        let thread = thread::spawn(move || {
            use WorkerMessage::*;
            for msg in rx {
                match msg {
                    Packet(data) => {
                        debug!("worker from thread {:?} got a packet of {} bytes!", thread::current().id(), data.len());
                        buffers.push(data.take());
                    },
                    Shutdown => break
                }
            }
        });

        Worker {
            tx,
            thread,
        }
    }
    pub fn tx(&self) -> Sender<WorkerMessage> {
        self.tx.clone()
    }
    pub fn stop(self) -> Result<(), Error> {
        self.tx.send(WorkerMessage::Shutdown);
        self.thread.join().map_err(|e| Error::ThreadPanic(Box::new(e)))
    }
}

#[derive(Debug)]
struct ClientManager {
    clients: HashMap<SocketAddr, Client>,
    next_worker: usize,
    workers: Vec<Worker>,
}
impl ClientManager {
    pub fn new(buffers: Arc<MsQueue<Vec<u8>>>) -> ClientManager {
        let mut workers = Vec::with_capacity(num_cpus::get());
        info!("creating {} workers...", workers.capacity());
        for _ in 0..workers.capacity() {
            workers.push(Worker::new(Arc::clone(&buffers)));
        }

        ClientManager {
            clients: HashMap::new(),
            workers,
            next_worker: 0,
        }
    }
    fn next_worker(&mut self) -> &Worker {
        let worker = &self.workers[self.next_worker];
        self.next_worker += 1;
        if self.next_worker == self.workers.len() {
            self.next_worker = 0;
        }

        worker
    }
    pub fn dispatch(&mut self, client_addr: SocketAddr, data: Data) {
        if !self.clients.contains_key(&client_addr) {
            let client = Client::new(client_addr, self.next_worker().tx());
            self.clients.insert(client_addr, client);
        }

        self.clients[&client_addr].dispatch(data)
    }
    pub fn stop(self) -> Result<(), Error> {
        for worker in self.workers {
            worker.stop()?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Broker {
    running: Arc<AtomicBool>,
    clients: Arc<Mutex<ClientManager>>,
    io_threads: Vec<thread::JoinHandle<Result<(), Error>>>,
}
impl Broker {
    pub fn bind<I>(addrs: I) -> Result<Broker, Error>
    where I: IntoIterator<Item = SocketAddr>
    {
        let buffers = Arc::new(MsQueue::new());
        for _ in 0..1024 {
            buffers.push(vec![0; common::IPV6_MAX_PACKET_SIZE]);
        }

        let clients = Arc::new(Mutex::new(ClientManager::new(Arc::clone(&buffers))));
        let running = Arc::new(AtomicBool::new(true));
        let addrs = addrs.into_iter();
        let (l, _) = addrs.size_hint();
        let mut io_threads = Vec::with_capacity(l);
        for addr in addrs {
            info!("binding on {}...", addr);

            let socket = UdpSocket::bind(addr)?;
            socket.set_read_timeout(Some(Duration::from_millis(250)))?;

            let running = Arc::clone(&running);
            let buffers = Arc::clone(&buffers);
            let clients = Arc::clone(&clients);
            io_threads.push(thread::spawn(move || {
                while running.load(Ordering::SeqCst) {
                    let mut buffer = buffers.pop();
                    let (size, src) = match socket.recv_from(&mut buffer) {
                        Ok((size, src)) => (size, src),
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            buffers.push(buffer);
                            continue;
                        },
                        Err(e) => return Err(Error::Io(e))
                    };
                    if size > match addr {
                        SocketAddr::V4(_) => common::IPV4_MAX_PACKET_SIZE,
                        SocketAddr::V6(_) => common::IPV6_MAX_PACKET_SIZE
                    } {
                        buffers.push(buffer);
                        continue;
                    }

                    clients.lock().unwrap().dispatch(src, Data::from_buffer(buffer, size));
                }

                Ok(())
            }));
        }

        Ok(Broker {
            clients,
            running,
            io_threads,
        })
    }

    pub fn running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    pub fn stop(self) -> Result<(), Error> {
        self.running.store(false, Ordering::SeqCst);
        for thread in self.io_threads {
            thread.join().map_err(|e| Error::ThreadPanic(Box::new(e)))??;
        }
        Arc::try_unwrap(self.clients).unwrap().into_inner().unwrap().stop()?;

        Ok(())
    }
}

pub fn run(config: Config) -> Result<(), Box<dyn StdError>> {
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

    let broker = Broker::bind(config.bind_addrs().clone())?;

    let &(ref stop_lock, ref stop_cond) = &*stop;
    let mut stop = stop_lock.lock().unwrap();
    while !*stop {
        stop = stop_cond.wait(stop).unwrap();
    }

    broker.stop().map_err(|e| Box::new(e) as Box<dyn StdError>)
}
