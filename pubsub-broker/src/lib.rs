#![feature(try_from)]
#![feature(integer_atomics)]

use std::any::Any;
use std::cmp;
use std::time::Duration;
use std::collections::HashMap;
use std::thread::{self, ThreadId};
use std::sync::atomic::{Ordering, AtomicBool, AtomicUsize, AtomicU32};
use std::sync::{Arc, Mutex, RwLock};
use std::io;
use std::net::{SocketAddr, UdpSocket};

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;
extern crate bytes;
extern crate num_cpus;
#[macro_use]
extern crate crossbeam_channel;

extern crate pubsub_common as common;

use bytes::{BufMut, Bytes};
use crossbeam_channel as channel;
use crossbeam_channel::{Sender, Receiver};

use common::constants;
use common::util::{self, BufferProvider};
use common::timer::TimerManager;
use common::packet::PacketType;
use common::protocol::Timeout;

mod client;
mod worker;

use client::Client;
use worker::{Worker, WorkerMessage};

const BUFFER_PREALLOC_SIZE: usize = 16384 * constants::IPV6_MAX_PACKET_SIZE;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        ThreadPanic(err: (ThreadId, Box<dyn Any + Send>)) {
            display("thread {:?} panicked: {:?}", err.0, err.1)
        }
        Io(err: io::Error) {
            from()
            display("io error: {}", err)
            description(err.description())
            cause(err)
        }
        Protocol(err: common::Error) {
            from()
            display("protocol error: {}", err)
            description(err.description())
            cause(err)
        }
        ServerOnly(packet_type: PacketType) {
            description("received packet which can only be sent by servers")
            display("received packet which can only be sent by servers: {}", packet_type)
        }
        KeepaliveDisabled {
            description("keepalive is disabled")
        }
        DisconnectAction
        AlreadySubscribed(topic: String, subscriber: SocketAddr) {
            description("user is already subscribed")
            display("user {} is already subscribed to {}", subscriber, topic)
        }
        NotSubscribed(topic: String, subscriber: SocketAddr) {
            description("user is not subscribed")
            display("user {} is not subscribed to {}", subscriber, topic)
        }
        PublishNotStarted(src: SocketAddr, id: u32) {
            description("publish has not started")
            display("publish has not started for message with id {} from {}", id, src)
        }
        PublishAlreadyStarted(src: SocketAddr, id: u32) {
            description("publish has already started")
            display("publish has already started for message with id {} from {}", id, src)
        }
        MessageTooBig(src: SocketAddr, id: u32, expected: u32, actual: u32) {
            description("message too big")
            display("message with server id {} from {} is too big (expected: {}, actual: {})", id, src, expected, actual)
        }
    }
}

#[derive(Debug)]
struct WorkerManager {
    timers: TimerManager<Timeout>,
    sockets: HashMap<SocketAddr, UdpSocket>,
    buf_source: BufferProvider,

    use_heartbeats: bool,
    next_worker: AtomicUsize,
    workers: Vec<Worker>,

    next_message_id: Arc<AtomicU32>,
    client_assignments: HashMap<SocketAddr, Sender<WorkerMessage>>,
    disconnect_rx: Receiver<WorkerMessage>,
}
impl WorkerManager {
    pub fn new(sockets: Vec<UdpSocket>, buf_source: &BufferProvider, use_heartbeats: bool) -> WorkerManager {
        let worker_count = cmp::max(num_cpus::get() - 1, 1);
        info!("creating {} workers...", worker_count);

        let mut workers = Vec::with_capacity(worker_count);
        let worker_txs = Arc::new(RwLock::new(Vec::with_capacity(worker_count)));
        let (disconnect_tx, disconnect_rx) = channel::unbounded();

        for _ in 0..worker_count {
            let worker = Worker::new(&worker_txs, &disconnect_tx);
            let (worker_tx, _) = worker.tx();
            worker_txs.write().unwrap().push(worker_tx);
            workers.push(worker);
        }

        WorkerManager {
            timers: TimerManager::new(),
            sockets: sockets.into_iter().map(|s| (s.local_addr().unwrap(), s)).collect(),
            buf_source: buf_source.clone(),

            use_heartbeats,
            workers,
            next_worker: AtomicUsize::new(0),

            next_message_id: Arc::new(AtomicU32::new(0)),
            client_assignments: HashMap::new(),
            disconnect_rx,
        }
    }

    fn next_worker(&self) -> &Worker {
        let worker = &self.workers[self.next_worker.fetch_add(1, Ordering::SeqCst)];
        if self.next_worker.load(Ordering::SeqCst) == self.workers.len() {
            self.next_worker.store(0, Ordering::SeqCst);
        }

        worker
    }
    pub fn dispatch(&mut self, local_addr: SocketAddr, client_addr: SocketAddr, data: Bytes) {
        while !self.disconnect_rx.is_empty() {
            match self.disconnect_rx.recv().unwrap() {
                WorkerMessage::Disconnect(client) => { debug!("removing client assignment {}", client); self.client_assignments.remove(&client).unwrap(); },
                _ => panic!("impossible"),
            }
        }

        if !self.client_assignments.contains_key(&client_addr) {
            let tx = {
                let worker = self.next_worker();
                let (tx, timeout_tx) = worker.tx();
                let client = Client::new(&self.timers, client_addr, self.sockets[&local_addr].try_clone().unwrap(), &self.buf_source, &timeout_tx, self.use_heartbeats, &self.next_message_id);
                worker.assign(client);
                tx
            };

            self.client_assignments.insert(client_addr, tx);
        }

        self.client_assignments[&client_addr].send(WorkerMessage::Packet(client_addr, data));
    }
    pub fn stop(self) {
        for worker in self.workers {
            worker.stop();
        }
        self.timers.stop();
    }
}

#[derive(Debug)]
pub struct Broker {
    running: Arc<AtomicBool>,
    clients: Arc<Mutex<WorkerManager>>,
    io_threads: Vec<thread::JoinHandle<()>>,
}
impl Broker {
    pub fn bind<'a, I>(addrs: I, use_heartbeats: bool) -> Result<Broker, Error>
    where I: IntoIterator<Item = &'a SocketAddr>
    {
        let buf_source = BufferProvider::new(BUFFER_PREALLOC_SIZE, constants::IPV6_MAX_PACKET_SIZE);

        let mut sockets = Vec::new();
        for addr in addrs {
            info!("binding on {}...", addr);

            let socket = UdpSocket::bind(addr)?;
            socket.set_read_timeout(Some(Duration::from_millis(500)))?;
            sockets.push(socket);
        }

        let clients = Arc::new(Mutex::new(WorkerManager::new(
                    sockets.iter().map(|s| s.try_clone().unwrap()).collect(),
                    &buf_source,
                    use_heartbeats)));
        let running = Arc::new(AtomicBool::new(true));
        let mut io_threads = Vec::new();
        for socket in sockets {
            let addr = socket.local_addr().unwrap();

            let running = Arc::clone(&running);
            let mut buf_source = buf_source.clone();
            let clients = Arc::clone(&clients);
            io_threads.push(thread::spawn(move || {
                while running.load(Ordering::SeqCst) {
                    let mut packet_buffer = buf_source.allocate(util::max_packet_size(addr));

                    let (size, src) = unsafe {
                        let (size, src) = match socket.recv_from(packet_buffer.bytes_mut()) {
                            Ok((size, src)) => (size, src),
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                            Err(e) => {
                                error!("error while receiving udp packet: {}", e);
                                continue;
                            }
                        };
                        packet_buffer.advance_mut(size);
                        (size, src)
                    };

                    packet_buffer.truncate(size);
                    clients.lock().unwrap().dispatch(addr, src, packet_buffer.freeze());
                }
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
    pub fn stop(self) {
        self.running.store(false, Ordering::SeqCst);
        for thread in self.io_threads {
            thread.join().unwrap();
        }
        Arc::try_unwrap(self.clients).unwrap().into_inner().unwrap().stop();
    }
}

