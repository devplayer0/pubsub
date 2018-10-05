#![feature(try_from)]

use std::error::Error as StdError;
use std::any::Any;
use std::convert::TryFrom;
use std::cmp;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use std::collections::{HashMap, LinkedList};
use std::thread::{self, ThreadId, JoinHandle};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Condvar};
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

use crossbeam::channel::{self, Sender, Receiver};
use crossbeam::queue::MsQueue;

use common::{Data, Packet};

pub mod config;

use config::Config;

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
        Decode(err: common::DecodeError) {
            from()
            display("packet decode error: {}", err)
            description(err.description())
            cause(err)
        }
        InvalidPacketType
    }
}

enum WorkerMessage {
    Packet(SocketAddr, Data),
    Disconnect(SocketAddr),
    Shutdown,
}

#[derive(Debug)]
struct Client {
    addr: SocketAddr,
    socket: UdpSocket,
    buffers: Arc<MsQueue<Vec<u8>>>,

    pub connected: bool,
    send_buffer: LinkedList<Data>,
    send_buffer_sseq: u8,
    send_seq: u8,

    recv_next: u8,
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
    pub fn new(addr: SocketAddr, socket: UdpSocket, buffers: Arc<MsQueue<Vec<u8>>>) -> Client {
        Client {
            addr,
            socket,
            buffers,

            connected: false,
            send_buffer: LinkedList::new(),
            send_buffer_sseq: 0,
            send_seq: 0,

            recv_next: 0,
        }
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn send_heartbeat(&self) -> Result<(), Error> {
        let heartbeat = Packet::make_heartbeat(self.buffers.pop());
        self.socket.send_to(&heartbeat, self.addr)?;
        self.buffers.push(heartbeat.take());
        Ok(())
    }
    pub fn send_disconnect(&self) -> Result<(), Error> {
        let disconnect = Packet::make_disconnect(self.buffers.pop());
        self.socket.send_to(&disconnect, self.addr)?;
        self.buffers.push(disconnect.take());
        Ok(())
    }
    pub fn handle_ack(&mut self, seq: u8) {
        if !common::seq_in_window(self.send_buffer_sseq, seq) {
            warn!("received ack from {} outside of window", self.addr);
            return;
        }

        let buffer_size = self.send_buffer.len();
        for buffered in self.send_buffer.split_off(cmp::min(common::slide_amount(self.send_buffer_sseq, seq) as usize, buffer_size)) {
            self.buffers.push(buffered.take());
        }
        self.send_buffer_sseq = common::next_seq(seq);
        self.send_seq = self.send_buffer_sseq;
    }
    pub fn handle(&mut self, packet: Packet) -> Result<(), Error> {
        match packet {
            Packet::Ack(seq) => self.handle_ack(seq),
            Packet::Heartbeat => {
                // TODO: timeout
                debug!("got heartbeat from {}", self.addr);
            },
            _ => return Err(Error::InvalidPacketType)
        }

        Ok(())
    }
}
impl Eq for Client {}

#[derive(Debug)]
struct Worker {
    clients: Arc<RwLock<HashMap<SocketAddr, Mutex<Client>>>>,
    tx: Sender<WorkerMessage>,
    thread: JoinHandle<()>,
}
impl Worker {
    pub fn new(buffers: Arc<MsQueue<Vec<u8>>>, disconnect_tx: Sender<WorkerMessage>) -> Worker {
        let clients: Arc<RwLock<HashMap<_, Mutex<Client>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = channel::unbounded();
        let thread = {
            let clients = Arc::clone(&clients);
            thread::spawn(move || {
                for msg in rx {
                    match msg {
                        WorkerMessage::Packet(src, data) => {
                            let r_clients = clients.read().unwrap();

                            trace!("worker from thread {:?} got a packet of {} bytes from {}!", thread::current().id(), data.len(), src);
                            if !r_clients[&src].lock().unwrap().connected {
                                if let Err(_) = Packet::validate_connect(&data) {
                                    warn!("received invalid connection packet from {}", src);
                                    drop(r_clients);
                                    clients.write().unwrap().remove(&src);
                                    disconnect_tx.send(WorkerMessage::Disconnect(src));
                                } else {
                                    let ret = {
                                        let mut client = r_clients[&src].lock().unwrap();
                                        client.connected = true;
                                        client.send_heartbeat()
                                    };
                                    if let Err(e) = ret {
                                        error!("failed to send connack to {}: {}", src, e);
                                        drop(r_clients);
                                        clients.write().unwrap().remove(&src);
                                        disconnect_tx.send(WorkerMessage::Disconnect(src));
                                    } else {
                                        debug!("got connection from {}", src);
                                    }
                                }

                                buffers.push(data.take());
                                continue;
                            }

                            let result = {
                                let result = Packet::try_from(&data);
                                if let Ok(packet) = result {
                                    match packet {
                                        Packet::Disconnect => {
                                            Ok(true)
                                        },
                                        _ => r_clients[&src].lock().unwrap().handle(packet).map(|_| false),
                                    }
                                } else {
                                    result.map(|_| false).map_err(|e| e.into())
                                }
                            };
                            match result {
                                Ok(true) => {
                                    debug!("disconnecting client {}", src);
                                    if let Err(e) = r_clients[&src].lock().unwrap().send_disconnect() {
                                        warn!("error while sending disconnect packet to {}: {}", src, e);
                                    }

                                    drop(r_clients);
                                    clients.write().unwrap().remove(&src);
                                    disconnect_tx.send(WorkerMessage::Disconnect(src));
                                },
                                Err(e) => match e {
                                    Error::InvalidPacketType | Error::Decode(_) => {
                                        error!("received invalid/malformed packet from {}: {}, disconnecting...", src, e);
                                        drop(r_clients);
                                        clients.write().unwrap().remove(&src);
                                        disconnect_tx.send(WorkerMessage::Disconnect(src));
                                    },
                                    _ => error!("error while processing packet from {}: {}", src, e)
                                },

                                Ok(false) => {},
                            }
                            buffers.push(data.take());
                        },
                        WorkerMessage::Shutdown => break,
                        _ => panic!("impossible"),
                    }
                }
            })
        };

        Worker {
            clients,
            tx,
            thread,
        }
    }
    pub fn tx(&self) -> Sender<WorkerMessage> {
        self.tx.clone()
    }
    pub fn stop(self) -> Result<(), Error> {
        self.tx.send(WorkerMessage::Shutdown);
        let tid = self.thread.thread().id();
        self.thread.join().map_err(|e| Error::ThreadPanic((tid, Box::new(e))))
    }
    pub fn assign(&self, client: Client) {
        self.clients.write().unwrap().insert(client.addr(), Mutex::new(client));
    }
}

#[derive(Debug)]
struct WorkerManager {
    sockets: HashMap<SocketAddr, UdpSocket>,
    buffers: Arc<MsQueue<Vec<u8>>>,
    next_worker: usize,
    workers: Vec<Worker>,
    client_assignments: HashMap<SocketAddr, Sender<WorkerMessage>>,
    disconnect_rx: Receiver<WorkerMessage>,
}
impl WorkerManager {
    pub fn new(sockets: Vec<UdpSocket>, buffers: Arc<MsQueue<Vec<u8>>>) -> WorkerManager {
        let mut workers = Vec::with_capacity(num_cpus::get());
        let (disconnect_tx, disconnect_rx) = channel::unbounded();
        info!("creating {} workers...", workers.capacity());
        for _ in 0..workers.capacity() {
            workers.push(Worker::new(Arc::clone(&buffers), disconnect_tx.clone()));
        }

        WorkerManager {
            sockets: sockets.into_iter().map(|s| (s.local_addr().unwrap(), s)).collect(),
            buffers,
            workers,
            next_worker: 0,
            client_assignments: HashMap::new(),
            disconnect_rx,
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
    pub fn dispatch(&mut self, local_addr: SocketAddr, client_addr: SocketAddr, data: Data) {
        while !self.disconnect_rx.is_empty() {
            match self.disconnect_rx.recv().unwrap() {
                WorkerMessage::Disconnect(client) => { self.client_assignments.remove(&client).unwrap(); },
                _ => panic!("impossible"),
            }
        }

        if !self.client_assignments.contains_key(&client_addr) {
            let client = Client::new(client_addr, self.sockets[&local_addr].try_clone().unwrap(), Arc::clone(&self.buffers));
            let tx = {
                let worker = self.next_worker();
                worker.assign(client);
                worker.tx()
            };
            self.client_assignments.insert(client_addr, tx);
        }

        self.client_assignments[&client_addr].send(WorkerMessage::Packet(client_addr, data));
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
    clients: Arc<Mutex<WorkerManager>>,
    io_threads: Vec<thread::JoinHandle<Result<(), Error>>>,
}
impl Broker {
    pub fn bind<'a, I>(addrs: I) -> Result<Broker, Error>
    where I: IntoIterator<Item = &'a SocketAddr>
    {
        let buffers = Arc::new(MsQueue::new());
        for _ in 0..1024 {
            buffers.push(vec![0; common::IPV6_MAX_PACKET_SIZE]);
        }

        let mut sockets = Vec::new();
        for addr in addrs {
            info!("binding on {}...", addr);

            let socket = UdpSocket::bind(addr)?;
            socket.set_read_timeout(Some(Duration::from_millis(250)))?;
            sockets.push(socket);
        }

        let clients = Arc::new(Mutex::new(WorkerManager::new(
                    sockets.iter().map(|s| s.try_clone().unwrap()).collect(),
                    Arc::clone(&buffers))));
        let running = Arc::new(AtomicBool::new(true));
        let mut io_threads = Vec::new();
        for socket in sockets {
            let addr = socket.local_addr().unwrap();

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

                    clients.lock().unwrap().dispatch(addr, src, Data::from_buffer(buffer, size));
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
            let tid = thread.thread().id();
            thread.join().map_err(|e| Error::ThreadPanic((tid, Box::new(e))))??;
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

    let broker = Broker::bind(config.bind_addrs())?;

    let &(ref stop_lock, ref stop_cond) = &*stop;
    let mut stop = stop_lock.lock().unwrap();
    while !*stop {
        stop = stop_cond.wait(stop).unwrap();
    }

    broker.stop().map_err(|e| Box::new(e) as Box<dyn StdError>)
}
