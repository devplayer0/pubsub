use std::any::Any;
use std::time::Duration;
use std::collections::HashMap;
use std::thread::{self, ThreadId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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

use common::Data;

pub mod config;
mod client;
mod worker;

use client::Client;
use worker::{Worker, WorkerMessage};

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

