use std::collections::HashMap;
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::net::SocketAddr;

use crossbeam::queue::MsQueue;
use crossbeam::channel::{self, Sender};

use ::common::{self, Data, Packet};
use ::Error;
use ::client::Client;

pub enum WorkerMessage {
    Packet(SocketAddr, Data),
    Disconnect(SocketAddr),
    Shutdown,
}

#[derive(Debug)]
pub struct Worker {
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
                                let mut client = r_clients[&src].lock().unwrap();
                                let result = client.gbn().decode(src, &data);
                                match result {
                                    Ok(packet) => match packet {
                                        Packet::Disconnect => {
                                            Ok(true)
                                        },
                                        _ => client.handle(packet).map(|_| false),
                                    },
                                    Err(e@common::DecodeError::OutOfOrder(_, _)) => {
                                        warn!("{}", e);
                                        Ok(false)
                                    },
                                    Err(e) => Err(e.into())
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
