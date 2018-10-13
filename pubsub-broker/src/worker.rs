use std::collections::HashMap;
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::net::SocketAddr;

use bytes::Bytes;
use crossbeam_channel as channel;
use crossbeam_channel::Sender;

use common;
use common::timer::TimerManager;
use common::packet::Packet;
use common::jqtt::Timeout;
use ::Error;
use client::{Client};

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum WorkerMessage {
    Packet(SocketAddr, Bytes),
    Disconnect(SocketAddr),
    Shutdown,
}

#[derive(Debug)]
pub struct Worker {
    timers: TimerManager<Timeout>,

    clients: Arc<RwLock<HashMap<SocketAddr, Mutex<Client>>>>,
    tx: Sender<WorkerMessage>,
    timeout_tx: Sender<Timeout>,

    thread: JoinHandle<()>,
}
impl Worker {
    pub fn new(timers: &TimerManager<Timeout>, disconnect_tx: Sender<WorkerMessage>) -> Worker {
        let clients: Arc<RwLock<HashMap<_, Mutex<Client>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = channel::unbounded();
        let (timeout_tx, timeout_rx) = channel::unbounded();
        let thread = {
            let clients = Arc::clone(&clients);
            thread::spawn(move || {
                let mut running = true;
                while running {
                    select! {
                        recv(timeout_rx, msg) => {
                            if let Some(msg) = msg {
                                match msg {
                                    Timeout::Heartbeat(src) => {
                                        error!("client {} timed out, disconnecting...", src);
                                        let mut clients = clients.write().unwrap();
                                        {
                                            let mut client = clients[&src].lock().unwrap();
                                            if let Err(e) = client.send_disconnect() {
                                                warn!("error while sending disconnect packet to {}: {}", src, e);
                                            }
                                        }

                                        clients.remove(&src);
                                        disconnect_tx.send(WorkerMessage::Disconnect(src));
                                    },
                                    Timeout::Ack(src) => {
                                        let clients = clients.read().unwrap();
                                        let mut client = clients[&src].lock().unwrap();
                                        warn!("client {} ack timed out, re-sending packets in window", src);
                                        if let Err(e) = client.handle_ack_timeout() {
                                            error!("error re-sending packets in window to {}: {}", src, e);
                                        }
                                    },
                                }
                            }
                        },
                        recv(rx, msg) => {
                            if let Some(msg) = msg {
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

                                            continue;
                                        }

                                        let result = {
                                            let mut client = r_clients[&src].lock().unwrap();
                                            let result = client.decode(&data);
                                            match result {
                                                Ok(packet) => match packet {
                                                    Packet::Disconnect => Ok(true),
                                                    _ => client.handle(packet).map(|_| false),
                                                },
                                                Err(e@common::Error::OutOfOrder(_, _)) => {
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
                                            Ok(false) => if let Err(e) = r_clients[&src].lock().unwrap().drain_message() {
                                                error!("error while encoding / sending message: {}", e);   
                                            },
                                        }
                                    },
                                    WorkerMessage::Shutdown => running = false,
                                    _ => panic!("impossible"),
                                }
                            } else {
                                running = false;
                            }
                        }
                    }
                }
            })
        };

        Worker {
            timers: timers.clone(),

            clients,
            tx,
            timeout_tx,

            thread,
        }
    }
    pub fn tx(&self) -> (Sender<WorkerMessage>, Sender<Timeout>) {
        (self.tx.clone(), self.timeout_tx.clone())
    }
    pub fn stop(self) {
        self.tx.send(WorkerMessage::Shutdown);
        self.thread.join().unwrap();
    }
    pub fn assign(&self, client: Client) {
        self.clients.write().unwrap().insert(client.addr(), Mutex::new(client));
    }
}
