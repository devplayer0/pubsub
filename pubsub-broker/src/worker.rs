use std::collections::{hash_set, HashMap, HashSet};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::net::SocketAddr;

use bytes::Bytes;
use crossbeam_channel as channel;
use crossbeam_channel::Sender;

use ::Error;
use common::protocol::{Timeout, MessageStart, MessageSegment};
use client::{Action, Client};

struct Subscriptions {
    inner: HashMap<String, HashSet<SocketAddr>>,
}
impl Subscriptions {
    pub fn new() -> Subscriptions {
        Subscriptions {
            inner: HashMap::new(),
        }
    }

    pub fn get_subscribers(&self, topic: &str) -> Option<hash_set::Iter<SocketAddr>> {
        if !self.inner.contains_key(topic) {
            return None;
        }

        Some(self.inner[topic].iter())
    }
    pub fn subscribe(&mut self, topic: &str, subscriber: SocketAddr) -> Result<(), Error> {
        if !self.inner.contains_key(topic) {
            self.inner.insert(topic.to_owned(), HashSet::new());
        }

        match self.inner.get_mut(topic).unwrap().insert(subscriber) {
            true => Ok(()),
            false => Err(Error::AlreadySubscribed(topic.to_owned(), subscriber)),
        }
    }
    pub fn unsubscribe(&mut self, topic: &str, subscriber: SocketAddr) -> Result<(), Error> {
        if !self.inner.contains_key(topic) {
            return Err(Error::NotSubscribed(topic.to_owned(), subscriber));
        }

        if {
            let set = self.inner.get_mut(topic).unwrap();
            if !set.remove(&subscriber) {
                return Err(Error::NotSubscribed(topic.to_owned(), subscriber));
            }
            set.is_empty()
        } {
            self.inner.remove(topic);
        };
        Ok(())
    }
    pub fn unsubscribe_all(&mut self, subscriber: SocketAddr) {
        for (_, subscribers) in self.inner.iter_mut() {
            subscribers.remove(&subscriber);
        }

        self.inner.retain(|_, set| !set.is_empty());
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum WorkerMessage {
    Packet(SocketAddr, Bytes),
    Disconnect(SocketAddr),
    DispatchStart(SocketAddr, MessageStart),
    DispatchSegment(SocketAddr, MessageSegment),

    Shutdown,
}

#[derive(Debug)]
pub struct Worker {
    clients: Arc<RwLock<HashMap<SocketAddr, Mutex<Client>>>>,
    tx: Sender<WorkerMessage>,
    timeout_tx: Sender<Timeout>,

    thread: JoinHandle<()>,
}
impl Worker {
    pub fn new(worker_txs: &Arc<RwLock<Vec<Sender<WorkerMessage>>>>, manager_tx: &Sender<WorkerMessage>) -> Worker {
        let clients: Arc<RwLock<HashMap<_, Mutex<Client>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = channel::unbounded();
        let (timeout_tx, timeout_rx) = channel::unbounded();
        let thread = {
            let clients = Arc::clone(&clients);
            let worker_txs = Arc::clone(worker_txs);
            let manager_tx = manager_tx.clone();
            thread::spawn(move || {
                let mut subscriptions = Subscriptions::new();

                let mut running = true;
                while running {
                    select! {
                        recv(timeout_rx, msg) => {
                            if let Some(msg) = msg {
                                match msg {
                                    Timeout::Keepalive(src) => {
                                        error!("client {} timed out, disconnecting...", src);
                                        let mut clients = clients.write().unwrap();
                                        {
                                            let mut client = clients[&src].lock().unwrap();
                                            if let Err(e) = client.send_disconnect() {
                                                warn!("error while sending disconnect packet to {}: {}", src, e);
                                            }
                                        }

                                        subscriptions.unsubscribe_all(src);
                                        clients.remove(&src);
                                        manager_tx.send(WorkerMessage::Disconnect(src));
                                    },
                                    Timeout::Ack(src) => {
                                        let clients = clients.read().unwrap();
                                        let mut client = clients[&src].lock().unwrap();
                                        warn!("client {} ack timed out, re-sending packets in window", src);
                                        if let Err(e) = client.handle_ack_timeout() {
                                            error!("error re-sending packets in window to {}: {}", src, e);
                                        }
                                    },
                                    Timeout::SendHeartbeat(_) => panic!("impossible"),
                                }
                            }
                        },
                        recv(rx, msg) => {
                            if let Some(msg) = msg {
                                match msg {
                                    WorkerMessage::Packet(src, data) => {
                                        let r_clients = clients.read().unwrap();
                                        if !r_clients.contains_key(&src) {
                                            debug!("packet from removed client");
                                            continue;
                                        }

                                        trace!("worker from thread {:?} got a packet of {} bytes from {}!", thread::current().id(), data.len(), src);
                                        let action = {
                                            let mut client = r_clients[&src].lock().unwrap();
                                            match client.handle(&data) {
                                                Ok(Some(a)) => a,
                                                Ok(None) => continue,
                                                Err(e) => {
                                                    error!("error while processing packet from {}: {}", src, e);
                                                    Action::Disconnect
                                                },
                                            }
                                        };
                                        if let Err(e) = match action {
                                            Action::Disconnect => Err(Error::DisconnectAction),
                                            Action::Subscribe(topic) => subscriptions.subscribe(topic, src),
                                            Action::Unsubscribe(topic) => subscriptions.unsubscribe(topic, src),
                                            Action::DispatchStart(mut start) => {
                                                for worker_tx in worker_txs.read().unwrap().iter() {
                                                    worker_tx.send(WorkerMessage::DispatchStart(src, start.clone()));
                                                }
                                                Ok(())
                                            },
                                            Action::DispatchSegment(segment) => {
                                                for worker_tx in worker_txs.read().unwrap().iter() {
                                                    worker_tx.send(WorkerMessage::DispatchSegment(src, segment.clone()));
                                                }
                                                Ok(())
                                            },
                                        } {
                                            match e {
                                                Error::DisconnectAction => info!("disconnecting {}", src),
                                                _ => info!("error while performing user {} action: {}", src, e), 
                                            }
                                            if let Err(e) = r_clients[&src].lock().unwrap().send_disconnect() {
                                                warn!("error while sending disconnect packet to {}: {}", src, e);
                                            }

                                            drop(r_clients);
                                            subscriptions.unsubscribe_all(src);
                                            clients.write().unwrap().remove(&src);
                                            manager_tx.send(WorkerMessage::Disconnect(src));
                                        }
                                    },
                                    m@WorkerMessage::DispatchStart(_, _) | m@WorkerMessage::DispatchSegment(_, _) => {
                                        let (src, topic) = match m {
                                            WorkerMessage::DispatchStart(src, ref start) => (src, start.topic()),
                                            WorkerMessage::DispatchSegment(src, ref segment) => (src, segment.topic().unwrap()),
                                            _ => panic!("impossible"),
                                        };
                                        let subs = match subscriptions.get_subscribers(topic) {
                                            Some(list) => list,
                                            None => continue,
                                        };

                                        let mut error = None;
                                        let r_clients = clients.read().unwrap();
                                        for addr in subs {
                                            let mut client = r_clients[&src].lock().unwrap();
                                            if let Err(e) = match m {
                                                WorkerMessage::DispatchStart(_, ref start) => client.send_msg_start(&start),
                                                WorkerMessage::DispatchSegment(_, ref segment) => client.send_msg_segment(&segment),
                                                _ => panic!("impossible"),
                                            } {
                                                error = Some((*addr, e));
                                                break;
                                            }
                                        }

                                        if let Some((src, e)) = error {
                                            error!("error while dispatching message start to {}: {}", src, e);
                                            if let Err(e) = r_clients[&src].lock().unwrap().send_disconnect() {
                                                warn!("error while sending disconnect packet to {}: {}", src, e);
                                            }

                                            drop(r_clients);
                                            clients.write().unwrap().remove(&src);
                                            manager_tx.send(WorkerMessage::Disconnect(src));
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
