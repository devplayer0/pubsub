use std::collections::{hash_set, HashMap, HashSet};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::net::SocketAddr;

use bytes::Bytes;
use crossbeam::queue::MsQueue;

use ::Error;
use common::protocol::{self, MessageStart, MessageSegment};
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
        match self.inner.get(topic) {
            Some(set) => Some(set.iter()),
            None => None,
        }
    }
    pub fn subscribe(&mut self, topic: &str, subscriber: SocketAddr) -> Result<(), Error> {
        let mut new = None;
        let result = {
            let set = match self.inner.get_mut(topic) {
                Some(set) => set,
                None => {
                    new = Some(HashSet::new());
                    new.as_mut().unwrap()
                },
            };

            match set.insert(subscriber) {
                true => Ok(()),
                false => Err(Error::AlreadySubscribed(topic.to_owned(), subscriber)),
            }
        };
        if let Some(set) = new {
            self.inner.insert(topic.to_owned(), set);
        }
        result
    }
    pub fn unsubscribe(&mut self, topic: &str, subscriber: SocketAddr) -> Result<(), Error> {
        if match self.inner.get_mut(topic) {
            None => return Err(Error::NotSubscribed(topic.to_owned(), subscriber)),
            Some(set) => {
                if !set.remove(&subscriber) {
                    return Err(Error::NotSubscribed(topic.to_owned(), subscriber));
                }
                set.is_empty()
            },
        } {
            self.inner.remove(topic);
        }
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
    Timeout(protocol::Timeout),
    DispatchStart(SocketAddr, MessageStart),
    DispatchSegment(SocketAddr, MessageSegment),

    Shutdown,
}
impl From<protocol::Timeout> for WorkerMessage {
    fn from(timeout: protocol::Timeout) -> Self {
        WorkerMessage::Timeout(timeout)
    }
}

#[derive(Debug)]
pub struct Worker {
    clients: Arc<RwLock<HashMap<SocketAddr, Mutex<Client>>>>,
    queue: Arc<MsQueue<WorkerMessage>>,

    thread: JoinHandle<()>,
}
impl Worker {
    pub fn new(worker_queues: &Arc<RwLock<Vec<Arc<MsQueue<WorkerMessage>>>>>, client_assignments: &Arc<RwLock<HashMap<SocketAddr, Arc<MsQueue<WorkerMessage>>>>>) -> Worker {
        let clients: Arc<RwLock<HashMap<_, Mutex<Client>>>> = Arc::new(RwLock::new(HashMap::new()));
        let queue = Arc::new(MsQueue::new());
        let thread = {
            let queue = Arc::clone(&queue);
            let clients = Arc::clone(&clients);

            let worker_queues = Arc::clone(worker_queues);
            let client_assignments = Arc::clone(client_assignments);
            thread::spawn(move || {
                let mut subscriptions = Subscriptions::new();

                let mut running = true;
                while running {
                    match queue.pop() {
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
                                Action::Subscribe(topic) => {
                                    debug!("subscribing {} to {}", src, topic);
                                    subscriptions.subscribe(topic, src)
                                },
                                Action::Unsubscribe(topic) => {
                                    debug!("unsubscribing {} from {}", src, topic);
                                    subscriptions.unsubscribe(topic, src)
                                },
                                Action::DispatchStart(mut start) => {
                                    info!("starting publish message for topic {}", start.topic());
                                    for queue in worker_queues.read().unwrap().iter() {
                                        queue.push(WorkerMessage::DispatchStart(src, start.clone()));
                                    }
                                    Ok(())
                                },
                                Action::DispatchSegment(segment) => {
                                    for queue in worker_queues.read().unwrap().iter() {
                                        queue.push(WorkerMessage::DispatchSegment(src, segment.clone()));
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
                                client_assignments.write().unwrap().remove(&src);
                            }

                        },
                        WorkerMessage::Timeout(protocol::Timeout::Keepalive(src)) => {
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
                            client_assignments.write().unwrap().remove(&src);
                        },
                        WorkerMessage::Timeout(protocol::Timeout::Ack(src)) => {
                            let clients = clients.read().unwrap();
                            let mut client = clients[&src].lock().unwrap();
                            warn!("client {} ack timed out, re-sending packets in window", src);
                            if let Err(e) = client.handle_ack_timeout() {
                                error!("error re-sending packets in window to {}: {}", src, e);
                            }
                        },
                        m@WorkerMessage::DispatchStart(_, _) | m@WorkerMessage::DispatchSegment(_, _) => {
                            let (_, topic) = match m {
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
                                let mut client = r_clients[addr].lock().unwrap();
                                if let Err(e) = match m {
                                    WorkerMessage::DispatchStart(msg_src, ref start) => {
                                        if msg_src == *addr {
                                            continue;
                                        }

                                        debug!("starting publish for topic {} to {}", start.topic(), addr);
                                        client.send_msg_start(&start)
                                    },
                                    WorkerMessage::DispatchSegment(msg_src, ref segment) => {
                                        if msg_src == *addr {
                                            continue;
                                        }

                                        client.send_msg_segment(&segment)
                                    }
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
                                client_assignments.write().unwrap().remove(&src);
                            }
                        },
                        WorkerMessage::Shutdown => running = false,
                    }
                }
            })
        };

        Worker {
            clients,
            queue,

            thread,
        }
    }
    pub fn queue(&self) -> Arc<MsQueue<WorkerMessage>> {
        Arc::clone(&self.queue)
    }
    pub fn stop(self) {
        self.queue.push(WorkerMessage::Shutdown);
        self.thread.join().unwrap();
    }
    pub fn assign(&self, client: Client) {
        self.clients.write().unwrap().insert(client.addr(), Mutex::new(client));
    }
}
