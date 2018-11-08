use std::thread::{self, JoinHandle};
use std::sync::Arc;
use std::net::SocketAddr;

use bytes::Bytes;
use crossbeam::queue::MsQueue;

use ::Error;
use common::protocol::{self, MessageStart, MessageSegment};
use client::{Action, Clients};

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
pub(crate) struct Worker {
    thread: JoinHandle<()>,
}
impl Worker {
    pub fn new(clients: &Clients, queue: &Arc<MsQueue<WorkerMessage>>) -> Worker {
        let thread = {
            let clients = clients.clone();
            let queue = Arc::clone(queue);

            thread::spawn(move || {
                let mut running = true;
                while running {
                    match queue.pop() {
                        WorkerMessage::Packet(src, data) => {
                            match clients.with(src, |mut client| {
                                trace!("worker from thread {:?} got a packet of {} bytes from {}!", thread::current().id(), data.len(), src);
                                let action = {
                                    match client.handle(&data) {
                                        Ok(Some(a)) => a,
                                        Ok(None) => return false,
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
                                        clients.subscriptions().subscribe(topic, src)
                                    },
                                    Action::Unsubscribe(topic) => {
                                        debug!("unsubscribing {} from {}", src, topic);
                                        clients.subscriptions().unsubscribe(topic, src)
                                    },
                                    Action::DispatchStart(start) => {
                                        info!("starting publish message for topic {}", start.topic());
                                        queue.push(WorkerMessage::DispatchStart(src, start));
                                        Ok(())
                                    },
                                    Action::DispatchSegment(segment) => {
                                        queue.push(WorkerMessage::DispatchSegment(src, segment));
                                        Ok(())
                                    },
                                } {
                                    match e {
                                        Error::DisconnectAction => info!("disconnecting {}", src),
                                        _ => info!("error while performing user {} action: {}", src, e),
                                    }
                                    if let Err(e) = client.send_disconnect() {
                                        warn!("error while sending disconnect packet to {}: {}", src, e);
                                    }

                                    return true;
                                }

                                false
                            }) {
                                Some(false) => {},
                                Some(true) => clients.remove(src),
                                None => debug!("packet from removed client"),
                            }
                        },
                        WorkerMessage::Timeout(protocol::Timeout::Keepalive(src)) => {
                            error!("client {} timed out, disconnecting...", src);
                            if let None = clients.with(src, |mut client| {
                                if let Err(e) = client.send_disconnect() {
                                    warn!("error while sending disconnect packet to {}: {}", src, e);
                                }
                            }) {
                                debug!("packet from removed client");
                            }

                            clients.remove(src);
                        },
                        WorkerMessage::Timeout(protocol::Timeout::Ack(src)) => {
                            if let None = clients.with(src, |mut client| {
                                warn!("client {} ack timed out, re-sending packets in window", src);
                                if let Err(e) = client.handle_ack_timeout() {
                                    error!("error re-sending packets in window to {}: {}", src, e);
                                }
                            }) {
                                debug!("packet from removed client");
                            }

                            clients.remove(src);
                        },
                        m@WorkerMessage::DispatchStart(_, _) | m@WorkerMessage::DispatchSegment(_, _) => {
                            let (src, topic) = match m {
                                WorkerMessage::DispatchStart(src, ref start) => (src, start.topic()),
                                WorkerMessage::DispatchSegment(src, ref segment) => (src, segment.topic().unwrap()),
                                _ => panic!("impossible"),
                            };

                            if let Some(Err((dst, e))) = clients.subscriptions().with_topic_each(topic, |dst| {
                                if dst == src {
                                    return Ok(());
                                }

                                clients.with(dst, |mut client| {
                                    if let Err(e) = match m {
                                        WorkerMessage::DispatchStart(_, ref start) => {
                                            debug!("starting publish for topic {} to {}", start.topic(), dst);
                                            client.send_msg_start(&start)
                                        },
                                        WorkerMessage::DispatchSegment(_, ref segment) => {
                                            client.send_msg_segment(&segment)
                                        }
                                        _ => panic!("impossible"),
                                    } {
                                        return Err((dst, e));
                                    }
                                    Ok(())
                                }).expect("subscription for disconnected client!")
                            }) {
                                error!("error while dispatching message start to {}: {}", dst, e);
                                clients.with(dst, |mut client| {
                                    if let Err(e) = client.send_disconnect() {
                                        warn!("error while sending disconnect packet to {}: {}", src, e);
                                    }
                                }).unwrap();

                                clients.remove(dst);
                            }
                        },
                        WorkerMessage::Shutdown => running = false,
                    }
                }
            })
        };

        Worker {
            thread,
        }
    }
    pub fn join(self) {
        self.thread.join().unwrap();
    }
}
