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

    Shutdown,
}
impl From<protocol::Timeout> for WorkerMessage {
    fn from(timeout: protocol::Timeout) -> Self {
        WorkerMessage::Timeout(timeout)
    }
}

#[derive(Debug)]
pub(crate) struct Worker {
    queue: Arc<MsQueue<WorkerMessage>>,
    dispatch: DispatchWorker,

    thread: JoinHandle<()>,
}
impl Worker {
    pub fn new(clients: &Clients) -> Worker {
        let queue = Arc::new(MsQueue::new());
        let dispatch = DispatchWorker::new(clients);
        let thread = {
            let clients = clients.clone();
            let queue = Arc::clone(&queue);
            let dispatch_queue = dispatch.queue();

            thread::spawn(move || {
                let mut running = true;
                while running {
                    match queue.pop() {
                        WorkerMessage::Packet(src, data) => match clients.with(src, |mut client| {
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
                                    dispatch_queue.push(DispatchMessage::Start(src, start));
                                    Ok(())
                                },
                                Action::DispatchSegment(segment) => {
                                    dispatch_queue.push(DispatchMessage::Segment(src, segment));
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
                        },
                        WorkerMessage::Timeout(protocol::Timeout::Keepalive(src)) => {
                            error!("client {} timed out, disconnecting...", src);
                            if let None = clients.with(src, |mut client| {
                                if let Err(e) = client.send_disconnect() {
                                    warn!("error while sending disconnect packet to {}: {}", src, e);
                                }
                            }) {
                                debug!("keepalive timeout from removed client");
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
                                debug!("ack timeout from removed client");
                            }

                            clients.remove(src);
                        },
                        WorkerMessage::Shutdown => running = false,
                    }
                }
            })
        };

        Worker {
            queue,
            dispatch,

            thread,
        }
    }

    pub fn queue(&self) -> Arc<MsQueue<WorkerMessage>> {
        Arc::clone(&self.queue)
    }
    pub fn stop(self) {
        self.dispatch.stop();

        self.queue.push(WorkerMessage::Shutdown);
        self.thread.join().unwrap();
    }
}

#[derive(Debug)]
enum DispatchMessage {
    Start(SocketAddr, MessageStart),
    Segment(SocketAddr, MessageSegment),

    Shutdown,
}
#[derive(Debug)]
struct DispatchWorker {
    queue: Arc<MsQueue<DispatchMessage>>,
    thread: JoinHandle<()>,
}
impl DispatchWorker {
    pub fn new(clients: &Clients) -> DispatchWorker {
        let queue = Arc::new(MsQueue::new());
        let thread = {
            let clients = clients.clone();
            let queue = Arc::clone(&queue);

            thread::spawn(move || {
                let mut running = true;
                while running {
                    match queue.pop() {
                        m@DispatchMessage::Start(_, _) | m@DispatchMessage::Segment(_, _) => {
                            let (src, topic) = match m {
                                DispatchMessage::Start(src, ref start) => (src, start.topic()),
                                DispatchMessage::Segment(src, ref segment) => (src, segment.topic().unwrap()),
                                _ => panic!("impossible"),
                            };

                            clients.subscriptions().with_topic_each::<_, ()>(topic, |dst| {
                                if dst == src {
                                    return Ok(());
                                }

                                match clients.with(dst, |mut client| match m {
                                    DispatchMessage::Start(_, ref start) => {
                                        debug!("starting publish for topic {} to {}", start.topic(), dst);
                                        client.send_msg_start(start)
                                    },
                                    DispatchMessage::Segment(_, ref segment) => {
                                        client.send_msg_segment(segment)
                                    }
                                    _ => panic!("impossible"),
                                }) {
                                    Some(Ok(_)) => {},
                                    Some(Err(e)) => {
                                        error!("error while dispatching message packet to {}: {}", dst, e);
                                        clients.with(dst, |mut client| {
                                            if let Err(e) = client.send_disconnect() {
                                                warn!("error while sending disconnect packet to {}: {}", src, e);
                                            }
                                        });

                                        clients.remove(dst);
                                    },
                                    None => debug!("subscription for disconnected client!"),
                                }
                                Ok(())
                            });
                        },
                        DispatchMessage::Shutdown => running = false,
                    }
                }
            })
        };

        DispatchWorker {
            queue,
            thread,
        }
    }

    pub fn queue(&self) -> Arc<MsQueue<DispatchMessage>> {
        Arc::clone(&self.queue)
    }
    pub fn stop(self) {
        self.queue.push(DispatchMessage::Shutdown);
        self.thread.join().unwrap();
    }
}
