use std::cmp::Ordering;
use std::hash::Hash;
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtOrdering;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::Sender;
use priority_queue::PriorityQueue;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
struct ReverseInstant(Instant);
impl Ord for ReverseInstant {
    fn cmp(&self, other: &ReverseInstant) -> Ordering {
        self.0.cmp(&other.0).reverse()
    }
}
impl PartialOrd for ReverseInstant {
    fn partial_cmp(&self, other: &ReverseInstant) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl From<Instant> for ReverseInstant {
    fn from(duration: Instant) -> Self {
        ReverseInstant(duration)
    }
}
impl Into<Instant> for ReverseInstant {
    fn into(self) -> Instant {
        self.0
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct TimerInfo<M: Send + Eq + Hash + 'static> {
    id: usize,
    client: usize,
    message: M,
    cancelled: bool,
}
impl<M: Send + Eq + Hash + 'static> TimerInfo<M> {
    pub fn new(id: usize, client: usize, message: M) -> TimerInfo<M> {
        TimerInfo {
            id,
            client,
            message,
            cancelled: false,
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct TimerManagerClient(usize);
#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct Timer(usize);
#[derive(Debug)]
pub struct TimerManager<M: Send + Eq + Hash + 'static> {
    clients: Arc<RwLock<HashMap<usize, Sender<M>>>>,
    next_client_id: Arc<AtomicUsize>,
    timers: Arc<Mutex<PriorityQueue<TimerInfo<M>, ReverseInstant>>>,
    next_id: Arc<AtomicUsize>,

    thread: Arc<JoinHandle<()>>,
    running: Arc<Mutex<bool>>,
    wakeup_tx: mpsc::SyncSender<()>,
}
impl<M: Send + Eq + Hash + 'static> Clone for TimerManager<M> {
    fn clone(&self) -> TimerManager<M> {
        TimerManager {
            clients: Arc::clone(&self.clients),
            next_client_id: Arc::clone(&self.next_client_id),
            timers: Arc::clone(&self.timers),
            next_id: Arc::clone(&self.next_id),

            thread: Arc::clone(&self.thread),
            running: Arc::clone(&self.running),
            wakeup_tx: self.wakeup_tx.clone(),
        }
    }
}
impl<M: Send + Eq + Hash + 'static> TimerManager<M> {
    pub fn new() -> TimerManager<M> {
        let running = Arc::new(Mutex::new(true));
        let clients: Arc<RwLock<HashMap<_, Sender<M>>>> = Arc::new(RwLock::new(HashMap::new()));
        let timers: Arc<Mutex<PriorityQueue<TimerInfo<M>, ReverseInstant>>> = Arc::new(Mutex::new(PriorityQueue::new()));
        let (wakeup_tx, wakeup_rx) = mpsc::sync_channel(0);
        let thread = Arc::new({
            let running_lock = Arc::clone(&running);
            let clients = Arc::clone(&clients);
            let timers = Arc::clone(&timers);
            thread::spawn(move || {
                while *running_lock.lock().unwrap() {
                    if timers.lock().unwrap().is_empty() {
                        if let Err(_) = wakeup_rx.recv() {
                            break;
                        }
                        continue;
                    }
                    let (time, cancelled) = {
                        let timers = timers.lock().unwrap();
                        let (timer, time) = timers.peek().unwrap();
                        ((*time).into(), timer.cancelled)
                    };

                    if cancelled {
                        timers.lock().unwrap().pop().unwrap();
                    }
                    if Instant::now() > time {
                        let (timer, _) = timers.lock().unwrap().pop().unwrap();
                        clients.read().unwrap()[&timer.client].send(timer.message);
                    } else {
                        match wakeup_rx.recv_timeout(time - Instant::now()) {
                            Err(mpsc::RecvTimeoutError::Timeout) => {
                                let (timer, _) = timers.lock().unwrap().pop().unwrap();
                                clients.read().unwrap()[&timer.client].send(timer.message);
                            },
                            Ok(_) => {},
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        }
                    }
                }
            })
        });

        TimerManager {
            clients,
            next_client_id: Arc::new(AtomicUsize::new(0)),
            timers,
            next_id: Arc::new(AtomicUsize::new(0)),

            thread,
            running,
            wakeup_tx,
        }
    }

    pub fn register(&self, client: &Sender<M>) -> TimerManagerClient {
        let id = self.next_client_id.fetch_add(1, AtOrdering::Relaxed);
        self.clients.write().unwrap().insert(id, client.clone());
        TimerManagerClient(id)
    }
    pub fn unregister(&self, client: usize) {
        self.clients.write().unwrap().remove(&client);
    }
    pub fn post_message(&self, client: TimerManagerClient, message: M, when: Instant) -> Timer {
        let id = self.next_id.fetch_add(1, AtOrdering::Relaxed);
        self.timers.lock().unwrap().push(TimerInfo::new(id, client.0, message), when.into());
        self.wakeup_tx.send(()).unwrap();
        Timer(id)
    }
    pub fn cancel_message(&self, timer: Timer) -> bool {
        let found = {
            let mut found = false;
            let mut timers = self.timers.lock().unwrap();
            for (info, _) in timers.iter_mut() {
                if info.id == timer.0 {
                    info.cancelled = true;
                    found = true;
                }
            }

            found
        };

        if !found {
            return false;
        }
        self.wakeup_tx.send(()).unwrap();
        true
    }

    pub fn stop(self) {
        if !*self.running.lock().unwrap() {
            return;
        }

        *self.running.lock().unwrap() = false;
        let _ = self.wakeup_tx.send(());
        Arc::try_unwrap(self.thread).unwrap().join().unwrap();
    }
}
