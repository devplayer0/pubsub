use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::sync::atomic::Ordering as AtOrdering;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::Sender;
use priority_queue::PriorityQueue;

macro_rules! wake_thread {
    ($cvar:expr) => {{
        let &(ref lock, ref cvar) = &*$cvar;
        let mut woke = lock.lock().unwrap();
        *woke = true;
        cvar.notify_one();
    }};
}

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

#[derive(Debug)]
struct TimerInfo<M: Send + Eq + Hash + 'static> {
    id: usize,
    client: usize,
    message: M,
    cancelled: Arc<AtomicBool>,
}
impl<M: Send + Eq + Hash + 'static> TimerInfo<M> {
    pub fn new(id: usize, client: usize, message: M) -> TimerInfo<M> {
        TimerInfo {
            id,
            client,
            message: message,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl<M: Send + Eq + Hash + 'static> PartialEq for TimerInfo<M> {
    fn eq(&self, other: &TimerInfo<M>) -> bool {
        self.id == other.id
    }
}
impl<M: Send + Eq + Hash + 'static> Eq for TimerInfo<M> {}
impl<M: Send + Eq + Hash + 'static> Hash for TimerInfo<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
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
    timers_map: Arc<RwLock<HashMap<usize, Arc<AtomicBool>>>>,
    next_id: Arc<AtomicUsize>,

    thread: Arc<JoinHandle<()>>,
    running: Arc<Mutex<bool>>,
    wakeup: Arc<(Mutex<bool>, Condvar)>,
}
impl<M: Send + Eq + Hash + 'static> Clone for TimerManager<M> {
    fn clone(&self) -> TimerManager<M> {
        TimerManager {
            clients: Arc::clone(&self.clients),
            next_client_id: Arc::clone(&self.next_client_id),
            timers: Arc::clone(&self.timers),
            timers_map: Arc::clone(&self.timers_map),
            next_id: Arc::clone(&self.next_id),

            thread: Arc::clone(&self.thread),
            running: Arc::clone(&self.running),
            wakeup: Arc::clone(&self.wakeup),
        }
    }
}
impl<M: Send + Eq + Hash + 'static> TimerManager<M> {
    pub fn new() -> TimerManager<M> {
        let running = Arc::new(Mutex::new(true));
        let clients: Arc<RwLock<HashMap<_, Sender<M>>>> = Arc::new(RwLock::new(HashMap::new()));
        let timers: Arc<Mutex<PriorityQueue<TimerInfo<M>, ReverseInstant>>> = Arc::new(Mutex::new(PriorityQueue::new()));
        let timers_map = Arc::new(RwLock::new(HashMap::new()));

        let wakeup = Arc::new((Mutex::new(false), Condvar::new()));
        let thread = Arc::new({
            let running_lock = Arc::clone(&running);
            let clients = Arc::clone(&clients);
            let timers = Arc::clone(&timers);
            let timers_map = Arc::clone(&timers_map);

            let wakeup = Arc::clone(&wakeup);
            thread::spawn(move || {
                while *running_lock.lock().unwrap() {
                    if timers.lock().unwrap().is_empty() {
                        let &(ref lock, ref cvar) = &*wakeup;
                        let mut woke = cvar.wait_until(lock.lock().unwrap(), |&mut woke| woke).unwrap();
                        *woke = false;
                        continue;
                    }
                    let (time, cancelled) = {
                        let timers = timers.lock().unwrap();
                        let (timer, time) = timers.peek().unwrap();
                        ((*time).into(), timer.cancelled.load(AtOrdering::SeqCst))
                    };

                    if cancelled {
                        timers.lock().unwrap().pop().unwrap();
                        continue;
                    }
                    if Instant::now() > time {
                        let (timer, _) = timers.lock().unwrap().pop().unwrap();
                        timers_map.write().unwrap().remove(&timer.id);
                        clients.read().unwrap()[&timer.client].send(timer.message);
                    } else {
                        let &(ref lock, ref cvar) = &*wakeup;
                        match cvar.wait_timeout_until(lock.lock().unwrap(), time - Instant::now(), |&mut woke| woke).unwrap() {
                            (_, timeout) if timeout.timed_out() => {
                                let (timer, _) = timers.lock().unwrap().pop().unwrap();
                                timers_map.write().unwrap().remove(&timer.id);
                                clients.read().unwrap()[&timer.client].send(timer.message);
                            },
                            (ref mut woke, _) => **woke = false,
                        }
                    }
                }
            })
        });

        TimerManager {
            clients,
            next_client_id: Arc::new(AtomicUsize::new(0)),
            timers,
            timers_map,
            next_id: Arc::new(AtomicUsize::new(0)),

            thread,
            running,
            wakeup,
        }
    }

    pub fn register(&self, client: &Sender<M>) -> TimerManagerClient {
        let id = self.next_client_id.fetch_add(1, AtOrdering::Relaxed);
        self.clients.write().unwrap().insert(id, client.clone());
        TimerManagerClient(id)
    }
    pub fn unregister(&self, client: TimerManagerClient) {
        self.clients.write().unwrap().remove(&client.0);
    }
    pub fn post_message(&self, client: TimerManagerClient, message: M, when: Instant) -> Timer {
        let id = self.next_id.fetch_add(1, AtOrdering::Relaxed);
        let info = TimerInfo::new(id, client.0, message);

        self.timers_map.write().unwrap().insert(id, Arc::clone(&info.cancelled));
        self.timers.lock().unwrap().push(info, when.into());

        wake_thread!(self.wakeup);
        Timer(id)
    }
    pub fn expired(&self, timer: Timer) -> bool {
        let cancellations = self.timers_map.read().unwrap();
        !cancellations.contains_key(&timer.0)
    }
    pub fn cancel_message(&self, timer: Timer) -> bool {
        let found = {
            let mut cancellations = self.timers_map.write().unwrap();
            match cancellations.remove(&timer.0) {
                Some(cancellation) => {
                    cancellation.store(true, AtOrdering::SeqCst);
                    true
                },
                None => false
            }
        };

        if !found {
            return false;
        }
        wake_thread!(self.wakeup);
        true
    }

    pub fn stop(self) {
        if !*self.running.lock().unwrap() {
            return;
        }

        *self.running.lock().unwrap() = false;
        wake_thread!(self.wakeup);
        Arc::try_unwrap(self.thread).unwrap().join().unwrap();
    }
}
