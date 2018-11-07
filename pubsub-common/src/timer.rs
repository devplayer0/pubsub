use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::sync::atomic::Ordering as AtOrdering;
use std::thread::{self, JoinHandle};
use std::time::Instant;

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

#[derive(DebugStub)]
pub struct Timer<M: Send + Sync + 'static> {
    id: usize,

    #[debug_stub = "M"]
    message: Arc<RwLock<Option<M>>>,
    completed: Arc<AtomicBool>,
    #[debug_stub = "FnMut(M)"]
    callback: Arc<Mutex<Box<FnMut(M) + Send + Sync>>>,
}
impl<M: Send + Sync + 'static> Clone for Timer<M> {
    fn clone(&self) -> Timer<M> {
        Timer {
            id: self.id,

            message: Arc::clone(&self.message),
            completed: Arc::clone(&self.completed),
            callback: Arc::clone(&self.callback),
        }
    }
}
impl<M: Send + Sync + 'static> Timer<M> {
    fn empty<C: FnMut(M) + Send + Sync + 'static>(id: usize, callback: C) -> Timer<M> {
        Timer {
            id,

            message: Arc::new(RwLock::new(None)),
            completed: Arc::new(AtomicBool::new(true)),
            callback: Arc::new(Mutex::new(Box::new(callback))),
        }
    }
    fn new<C: FnMut(M) + Send + Sync + 'static>(id: usize, message: M, callback: C) -> Timer<M> {
        Timer {
            id,

            completed: Arc::new(AtomicBool::new(false)),
            message: Arc::new(RwLock::new(Some(message))),
            callback: Arc::new(Mutex::new(Box::new(callback))),
        }
    }

    pub fn completed(&self) -> bool {
        self.completed.load(AtOrdering::SeqCst)
    }
    fn reset(&self) {
        self.completed.store(false, AtOrdering::SeqCst);
    }
    #[inline]
    fn cancel(&self) -> bool {
        let old = self.completed();
        self.completed.store(true, AtOrdering::SeqCst);
        !old
    }
    fn trigger(&self) {
        let message = self.message.write().unwrap().take().unwrap();
        (&mut *self.callback.lock().unwrap())(message);
        self.cancel();
    }

    pub fn has_message(&self) -> bool {
        self.message.read().unwrap().is_some()
    }
    pub fn set_message(&self, message: M) -> bool {
        self.message.write().unwrap().replace(message).is_some()
    }
}
impl<M: Send + Sync + 'static> PartialEq for Timer<M> {
    fn eq(&self, other: &Timer<M>) -> bool {
        self.id == other.id
    }
}
impl<M: Send + Sync + 'static> Eq for Timer<M> {}
impl<M: Send + Sync + 'static> Hash for Timer<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Debug)]
pub struct TimerManager<M: Send + Sync + 'static> {
    timers: Arc<Mutex<PriorityQueue<Timer<M>, ReverseInstant>>>,
    next_id: Arc<AtomicUsize>,

    thread: Arc<JoinHandle<()>>,
    running: Arc<Mutex<bool>>,
    wakeup: Arc<(Mutex<bool>, Condvar)>,
}
impl<M: Send + Sync + Hash + 'static> Clone for TimerManager<M> {
    fn clone(&self) -> TimerManager<M> {
        TimerManager {
            timers: Arc::clone(&self.timers),
            next_id: Arc::clone(&self.next_id),

            thread: Arc::clone(&self.thread),
            running: Arc::clone(&self.running),
            wakeup: Arc::clone(&self.wakeup),
        }
    }
}
impl<M: Send + Sync + 'static> TimerManager<M> {
    pub fn new() -> TimerManager<M> {
        let running = Arc::new(Mutex::new(true));
        let timers: Arc<Mutex<PriorityQueue<Timer<M>, ReverseInstant>>> = Arc::new(Mutex::new(PriorityQueue::new()));

        let wakeup = Arc::new((Mutex::new(false), Condvar::new()));
        let thread = Arc::new({
            let running_lock = Arc::clone(&running);
            let timers = Arc::clone(&timers);

            let wakeup = Arc::clone(&wakeup);
            thread::spawn(move || {
                while *running_lock.lock().unwrap() {
                    if timers.lock().unwrap().is_empty() {
                        let &(ref lock, ref cvar) = &*wakeup;
                        let mut woke = cvar.wait_until(lock.lock().unwrap(), |&mut woke| woke).unwrap();
                        *woke = false;
                        continue;
                    }
                    let (time, completed) = {
                        let timers = timers.lock().unwrap();
                        let (timer, time) = timers.peek().unwrap();
                        ((*time).into(), timer.completed())
                    };

                    if completed {
                        timers.lock().unwrap().pop().unwrap();
                        continue;
                    }
                    if Instant::now() > time {
                        let (timer, _) = timers.lock().unwrap().pop().unwrap();
                        timer.trigger();
                    } else {
                        let &(ref lock, ref cvar) = &*wakeup;
                        match cvar.wait_timeout_until(lock.lock().unwrap(), time - Instant::now(), |&mut woke| woke).unwrap() {
                            (_, timeout) if timeout.timed_out() => {
                                let (timer, _) = timers.lock().unwrap().pop().unwrap();
                                timer.trigger();
                            },
                            (mut woke, _) => *woke = false,
                        }
                    }
                }
            })
        });

        TimerManager {
            timers,
            next_id: Arc::new(AtomicUsize::new(0)),

            thread,
            running,
            wakeup,
        }
    }

    pub fn create_empty<C: FnMut(M) + Send + Sync + 'static>(&self, callback: C) -> Timer<M> {
        let id = self.next_id.fetch_add(1, AtOrdering::Relaxed);
        Timer::empty(id, callback)
    }
    pub fn create<C: FnMut(M) + Send + Sync + 'static>(&self, message: M, callback: C) -> Timer<M> {
        let id = self.next_id.fetch_add(1, AtOrdering::Relaxed);
        let t = Timer::new(id, message, callback);
        t.cancel();
        t
    }
    pub fn post_new<C: FnMut(M) + Send + Sync + 'static>(&self, message: M, callback: C, when: Instant) -> Timer<M> {
        let id = self.next_id.fetch_add(1, AtOrdering::Relaxed);
        let timer = Timer::new(id, message, callback);
        self.timers.lock().unwrap().push(timer.clone(), when.into());

        wake_thread!(self.wakeup);
        timer
    }
    pub fn cancel(&self, timer: &Timer<M>) -> bool {
        if timer.cancel() {
            wake_thread!(self.wakeup);
            true
        } else {
            false
        }
    }
    pub fn reschedule(&self, timer: &Timer<M>, when: Instant) -> bool {
        if timer.completed() {
            assert!(timer.has_message());
            timer.reset();
            self.timers.lock().unwrap().push(timer.clone(), when.into());

            wake_thread!(self.wakeup);
            return true;
        }

        self.timers.lock().unwrap().change_priority(timer, when.into());
        wake_thread!(self.wakeup);
        false
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
