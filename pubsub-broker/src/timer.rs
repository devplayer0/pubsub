use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam::channel::{self, Sender, Receiver};
use crossbeam::queue::MsQueue;

use ::worker::WorkerMessage;

struct Timer {
    client: usize,
    time: Instant,
    message: WorkerMessage,
}
pub struct TimerManager {
    clients: Arc<RwLock<HashMap<usize, Sender<WorkerMessage>>>>,
    timers: Arc<MsQueue<Timer>>,

    thread: JoinHandle<()>,
    running: Arc<Mutex<bool>>,
}
impl TimerManager {
    pub fn new() -> TimerManager {
        let running = Arc::new(Mutex::new(true));
        let clients = Arc::new(RwLock::new(HashMap::new()));
        let timers = Arc::new(MsQueue::new());
        let thread = {
            let running_lock = Arc::clone(&running);
            let clients = Arc::clone(&clients);
            let timers = Arc::clone(&timers);
            thread::spawn(move || {
                while *running_lock.lock().unwrap() {
                    let timer = timers.pop();
                    //thread::sleep(Instant::now() - timer.time);
                    // use mpsc recv_timeout instead
                    // priority queue based on time
                }
            })
        };

        TimerManager {
            clients,
            timers,
            thread,
            running,
        }
    }
}
