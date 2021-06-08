use std::{
    sync::{
        Arc,
        Mutex,
    },
    thread,
    time,
};

/// A thread safe counter
pub struct Counter {
    count: Arc<Mutex<usize>>,
}

impl Counter {
    pub fn new() -> Counter {
        Counter {
            count: Arc::new(Mutex::new(0)),
        }
    }

    pub fn clone(&self) -> Counter {
        Counter {
            count: Arc::clone(&self.count),
        }
    }

    pub fn wait_if_above(&self, target: usize) {
        const WAIT_TIME: std::time::Duration = time::Duration::from_millis(100);
        loop {
            {
                let count = self.count.lock().unwrap();
                if *count <= target {
                    break;
                }
            }
            thread::sleep(WAIT_TIME);
        }
    }

    pub fn increase(&self) {
        let mut count = self.count.lock().unwrap();
        *count += 1;
        println!("Block processor count: {}", *count);
    }

    pub fn decrease(&self) {
        let mut count = self.count.lock().unwrap();
        *count -= 1;
        println!("Block processor count: {}", *count);
    }
}
