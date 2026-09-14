use std::sync::{Condvar, Mutex};

use crate::RingBuffer;

pub struct Pipe<T> {
    rb: Mutex<RingBuffer<T>>,
    is_full: Condvar,
    is_empty: Condvar,
}

impl<T> Pipe<T> {
    fn new(capacity: usize) -> Self {
        Pipe {
            rb: Mutex::new(RingBuffer::new(capacity)),
            is_full: Condvar::new(),
            is_empty: Condvar::new(),
        }
    }

    fn send(&self, mut message: T) {
        let mut guard = self.rb.lock().unwrap();
        loop {
            if guard.is_full() {
                guard = self.is_full.wait(guard).unwrap();
            } else {
                let result = guard.try_push(message);
                self.is_empty.notify_one();
                match result {
                    Err(val) => message = val,
                    Ok(()) => break,
                }
            }
        }
    }

    fn consume(&self) -> Option<T> {
        let mut guard = self.rb.lock().unwrap();
        loop {
            if guard.is_empty() {
                guard = self.is_empty.wait(guard).unwrap();
            } else {
                let result = guard.pop();
                self.is_full.notify_one();
                return result;
            }
        }
    }
}
