use std::sync::{Condvar, Mutex};

use crate::RingBuffer;

#[derive(Debug)]
pub struct Pipe<T> {
    rb: Mutex<RingBuffer<T>>,
    is_full: Condvar,
    is_empty: Condvar,
}

impl<T> Pipe<T> {
    pub fn new(capacity: usize) -> Self {
        Pipe {
            rb: Mutex::new(RingBuffer::new(capacity)),
            is_full: Condvar::new(),
            is_empty: Condvar::new(),
        }
    }

    pub fn send(&self, mut message: T) {
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

    pub fn send_all(&self, messages: Vec<T>) {
        let mut guard = self.rb.lock().unwrap();
        for mut message in messages {
            loop {
                if guard.is_full() {
                    self.is_empty.notify_one();
                    guard = self.is_full.wait(guard).unwrap();
                } else {
                    let result = guard.try_push(message);
                    match result {
                        Err(val) => message = val,
                        Ok(()) => break,
                    }
                }
            }
        }
        self.is_empty.notify_one();
    }

    pub fn consume_all(&self, total: u32) -> Vec<T> {
        let mut guard = self.rb.lock().unwrap();
        let mut consumed_ret = Vec::<T>::new();
        let mut count = 0;
        loop {
            if guard.is_empty() {
                if count == total {
                    self.is_full.notify_one();
                    drop(guard);
                    return consumed_ret;
                }
                self.is_full.notify_one();
                guard = self.is_empty.wait(guard).unwrap();
            } else {
                let result = guard.pop();
                consumed_ret.push(result.unwrap());
                count += 1;
            }
        }
    }

    pub fn consume(&self) -> Option<T> {
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
