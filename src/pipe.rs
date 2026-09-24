use crate::RingBuffer;
use std::{cell::UnsafeCell, thread::sleep, time::Duration};

#[derive(Debug)]
pub struct Producer<'a, T> {
    rb: &'a UnsafeCell<RingBuffer<T>>,
}

#[derive(Debug)]
pub struct Consumer<'a, T> {
    rb: &'a UnsafeCell<RingBuffer<T>>,
}

unsafe impl<'a, T> Sync for Producer<'a, T> where T: Send {}
unsafe impl<'a, T> Sync for Consumer<'a, T> where T: Send {}

impl<'a, T> Producer<'a, T> {
    pub fn new(rb: &'a UnsafeCell<RingBuffer<T>>) -> Self {
        Producer { rb: rb }
    }

    pub fn send(&self, message: T) {
        unsafe {
            while (*self.rb.get()).is_full() {
                println!("buffer is full, producer decides to wait");
                sleep(Duration::from_millis(200));
            }
            let mut result = (*self.rb.get()).try_push(message);
            while result.is_err() {
                result = (*self.rb.get()).try_push(result.unwrap_err());
                if result.is_ok() {
                    break;
                }
            }
        }
    }
}

impl<'a, T> Consumer<'a, T> {
    pub fn new(rb: &'a UnsafeCell<RingBuffer<T>>) -> Self {
        Consumer { rb: rb }
    }

    pub fn consume(&self) -> Option<T> {
        unsafe {
            while (*self.rb.get()).is_empty() {
                println!("buffer is empty, consumer decides to wait");
                sleep(Duration::from_millis(200));
            }
            (*self.rb.get()).pop()
        }
    }
}
