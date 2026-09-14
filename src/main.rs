use std::sync::Condvar;
use std::{sync::Mutex, thread, time::Duration};

use crate::lib_bkp::RingBuffer;

mod lib_bkp;
mod pipe;

/*
*
*/

fn consumer(rb: &Mutex<RingBuffer<u32>>, total: u32, wait_lock: &Condvar, wait_producer: &Condvar) {
    let mut count = 0;
    loop {
        let mut guard_on_ring_buffer = rb.lock().unwrap();
        if guard_on_ring_buffer.is_empty() {
            if count == total {
                drop(guard_on_ring_buffer);
                break;
            }
            guard_on_ring_buffer = wait_lock.wait(guard_on_ring_buffer).unwrap();
        } else {
            guard_on_ring_buffer.pop();
            drop(guard_on_ring_buffer);
            wait_producer.notify_one();
            count += 1;
        }
    }
    println!("count is : {}", count);
    assert_eq!(count, total);
}

// Earlier variant: holds the lock across the whole drain and
// notifies while still holding it.
fn consumer_batch(
    rb: &Mutex<RingBuffer<u32>>,
    total: u32,
    wait_lock: &Condvar,
    wait_producer: &Condvar,
) {
    let mut count = 0;
    let mut guard_on_ring_buffer = rb.lock().unwrap();
    loop {
        if guard_on_ring_buffer.is_empty() {
            if count == total {
                drop(guard_on_ring_buffer);
                break;
            }
            guard_on_ring_buffer = wait_lock.wait(guard_on_ring_buffer).unwrap();
        } else {
            guard_on_ring_buffer.pop();
            wait_producer.notify_one();
            count += 1;
        }
    }
    println!("count is : {}", count);
    assert_eq!(count, total);
}

fn producer(rb: &Mutex<RingBuffer<u32>>, total: u32, wait_lock: &Condvar, wait_producer: &Condvar) {
    for i in 0..total {
        let mut g_ret = rb.lock().unwrap();
        let ret = g_ret.try_push(i);
        match ret {
            Err(val) => loop {
                if g_ret.is_full() {
                    g_ret = wait_producer.wait(g_ret).unwrap();
                } else {
                    let retry = g_ret.try_push(val);
                    if retry.is_ok() {
                        drop(g_ret);
                        wait_lock.notify_one();
                        break;
                    }
                }
            },
            Ok(()) => {
                drop(g_ret);
                wait_lock.notify_one()
            }
        }
    }
    println!("producer finished its job");
}

fn main() {
    // main thread will be the producer 10000000
    let total: u32 = 10000000; // 10 million
    let rb = Mutex::new(lib_bkp::RingBuffer::new(1024));
    let start = std::time::Instant::now();
    let wait_lock = Condvar::new();
    let wait_producer = Condvar::new();
    std::thread::scope(|s| {
        s.spawn(|| producer(&rb, total, &wait_lock, &wait_producer));
        s.spawn(|| consumer_batch(&rb, total, &wait_lock, &wait_producer));
    });
    let end = std::time::Instant::now();
    let ops = (total as f32) / ((end - start).as_secs_f32());
    println!("ops/s performed while processing: {total} is {}", ops);
}
