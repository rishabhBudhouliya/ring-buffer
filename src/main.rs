use std::sync::Condvar;
use std::{sync::Mutex, thread, time::Duration};

use crate::lib_bkp::RingBuffer;
use crate::pipe::Pipe;

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

fn consumer_batch(pipe: &Pipe<u32>, total: u32) {
    let result = pipe.consume_all(total);
    println!("count is : {}", result.len());
    assert_eq!(result.len() as u32, total);
}

fn producer(pipe: &Pipe<u32>, total: u32) {
    let mut input = Vec::new();
    for i in 0..total {
        input.push(i);
    }
    pipe.send_all(input);
    println!("producer finished its job");
}

fn main() {
    // main thread will be the producer 10000000
    let total: u32 = 10000000; // 10 million
    let pipe = Pipe::new(1024);
    let start = std::time::Instant::now();
    std::thread::scope(|s| {
        s.spawn(|| producer(&pipe, total));
        s.spawn(|| consumer_batch(&pipe, total));
    });
    let end = std::time::Instant::now();
    let ops = (total as f32) / ((end - start).as_secs_f32());
    println!("ops/s performed while processing: {total} is {}", ops);
}
