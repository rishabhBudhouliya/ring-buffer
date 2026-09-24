use ring_buffer::{RingBuffer, pipe::Consumer, pipe::Producer};
use std::cell::UnsafeCell;

fn consumer(pipe: &Consumer<u32>, total: u32) {
    let mut result_list = Vec::new();
    for i in 0..total {
        let ret = pipe.consume();
        if ret.is_some() {
            result_list.push(ret.unwrap());
        } else {
            println!("element: {i} is missing");
        }
    }

    for i in 0..total {
        assert_eq!(i, result_list[i as usize])
    }
    // assert order for the consumer
    //     assert_eq!(
    //         i,
    //         ret.unwrap(),
    //         "consumer expectation: {} vs real: {}",
    //         i,
    //         ret.unwrap()
    //     );
    // }
    // println!("count is : {}", result_list.len());
    // let ret = std::panic::catch_unwind(|| {
    //     assert_eq!(result_list.len() as u32, total);
    // });

    // if ret.is_err() {
    //     println!("producer/consumer mismatch: {ret:?}");
    // }
}

fn producer(pipe: &Producer<u32>, total: u32) {
    for i in 0..total {
        pipe.send(i);
    }
    println!("producer finished its job");
}

fn main() {
    // main thread will be the producer 10000000
    let total: u32 = 10000000; // 10 million
    let rb = UnsafeCell::new(RingBuffer::new(1024));
    let p = Producer::new(&rb);
    let c = Consumer::new(&rb);
    let start = std::time::Instant::now();
    std::thread::scope(|s| {
        s.spawn(|| producer(&p, total));
        s.spawn(|| consumer(&c, total));
    });
    let end = std::time::Instant::now();
    let ops = (total as f32) / ((end - start).as_secs_f32());
    println!("ops/s performed while processing: {total} is {}", ops);
}
