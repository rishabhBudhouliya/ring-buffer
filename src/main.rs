use ring_buffer::Pipe;

fn consumer(pipe: &Pipe<u32>, total: u32) {
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
        s.spawn(|| consumer(&pipe, total));
    });
    let end = std::time::Instant::now();
    let ops = (total as f32) / ((end - start).as_secs_f32());
    println!("ops/s performed while processing: {total} is {}", ops);
}
