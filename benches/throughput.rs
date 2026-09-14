//! Throughput of one producer thread pushing 0..N through the pipe while one
//! consumer thread drains it. Compare versions by checking out a git tag and
//! rerunning `cargo bench`; criterion prints the change against the previous run.

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use ring_buffer::Pipe;
use std::hint::black_box;
use std::time::Duration;

const N: u32 = 1_000_000;
const CAPACITY: usize = 1024;

fn producer_consumer(input: Vec<u32>) -> Vec<u32> {
    let pipe = Pipe::new(CAPACITY);
    std::thread::scope(|s| {
        s.spawn(|| pipe.send_all(input));
        let consumer = s.spawn(|| pipe.consume_all(N));
        consumer.join().unwrap()
    })
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc");
    group.throughput(Throughput::Bytes(N as u64));
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("pipe_1M", |b| {
        b.iter_batched(
            || (0..N).collect::<Vec<u32>>(),
            |input| black_box(producer_consumer(input)),
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
