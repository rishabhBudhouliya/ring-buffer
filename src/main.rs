// use ring_buffer::;
// use ring_buffer

mod lib_bkp;

fn main() {
    let mut rb = lib_bkp::RingBuffer::new(12);
    for i in 1..14 {
        println!("the head is: {}", &rb.head);
        println!("the tail is: {}", &rb.tail);
        &rb.push(i);
    }
    dbg!(&rb);
    assert_eq!(*(&rb.peek()), 1);

    // let assert_ret = 1;
    // for _ in ..3 {
    //     let ret = &rb.pop();
    //     assert_eq!(ret, assert_ret);
    // }

    println!("{}", &rb.peek());
}
