/*
 * Implementing a lock free ring buffer
 * 1) create a data structure that is naive, uses a memory backed vector and pointer arithmetic
 * 2) keep reading the blog, use memory transumtation with shared memory to make life easier
 * 3) look at atomics at Rust and multi threaded
 *
 */
#[derive(Debug)]
pub struct RingBuffer<T> {
    //  bunch of bytes -- no, a bunch of generic types
    data: Vec<Option<T>>,
    // apparently, 64 bytes is good for cache coherency?
    pub head: u32,
    pub tail: u32,
    capacity: usize,
}

// now, I need a way to initialize this data structure, let's keep it on a heap for now and then define the contract to interact
// with it

// concerns in impl-aug-25.txt

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        if capacity > u32::MAX as usize {
            panic!("can construct larger than u32 max")
        }
        let mut data_alloc: Vec<Option<T>> = Vec::with_capacity(capacity);
        data_alloc.resize_with(capacity, || None);
        RingBuffer {
            data: data_alloc,
            head: 0,
            tail: 0,
            capacity,
        }
    }

    pub fn is_full(&self) -> bool {
        (self.tail - self.head) == (self.capacity as u32)
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    // what can we pop?
    // a peek should not take an actual slot value
    pub fn peek(&self) -> Option<&T> {
        if self.is_empty() {
            println!("sorry there's nothing to peek");
            return None;
        }
        let index = (self.head as usize) % self.capacity;
        self.data[index].as_ref()
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            println!("sorry there's nothing to pop");
            return None;
        }
        let index = (self.head as usize) % self.capacity;
        self.head += 1;
        self.data[index].take()
    }
    // we own the value with a push
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        if self.is_full() {
            // println!("sorry no space left");
            return Err(value);
        }
        let index = (self.tail as usize) % self.capacity;
        self.data[index] = Option::Some(value);
        self.tail += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
     * Push until the buffer gets full
     */
    #[test]
    fn push_until_full() {
        const CAPACITY: usize = 10;
        let mut rb = self::RingBuffer::new(CAPACITY);
        // try to fill 12 items into a fixed size buffer
        for data in 1..13 {
            let result = rb.try_push(data);
            match result {
                Err(val) => {
                    assert_eq!(rb.is_full(), true);
                    assert_eq!(val, data)
                }
                Ok(()) => assert!(check(rb.capacity, data)),
            }
        }
        assert_eq!(rb.is_full(), true);
        assert_eq!(*rb.peek().unwrap(), 1);
    }

    fn check(capacity: usize, itr: usize) -> bool {
        capacity >= itr
    }

    #[test]
    fn pop_until_empty() {
        const CAPACITY: usize = 10;
        let mut rb = RingBuffer::new(CAPACITY);
        for i in 0..9 {
            let r = rb.try_push(i);
            match r {
                Err(val) => println!("couldn't push: {val}"),
                _ => (),
            }
        }

        for i in 0..9 {
            let popped = rb.pop();
            assert_eq!(popped.unwrap(), i);
        }
    }

    #[test]
    fn wrap_around() {
        let mut rb = RingBuffer::new(4);
        // [0, 1, 2, 0]
        //  ^H       ^T
        for i in 0..3 {
            rb.try_push(i);
        }
        // [0, 1, 2, 0]
        //        ^H ^T
        for _ in 0..2 {
            rb.pop();
        }
        // [1, 2, 2, 0]
        //     ^T ^H
        for i in 0..3 {
            let r = rb.try_push(i);
            match r {
                Err(val) => println!("unable to push {val}"),
                _ => (),
            }
        }
        //
        assert_eq!(rb.pop().unwrap(), 2);
        assert_eq!(rb.pop().unwrap(), 0);
        assert_eq!(rb.pop().unwrap(), 1);
        assert_eq!(rb.pop().unwrap(), 2);
        assert_eq!(rb.is_empty(), true);
    }

    #[test]
    fn test_front() {
        let mut rb = RingBuffer::new(3);
        rb.try_push(String::from("test1"));
        rb.try_push(String::from("test2"));
        rb.try_push(String::from("test3"));
        let b = rb.peek();
        let c = rb.peek();
        assert_eq!(b.unwrap(), c.unwrap());
    }
}
