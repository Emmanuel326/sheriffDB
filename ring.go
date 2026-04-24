package main

import (
	"errors"
	"sync/atomic"
)

var (
	ErrBufferFull  = errors.New("ring buffer is full")
	ErrBufferEmpty = errors.New("ring buffer is empty")
)

type RingBuffer struct {
	storage [][]byte
	size    uint32
	head    uint32 // Read from here
	tail    uint32 // Write to here
}

// NewRingBuffer creates a fixed-size pool of memory.
// capacity: how many buffers we want.
// bufferSize: the size of each buffer (e.g., 4096 for page alignment).
func NewRingBuffer(capacity int, bufferSize int) *RingBuffer {
	rb := &RingBuffer{
		storage: make([][]byte, capacity),
		size:    uint32(capacity),
		head:    0,
		tail:    uint32(capacity),
	}
	for i := range rb.storage {
		rb.storage[i] = make([]byte, bufferSize)
	}
	return rb
}

// Pop grabs a free buffer for the storage engine to use.
func (rb *RingBuffer) Pop() ([]byte, error) {
	h := atomic.LoadUint32(&rb.head)
	t := atomic.LoadUint32(&rb.tail)

	// If head == tail, the buffer is empty
	if h == t {
		return nil, ErrBufferEmpty
	}

	buf := rb.storage[h%rb.size]

	// Increment head atomically to "claim" the buffer
	if atomic.CompareAndSwapUint32(&rb.head, h, h+1) {
		return buf, nil
	}

	// If CAS fails, another goroutine beat us to it; try again
	return rb.Pop()
}

// Push returns a buffer to the ring once the I/O is finished.
func (rb *RingBuffer) Push(b []byte) error {
	h := atomic.LoadUint32(&rb.head)
	t := atomic.LoadUint32(&rb.tail)

	// Check if full (tail caught up to head)
	if t-h == rb.size {
		return ErrBufferFull
	}

	rb.storage[t%rb.size] = b

	// Increment tail atomically
	if atomic.CompareAndSwapUint32(&rb.tail, t, t+1) {
		return nil
	}

	return rb.Push(b)
}
