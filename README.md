# SherifDB 🕵️‍♂️

A high-performance, log-structured key-value engine written in Go, focused on "Mechanical Sympathy" and systems-level determinism.

## Design Philosophy
SherifDB is built with a **Tiger Style** engineering approach:
- **Sequential I/O:** All writes are appends to a Write-Ahead Log (WAL), maximizing throughput for NVMe/SSD.
- **Lock-Free Memory Management:** Utilizes an atomic circular Ring Buffer to recycle byte slices, eliminating GC thrashing during high-frequency writes.
- **Kernel-Safe Interaction:** Uses `runtime.Pinner` to ensure Go-allocated memory remains stationary during syscalls.
- **Deterministic Ordering:** Implements a strict binary protocol with CRC32 checksums for crash-consistency.

## Architecture
SherifDB consists of three primary layers:
1. **KeyDir (In-Memory Index):** An $O(1)$ hash map storing the offset and size of the latest value on disk.
2. **Storage Engine:** Handles binary serialization and raw file I/O using pre-allocated buffers.
3. **Ring Buffer:** A pre-allocated arena that manages memory without relying on the runtime's Garbage Collector.



## Binary Protocol
Every entry on disk follows a fixed 44-byte overhead format (excluding payload):

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| 0 | 4 | CRC32 | IEEE Checksum of the record |
| 4 | 8 | Timestamp | UnixNano (or Atomic Version) |
| 12 | 4 | KeySize | Length of the key |
| 16 | 4 | ValSize | Length of the value |
| 20 | N | Key | Raw key bytes |
| 20+N | M | Value | Raw value bytes |

## Performance Goals
- **Target:** Sub-millisecond write latency.
- **Footprint:** < 1,000 LOC.
- **Memory:** Fixed-size buffer pool to prevent memory leaks in long-running processes.

## Usage
```go
// Initialize components
rb := NewRingBuffer(1024, 4096)
index := NewKeyDir()
storage := &Storage{file: f, ring: rb}

// Perform a write
offset, size, err := storage.Write([]byte("key"), []byte("value"))
index.Set("key", Meta{Offset: offset, Size: size})


License
MIT - Developed by Emmanuel326

