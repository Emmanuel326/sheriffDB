# SherifDB 🕵️

A Bitcask-inspired, append-only key-value storage engine in Go.  
Single file. Under 420 LOC. Bulletproof.

## Design

One append-only log. One in-memory index. Five guarantees:

- **Durability** — `fsync` after every write. No data loss on power cut.
- **Crash recovery** — CRC32 per record. Torn writes detected and truncated on startup.
- **Tombstone deletes** — Deletes are records, not erasures. Survives restart.
- **Single writer** — Lockfile with `O_EXCL`. Two processes cannot corrupt the log.
- **Fast restarts** — Hint file written on close. Index rebuilds in O(keys), not O(data).

## Binary Protocol

```
[ crc32(4) | timestamp(8) | kLen(4) | vLen(4) | key | value ]
```

A tombstone record has `vLen = 0xFFFFFFFF` and no value bytes.

## API

```go
db, err := Open("path/to/db")
defer db.Close()

db.Set([]byte("key"), []byte("value"))
val, err := db.Get([]byte("key"))
err = db.Delete([]byte("key"))
```

That's it.

## Usage

```go
import "github.com/Emmanuel326/sheriffDB"

db, err := sherifdb.Open("my.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

db.Set([]byte("emmanuel"), []byte("systems_engineer"))

val, err := db.Get([]byte("emmanuel"))
fmt.Println(string(val)) // systems_engineer

db.Delete([]byte("emmanuel"))
```

## Files on disk

```
my.db       → append-only data log
my.db.hint  → compact index snapshot (written on clean close)
my.db.lock  → exclusive lock (deleted on close)
```

## Stats

| Metric | Value |
|---|---|
| Engine LOC | 419 |
| Public API surface | 5 functions |
| Dependencies | stdlib only |

## License

MIT — Emmanuel326
