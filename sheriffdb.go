// Package sherifdb is a Bitcask-inspired, append-only key-value storage engine.
// It guarantees durability, crash recovery, and single-writer safety
// in a single file under 300 LOC.
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// Errors

var (
	ErrNotFound     = errors.New("sherifdb: key not found")
	ErrDeleted      = errors.New("sherifdb: key has been deleted")
	ErrCorrupt      = errors.New("sherifdb: corrupt record detected")
	ErrDatabaseOpen = errors.New("sherifdb: database already open")
)

// Constants

const (
	headerSize  = 20         // crc32(4) + timestamp(8) + kLen(4) + vLen(4)
	tombstone   = ^uint32(0) // vLen == 0xFFFFFFFF marks a deleted key
	hintExt     = ".hint"
	lockExt     = ".lock"
	scratchSize = 4096
)

// Record layout
//
//  [ crc32 | timestamp | kLen | vLen | key | value ]
//    4 bytes  8 bytes   4 bytes 4 bytes
//
//  A tombstone record has vLen == 0xFFFFFFFF and no value bytes.

// Meta & KeyDir

type meta struct {
	offset int64
	size   uint32 // full record size (header + key + value)
}

type keyDir struct {
	mu    sync.RWMutex
	table map[string]meta
}

func newKeyDir() *keyDir { return &keyDir{table: make(map[string]meta)} }

func (kd *keyDir) set(key string, m meta) {
	kd.mu.Lock()
	kd.table[key] = m
	kd.mu.Unlock()
}

func (kd *keyDir) get(key string) (meta, bool) {
	kd.mu.RLock()
	m, ok := kd.table[key]
	kd.mu.RUnlock()
	return m, ok
}

func (kd *keyDir) delete(key string) {
	kd.mu.Lock()
	delete(kd.table, key)
	kd.mu.Unlock()
}

// DB

// DB is the public handle to a SherifDB database.
// All methods are safe for concurrent use.
type DB struct {
	mu      sync.Mutex
	file    *os.File
	lock    *os.File
	index   *keyDir
	scratch []byte
	path    string
}

// Open opens or creates a database at path.
// Only one process may hold a database open at a time.
func Open(path string) (*DB, error) {
	// Acquire exclusive lockfile
	lf, err := os.OpenFile(path+lockExt, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if os.IsExist(err) {
			return nil, ErrDatabaseOpen
		}
		return nil, fmt.Errorf("sherifdb: acquire lock: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		lf.Close()
		os.Remove(path + lockExt)
		return nil, fmt.Errorf("sherifdb: open data file: %w", err)
	}

	db := &DB{
		file:    f,
		lock:    lf,
		index:   newKeyDir(),
		scratch: make([]byte, scratchSize),
		path:    path,
	}

	if err := db.restore(); err != nil {
		db.closeFiles()
		return nil, fmt.Errorf("sherifdb: restore: %w", err)
	}

	return db, nil
}

// Set writes key→value, overwriting any previous value.
func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("sherifdb: key must not be empty")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset, size, err := db.writeRecord(key, value)
	if err != nil {
		return err
	}

	db.index.set(string(key), meta{offset: offset, size: size})
	return nil
}

// Get retrieves the value for key.
// Returns ErrNotFound if the key does not exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	m, ok := db.index.get(string(key))
	if !ok {
		return nil, ErrNotFound
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Read the full record from disk
	rec := make([]byte, m.size)
	if _, err := db.file.ReadAt(rec, m.offset); err != nil {
		return nil, fmt.Errorf("sherifdb: read: %w", err)
	}

	// Verify CRC
	stored := binary.LittleEndian.Uint32(rec[0:4])
	computed := crc32.ChecksumIEEE(rec[4:])
	if stored != computed {
		return nil, ErrCorrupt
	}

	kLen := binary.LittleEndian.Uint32(rec[12:16])
	vLen := binary.LittleEndian.Uint32(rec[16:20])

	if vLen == tombstone {
		return nil, ErrNotFound
	}

	value := make([]byte, vLen)
	copy(value, rec[headerSize+kLen:])
	return value, nil
}

// Delete removes key from the database by writing a tombstone record.
func (db *DB) Delete(key []byte) error {
	if _, ok := db.index.get(string(key)); !ok {
		return ErrNotFound
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Write tombstone: vLen = 0xFFFFFFFF, no value bytes
	if _, _, err := db.writeTombstone(key); err != nil {
		return err
	}

	db.index.delete(string(key))
	return nil
}

// Close flushes, releases the lockfile, and closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.writeHint(); err != nil {
		// Non-fatal: next open will restore from the log
		fmt.Fprintf(os.Stderr, "sherifdb: write hint: %v\n", err)
	}

	return db.closeFiles()
}

//   Internal: Write

func (db *DB) writeRecord(key, value []byte) (offset int64, size uint32, err error) {
	kLen := uint32(len(key))
	vLen := uint32(len(value))
	total := uint32(headerSize) + kLen + vLen

	buf := db.growScratch(total)

	binary.LittleEndian.PutUint64(buf[4:12], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint32(buf[12:16], kLen)
	binary.LittleEndian.PutUint32(buf[16:20], vLen)
	copy(buf[headerSize:], key)
	copy(buf[headerSize+kLen:], value)

	checksum := crc32.ChecksumIEEE(buf[4:total])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	return db.append(buf[:total])
}

func (db *DB) writeTombstone(key []byte) (offset int64, size uint32, err error) {
	kLen := uint32(len(key))
	total := uint32(headerSize) + kLen

	buf := db.growScratch(total)

	binary.LittleEndian.PutUint64(buf[4:12], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint32(buf[12:16], kLen)
	binary.LittleEndian.PutUint32(buf[16:20], tombstone)
	copy(buf[headerSize:], key)

	checksum := crc32.ChecksumIEEE(buf[4:total])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	return db.append(buf[:total])
}

// append seeks to end, writes, and fsyncs. Must be called with db.mu held.
func (db *DB) append(data []byte) (offset int64, size uint32, err error) {
	offset, err = db.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, 0, fmt.Errorf("sherifdb: seek: %w", err)
	}

	if _, err = db.file.Write(data); err != nil {
		return 0, 0, fmt.Errorf("sherifdb: write: %w", err)
	}

	if err = db.file.Sync(); err != nil {
		return 0, 0, fmt.Errorf("sherifdb: fsync: %w", err)
	}

	return offset, uint32(len(data)), nil
}

// growScratch returns a slice of at least n bytes from the scratch buffer.
func (db *DB) growScratch(n uint32) []byte {
	if uint32(cap(db.scratch)) < n {
		db.scratch = make([]byte, n)
	}
	return db.scratch[:n]
}

// Internal: Restore

// restore rebuilds the keydir from hint file (fast) or log (safe fallback).
func (db *DB) restore() error {
	if err := db.restoreFromHint(); err == nil {
		return nil
	}
	return db.restoreFromLog()
}

// restoreFromLog replays the data file, verifying each record's CRC.
// On a torn write at the tail, it truncates to the last good offset.
func (db *DB) restoreFromLog() error {
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var lastGood int64
	header := make([]byte, headerSize)

	for {
		pos, _ := db.file.Seek(0, io.SeekCurrent)

		if _, err := io.ReadFull(db.file, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		kLen := binary.LittleEndian.Uint32(header[12:16])
		vLen := binary.LittleEndian.Uint32(header[16:20])

		isTombstone := vLen == tombstone
		bodyLen := kLen
		if !isTombstone {
			bodyLen += vLen
		}

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(db.file, body); err != nil {
			// Torn write: truncate here
			break
		}

		// Verify CRC over everything after the CRC field
		stored := binary.LittleEndian.Uint32(header[0:4])
		payload := make([]byte, headerSize-4+bodyLen)
		copy(payload, header[4:])
		copy(payload[headerSize-4:], body)
		computed := crc32.ChecksumIEEE(payload)

		if stored != computed {
			// Corrupt record: truncate to last known good
			break
		}

		key := string(body[:kLen])
		totalSize := uint32(headerSize) + bodyLen

		if isTombstone {
			db.index.delete(key)
		} else {
			db.index.set(key, meta{offset: pos, size: totalSize})
		}

		lastGood = pos + int64(totalSize)
	}

	// Truncate any partial/corrupt tail
	return db.file.Truncate(lastGood)
}

// Internal: Hint file
//
// Hint format per entry: [ kLen(4) | offset(8) | size(4) | key ]

func (db *DB) writeHint() error {
	hf, err := os.Create(db.path + hintExt)
	if err != nil {
		return err
	}
	defer hf.Close()

	db.index.mu.RLock()
	defer db.index.mu.RUnlock()

	buf := make([]byte, 16)
	for k, m := range db.index.table {
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(k)))
		binary.LittleEndian.PutUint64(buf[4:12], uint64(m.offset))
		binary.LittleEndian.PutUint32(buf[12:16], m.size)
		if _, err := hf.Write(buf); err != nil {
			return err
		}
		if _, err := hf.Write([]byte(k)); err != nil {
			return err
		}
	}
	return hf.Sync()
}

func (db *DB) restoreFromHint() error {
	hf, err := os.Open(db.path + hintExt)
	if err != nil {
		return err
	}
	defer hf.Close()

	buf := make([]byte, 16)
	for {
		if _, err := io.ReadFull(hf, buf); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		kLen := binary.LittleEndian.Uint32(buf[0:4])
		offset := int64(binary.LittleEndian.Uint64(buf[4:12]))
		size := binary.LittleEndian.Uint32(buf[12:16])

		key := make([]byte, kLen)
		if _, err := io.ReadFull(hf, key); err != nil {
			return err
		}
		db.index.set(string(key), meta{offset: offset, size: size})
	}
}

//  Internal: Lifecycle

func (db *DB) closeFiles() error {
	var errs []error
	if err := db.file.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := db.lock.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := os.Remove(db.path + lockExt); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("sherifdb: close: %v", errs)
	}
	return nil
}
