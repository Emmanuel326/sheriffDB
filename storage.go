package main

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"runtime"
	"time"
)

const HeaderSize = 20 // 4+8+4+4

type Storage struct {
	file *os.File
	ring *RingBuffer
}

func (s *Storage) Write(key, value []byte) (int64, uint32, error) {
	// 1. Get a buffer from our Ring Buffer
	buf, err := s.ring.Pop()
	if err != nil {
		return 0, 0, err
	}
	defer s.ring.Push(buf)

	// 2. Calculate sizes and total length
	kLen := uint32(len(key))
	vLen := uint32(len(value))
	totalSize := HeaderSize + kLen + vLen

	// 3. Pin the buffer so the GC doesn't move it during the syscall
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	defer pinner.Unpin()

	// 4. Encode Header (Skip first 4 bytes for CRC)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint32(buf[12:16], kLen)
	binary.LittleEndian.PutUint32(buf[16:20], vLen)

	// 5. Copy Data
	copy(buf[20:], key)
	copy(buf[20+kLen:], value)

	// 6. Calculate CRC (Everything after the CRC field itself)
	checksum := crc32.ChecksumIEEE(buf[4:totalSize])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	// 7. Physical Append to Disk
	offset, err := s.file.Seek(0, 2)
	if err != nil {
		return 0, 0, err
	}

	_, err = s.file.Write(buf[:totalSize])
	return offset, totalSize, err
}

func (s *Storage) Restore(index *KeyDir) error {
	// Start from the beginning of the WAL
	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}

	for {
		header := make([]byte, HeaderSize)
		_, err := s.file.Read(header)
		if err != nil {
			// End of file reached
			break
		}

		// 1. Parse header to get metadata
		kLen := binary.LittleEndian.Uint32(header[12:16])
		vLen := binary.LittleEndian.Uint32(header[16:20])

		// 2. Calculate where this entry started
		currentPos, _ := s.file.Seek(0, 1)
		currentEntryOffset := currentPos - HeaderSize

		// 3. Read the Key (we need this to update the index)
		keyBuf := make([]byte, kLen)
		if _, err := s.file.Read(keyBuf); err != nil {
			break
		}

		// 4. Update the index with the latest version of this key
		index.Set(string(keyBuf), Meta{
			Offset: currentEntryOffset,
			Size:   HeaderSize + kLen + vLen,
		})

		// 5. Jump over the Value to get to the next Header
		if _, err := s.file.Seek(int64(vLen), 1); err != nil {
			break
		}
	}
	return nil
}
