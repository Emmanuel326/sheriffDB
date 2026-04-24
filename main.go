package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	// 1. Initialize our Ring Buffer (1024 slots of 4KB each) 4mb total
	rb := NewRingBuffer(1024, 4096)

	// 2. Open the WAL file
	f, err := os.OpenFile("sherif.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// 3. Initialize Storage and Index
	storage := &Storage{file: f, ring: rb}
	index := NewKeyDir()

	// 4. Recovery: Rebuild index from disk
	fmt.Println("SherifDB: Restoring index...")
	if err := storage.Restore(index); err != nil {
		log.Printf("Restore warning: %v", err)
	}

	// 5. Test Write
	key := []byte("emmanuel")
	val := []byte("systems_engineer")

	offset, size, err := storage.Write(key, val)
	if err != nil {
		log.Fatal(err)
	}

	// Update the index
	index.Set(string(key), Meta{Offset: offset, Size: size})

	fmt.Printf(" Saved '%s' at offset %d (Size: %d bytes)\n", key, offset, size)

	// 6. Test Read
	meta, ok := index.Get("emmanuel")
	if ok {
		fmt.Printf("🔍 Found key in index! Ready to ReadAt offset %d\n", meta.Offset)
	}
}
