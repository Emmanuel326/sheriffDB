package main

import "sync"

type Meta struct {
	Offset int64
	Size   uint32
}

type KeyDir struct {
	mu    sync.RWMutex
	table map[string]Meta
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		table: make(map[string]Meta),
	}
}

func (kd *KeyDir) Set(key string, m Meta) {
	kd.mu.Lock()
	kd.table[key] = m
	kd.mu.Unlock()
}

func (kd *KeyDir) Get(key string) (Meta, bool) {
	kd.mu.RLock()
	m, ok := kd.table[key]
	kd.mu.RUnlock()
	return m, ok
}
