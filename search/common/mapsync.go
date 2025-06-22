package common

import (
	"maps"
	"sync"
)

type MapSync[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func NewMapSync[K comparable, V any](initial map[K]V) *MapSync[K, V] {
	m := initial
	if m == nil {
		m = make(map[K]V)
	}
	return &MapSync[K, V]{m: m}
}

// Get retrieves the value for a key, returns (zero, false) if not present.
func (ms *MapSync[K, V]) Get(key K) (V, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	v, ok := ms.m[key]
	return v, ok
}

// Set assigns a value to a key.
func (ms *MapSync[K, V]) Set(key K, val V) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = val
}

// Modify atomically updates the value for a key using the provided function.
// If the key does not exist, zero value is passed to fn, and the result is stored.
func (ms *MapSync[K, V]) Modify(key K, fn func(old V) V) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	old := ms.m[key]
	ms.m[key] = fn(old)
}

// Delete removes a key from the map.
func (ms *MapSync[K, V]) Delete(key K) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.m, key)
}

func (ms *MapSync[K, V]) DeleteBatch(keys ...K) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, key := range keys {
		delete(ms.m, key)
	}
}

// Range iterates over all entries. If callback returns false, iteration stops.
func (ms *MapSync[K, V]) Range(callback func(key K, val V) bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for k, v := range ms.m {
		if !callback(k, v) {
			break
		}
	}
}

// Snapshot returns a shallow copy of the map for safe iteration without locks.
func (ms *MapSync[K, V]) Snapshot() map[K]V {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	copy := make(map[K]V, len(ms.m))
	maps.Copy(copy, ms.m)
	return copy
}

// UnsafeGet retrieves a value without locking. Use only when externally synchronized.
func (ms *MapSync[K, V]) UnsafeGet(key K) (V, bool) {
	v, ok := ms.m[key]
	return v, ok
}

// UnsafeRaw returns the underlying map without any synchronization.
// Only use this if you are sure no other goroutines are reading/writing concurrently.
func (ms *MapSync[K, V]) UnsafeRaw() map[K]V {
	return ms.m
}

// RLock manually acquires a read lock on the internal mutex.
// You must call RUnlock() to release the lock after usage.
// This method is intended for advanced usage scenarios where you
// need to perform multiple read operations atomically.
func (ms *MapSync[K, V]) RLock() {
	ms.mu.RLock()
}

// RUnlock manually releases the read lock previously acquired with RLock().
// Failing to call this after RLock() will result in a deadlock.
// Always ensure RUnlock is deferred or explicitly called.
func (ms *MapSync[K, V]) RUnlock() {
	ms.mu.RUnlock()
}

// Lock manually acquires a write lock on the internal mutex.
// You must call Unlock() to release the lock after usage.
// This method is intended for advanced usage scenarios where you
// need to perform multiple write operations atomically or protect mutation logic.
func (ms *MapSync[K, V]) Lock() {
	ms.mu.Lock()
}

// Unlock manually releases the write lock previously acquired with Lock().
// Failing to call this after Lock() will result in a deadlock.
// Always ensure Unlock is deferred or explicitly called.
func (ms *MapSync[K, V]) Unlock() {
	ms.mu.Unlock()
}
