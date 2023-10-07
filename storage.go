package mkv

import (
	"errors"
	"maps"
	"sync"
	"time"
)

const (
	copyFactor  = 10
	minDeletion = 10000
)

var keyNotExitError = errors.New("key is not exist")

type KVStorage[T any] struct {
	ttl time.Duration
	l   sync.RWMutex
	t   map[string]time.Time
	s   map[string]T
	d   int // deletion count
}

func (k *KVStorage[T]) Get(key string) (T, error) {
	k.l.RLock()
	defer k.l.RUnlock()
	v, ok := k.s[key]
	if ok && time.Now().Before(k.t[key]) {
		return v, nil
	}
	return *new(T), keyNotExitError
}

func (k *KVStorage[T]) Set(key string, v T) {
	k.l.Lock()
	defer k.l.Unlock()
	k.t[key] = time.Now().Add(k.ttl)
	k.s[key] = v
}

func (k *KVStorage[T]) delete(key string) {
	delete(k.t, key)
	delete(k.s, key)
	k.d++
}

func (k *KVStorage[T]) Delete(key string) {
	k.l.Lock()
	defer k.l.Unlock()
	if _, ok := k.t[key]; ok {
		k.delete(key)
	}
}

func (k *KVStorage[T]) SetIfNotExist(key string, v T) bool {
	if _, err := k.Get(key); err != nil {
		k.Set(key, v)
		return true
	}
	return false
}

func (k *KVStorage[T]) Keys() (keys []string) {
	k.l.RLock()
	defer k.l.RUnlock()
	for key, etime := range k.t {
		if time.Now().Before(etime) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (k *KVStorage[T]) clean() {
	active := time.NewTicker(min(time.Minute, k.ttl))
	defer active.Stop()
	for {
		<-active.C
		k.l.Lock()
		for key, etime := range k.t {
			if time.Now().After(etime) {
				k.delete(key)
			}
		}
		if k.d > minDeletion && len(k.t)*copyFactor < k.d {
			k.t = maps.Clone(k.t)
			k.s = maps.Clone(k.s)
			k.d = 0
		}
		k.l.Unlock()
	}
}

func NewKVStorage[T any](ttl time.Duration, t T) *KVStorage[T] {
	s := &KVStorage[T]{
		ttl: ttl,
		t:   make(map[string]time.Time),
		s:   make(map[string]T),
	}
	go s.clean()
	return s
}
