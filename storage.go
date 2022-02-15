package mkv

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	defaultTTL      = 5 * time.Minute
	keyNotExitError = errors.New("key is not exist")
)

var defaultStorage = NewKVStorage(defaultTTL)

type KVStorage interface {
	Get(key string) (interface{}, error)
	Delete(key string)
	Set(key string, value interface{})
	SetWithExTime(key string, value interface{}, ttl time.Duration)
	SetIfNotExist(key string, value interface{}) bool
}

type clean struct {
	key   string
	etime time.Time
}

type storage struct {
	storage sync.Map
	ttl     time.Duration
	cq      chan clean
}

var (
	version = map[string]int64{}
	rw      sync.RWMutex
)

func (s *storage) clean() {
	for c := range s.cq {
		select {
		case <-time.After(c.etime.Sub(time.Now())):
			s.Delete(c.key)
		}
	}
}

func (s *storage) keyForGet(k string) string {
	rw.RLock()
	defer rw.RUnlock()
	return fmt.Sprintf("%s-%d", k, version[k])
}

func (s *storage) keyForSet(k string) string {
	rw.Lock()
	defer rw.Unlock()
	version[k] = version[k] + 1
	return fmt.Sprintf("%s-%d", k, version[k])
}

func (s *storage) get(k string) (interface{}, error) {
	v, ok := s.storage.Load(s.keyForGet(k))
	if !ok {
		return nil, keyNotExitError
	}
	return v, nil
}

func (s *storage) Get(k string) (interface{}, error) {
	return s.get(k)
}

func (s *storage) Delete(k string) {
	s.storage.Delete(k)
}

func (s *storage) addToClean(key string, etime time.Time) {
	s.cq <- clean{key: key, etime: etime}
}

func (s *storage) set(k string, v interface{}) string {
	key := s.keyForSet(k)
	s.storage.Store(key, v)
	return key
}

func (s *storage) Set(k string, v interface{}) {
	key := s.set(k, v)
	go s.addToClean(key, time.Now().Add(s.ttl))
}

func (s *storage) SetWithExTime(k string, v interface{}, ttl time.Duration) {
	key := s.set(k, v)
	go s.addToClean(key, time.Now().Add(ttl))
}

func (s *storage) SetIfNotExist(k string, v interface{}) bool {
	if _, err := s.Get(k); err != nil {
		s.Set(k, v)
		return true
	} else {
		return false
	}
}

func NewKVStorage(ttl time.Duration) KVStorage {
	s := &storage{ttl: ttl, cq: make(chan clean, 10000)}
	go s.clean()
	return s
}
