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
	Keys() []string
}

type clean struct {
	verKey string
	oriKey string
	etime  time.Time
}

type storage struct {
	storage sync.Map
	ttl     time.Duration
	cq      chan clean
	version map[string]int64
	rw      sync.RWMutex
}

func (s *storage) clean() {
	for c := range s.cq {
		select {
		case <-time.After(c.etime.Sub(time.Now())):
			s.delete(c.oriKey, c.verKey)
		}
	}
}

func (s *storage) keyForGet(oriKey string) string {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return fmt.Sprintf("%s-%d", oriKey, s.version[oriKey])
}

func (s *storage) keyForSet(oriKey string) string {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.version[oriKey] = s.version[oriKey] + 1
	return fmt.Sprintf("%s-%d", oriKey, s.version[oriKey])
}

func (s *storage) get(oriKey string) (interface{}, error) {
	v, ok := s.storage.Load(s.keyForGet(oriKey))
	if !ok {
		return nil, keyNotExitError
	}
	return v, nil
}

func (s *storage) Get(oriKey string) (interface{}, error) {
	return s.get(oriKey)
}

func (s *storage) delete(oriKey, verKey string) {
	s.storage.Delete(verKey)
	s.deleteKey(oriKey, verKey)
}

func (s *storage) deleteKey(oriKey, verKey string) {
	if s.keyForGet(oriKey) == verKey {
		s.rw.Lock()
		defer s.rw.Unlock()
		delete(s.version, oriKey)
	}
}

func (s *storage) Delete(oriKey string) {
	s.delete(oriKey, s.keyForGet(oriKey))
}

func (s *storage) addToClean(oriKey, verKey string, etime time.Time) {
	s.cq <- clean{oriKey: oriKey, verKey: verKey, etime: etime}
}

func (s *storage) set(oriKey string, v interface{}) string {
	s.rw.RLock()
	ver := s.version[oriKey]
	s.rw.RUnlock()
	if ver != 0 {
		s.delete(oriKey, s.keyForGet(oriKey))
	}
	verKey := s.keyForSet(oriKey)
	s.storage.Store(verKey, v)
	return verKey
}

func (s *storage) Set(oriKey string, v interface{}) {
	verKey := s.set(oriKey, v)
	go s.addToClean(oriKey, verKey, time.Now().Add(s.ttl))
}

func (s *storage) SetWithExTime(oriKey string, v interface{}, ttl time.Duration) {
	verKey := s.set(oriKey, v)
	go s.addToClean(oriKey, verKey, time.Now().Add(ttl))
}

func (s *storage) SetIfNotExist(oriKey string, v interface{}) bool {
	if _, err := s.Get(oriKey); err != nil {
		s.Set(oriKey, v)
		return true
	} else {
		return false
	}
}

func (s *storage) Keys() []string {
	s.rw.RLock()
	defer s.rw.RUnlock()
	var keys []string
	for key := range s.version {
		keys = append(keys, key)
	}
	return keys
}

func NewKVStorage(ttl time.Duration) KVStorage {
	s := &storage{ttl: ttl, cq: make(chan clean, 10000), version: make(map[string]int64)}
	go s.clean()
	return s
}
