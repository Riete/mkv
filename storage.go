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

type version struct {
	id       int64
	isDelete bool
}

func (v *version) newVersion() {
	v.id += 1
	v.isDelete = false
}

func (v *version) delete() {
	v.isDelete = true
}

type storage struct {
	storage sync.Map
	ttl     time.Duration
	cq      chan clean
	version map[string]version
	rw      sync.RWMutex
}

func (s *storage) clean() {
	etime := time.NewTimer(time.Second)
	ttl := time.NewTimer(time.Second)
	defer etime.Stop()
	defer ttl.Stop()

	for c := range s.cq {
		etime.Reset(time.Until(c.etime))
		ttl.Reset(s.ttl)

		select {
		case <-etime.C:
			s.delete(c.verKey)
			s.deleteKey(c.oriKey, c.verKey)
		case <-ttl.C:
			s.cq <- c
		}
	}
}

func (s *storage) keyForGet(oriKey string) string {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return fmt.Sprintf("%s-%d", oriKey, s.version[oriKey].id)
}

func (s *storage) keyForSet(oriKey string) string {
	s.rw.Lock()
	defer s.rw.Unlock()
	v := s.version[oriKey]
	v.newVersion()
	s.version[oriKey] = v
	return fmt.Sprintf("%s-%d", oriKey, s.version[oriKey].id)
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

func (s *storage) delete(verKey string) {
	s.storage.Delete(verKey)
}

func (s *storage) deleteKey(oriKey, verKey string) {
	if s.keyForGet(oriKey) == verKey {
		s.rw.Lock()
		defer s.rw.Unlock()
		delete(s.version, oriKey)
	}
}

func (s *storage) Delete(oriKey string) {
	s.delete(s.keyForGet(oriKey))
	s.rw.Lock()
	defer s.rw.Unlock()
	v := s.version[oriKey]
	v.delete()
	s.version[oriKey] = v
}

func (s *storage) addToClean(oriKey, verKey string, etime time.Time) {
	s.cq <- clean{oriKey: oriKey, verKey: verKey, etime: etime}
}

func (s *storage) set(oriKey, verKey string, v interface{}, ttl time.Duration) {
	go s.addToClean(oriKey, verKey, time.Now().Add(ttl))
	s.rw.RLock()
	defer s.rw.RUnlock()
	ver := s.version[oriKey]
	if ver.id > 1 {
		s.delete(fmt.Sprintf("%s-%d", oriKey, ver.id-1))
	}
	s.storage.Store(verKey, v)
}

func (s *storage) Set(oriKey string, v interface{}) {
	s.set(oriKey, s.keyForSet(oriKey), v, s.ttl)
}

func (s *storage) SetWithExTime(oriKey string, v interface{}, ttl time.Duration) {
	s.set(oriKey, s.keyForSet(oriKey), v, ttl)
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
	for key, value := range s.version {
		if !value.isDelete {
			keys = append(keys, key)
		}
	}
	return keys
}

func NewKVStorage(ttl time.Duration) KVStorage {
	s := &storage{ttl: ttl, cq: make(chan clean, 10000), version: make(map[string]version)}
	go s.clean()
	return s
}
