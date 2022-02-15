package mkv

import (
	"errors"
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

func (s *storage) clean() {
	for c := range s.cq {
		select {
		case <-time.After(c.etime.Sub(time.Now())):
			s.Delete(c.key)
		}
	}
}

func (s *storage) get(k string) (interface{}, error) {
	v, ok := s.storage.Load(k)
	if !ok {
		return nil, keyNotExitError
	}
	return v, nil
}

func (s *storage) Get(k string) (interface{}, error) {
	if v, err := s.get(k); err != nil {
		return nil, err
	} else {
		return v, nil
	}
}

func (s *storage) Delete(k string) {
	s.storage.Delete(k)
}

func (s *storage) addToClean(key string, etime time.Time) {
	s.cq <- clean{key: key, etime: etime}
}

func (s *storage) Set(k string, v interface{}) {
	s.storage.Store(k, v)
	go s.addToClean(k, time.Now().Add(s.ttl))
}

func (s *storage) SetWithExTime(k string, v interface{}, ttl time.Duration) {
	s.storage.Store(k, v)
	go s.addToClean(k, time.Now().Add(ttl))
}

func (s *storage) SetIfNotExist(k string, v interface{}) bool {
	_, loaded := s.storage.LoadOrStore(k, v)
	if !loaded {
		go s.addToClean(k, time.Now().Add(s.ttl))
	}
	return !loaded
}

func NewKVStorage(ttl time.Duration) KVStorage {
	s := &storage{ttl: ttl, cq: make(chan clean, 10000)}
	go s.clean()
	return s
}
