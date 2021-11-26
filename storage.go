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
	SetNX(key string) bool
}

type storage struct {
	storage sync.Map
	ttl     time.Duration
}

type value struct {
	data  interface{}
	ctime time.Time
}

func (s *storage) clean() {
	for {
		time.Sleep(s.ttl)
		s.storage.Range(func(k, v interface{}) bool {
			d := v.(value)
			if time.Now().Second()-d.ctime.Second() > int(s.ttl.Seconds()) {
				s.storage.Delete(k)
			}
			return true
		})
	}
}

func (s *storage) Get(k string) (interface{}, error) {
	v, ok := s.storage.Load(k)
	if !ok {
		return "", keyNotExitError
	}
	d := v.(value)
	return d.data, nil
}

func (s *storage) Delete(k string) {
	s.storage.Delete(k)
}

func (s *storage) Set(k string, v interface{}) {
	d := value{data: v, ctime: time.Now()}
	s.storage.Store(k, d)
}

func (s *storage) SetNX(k string) bool {
	d := value{data: "", ctime: time.Now()}
	_, loaded := s.storage.LoadOrStore(k, d)
	return !loaded
}

func NewKVStorage(ttl time.Duration) KVStorage {
	s := &storage{ttl: ttl}
	go s.clean()
	return s
}
