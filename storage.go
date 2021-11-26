package mkv

import (
	"errors"
	"sync"
	"time"
)

var (
	defaultTTL      = 5 * time.Minute
	keyNotExitError = errors.New("key is not exist")
	keyExpiredError = errors.New("key is expired")
)

var defaultStorage = NewKVStorage(defaultTTL)

type Storage interface {
	Get(key string) (string, error)
	Delete(key string)
	Set(key string, value string)
	SetNX(key string) bool
}

type storage struct {
	storage sync.Map
	ttl     time.Duration
}

type value struct {
	data    string
	setTime time.Time
}

func (s *storage) Get(k string) (string, error) {
	v, ok := s.storage.Load(k)
	if !ok {
		return "", keyNotExitError
	}
	d := v.(value)
	if time.Now().Second()-d.setTime.Second() > int(s.ttl.Seconds()) {
		s.Delete(k)
		return "", keyExpiredError
	}
	return d.data, nil
}

func (s *storage) Delete(k string) {
	s.storage.Delete(k)
}

func (s *storage) Set(k string, v string) {
	d := value{data: v, setTime: time.Now()}
	s.storage.Store(k, d)
}

func (s *storage) SetNX(k string) bool {
	d := value{data: "", setTime: time.Now()}
	_, loaded := s.storage.LoadOrStore(k, d)
	return !loaded
}

func NewKVStorage(ttl time.Duration) Storage {
	return &storage{ttl: ttl}
}
