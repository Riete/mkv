package mkv

import (
	"errors"
	"sync"
	"time"
)

var (
	defaultTTL      = 5 * time.Minute
	infTTL          = 9999 * time.Minute
	keyNotExitError = errors.New("key is not exist")
)

var defaultStorage = NewKVStorage(defaultTTL)

type KVStorage interface {
	Get(key string) (interface{}, error)
	Delete(key string)
	Set(key string, value interface{})
	SetEX(key string, value interface{}, ttl time.Duration)
	SetNX(key string) bool
	TTL(key string) (time.Duration, error)
	IncTTL(key string, ttl time.Duration) error
	DecTTL(key string, ttl time.Duration) error
}

type storage struct {
	storage sync.Map
	ttl     time.Duration
}

type value struct {
	data  interface{}
	etime time.Time
}

func (s *storage) clean() {
	for {
		time.Sleep(s.ttl)
		s.storage.Range(func(k, v interface{}) bool {
			d := v.(value)
			if time.Now().After(d.etime) {
				s.storage.Delete(k)
			}
			return true
		})
	}
}

func (s *storage) get(k string) (*value, error) {
	v, ok := s.storage.Load(k)
	if !ok {
		return nil, keyNotExitError
	}
	d := v.(*value)
	if time.Now().After(d.etime) {
		s.storage.Delete(k)
		return nil, keyNotExitError
	}
	return d, nil
}

func (s *storage) Get(k string) (interface{}, error) {
	if v, err := s.get(k); err != nil {
		return nil, err
	} else {
		return v.data, nil
	}
}

func (s *storage) Delete(k string) {
	s.storage.Delete(k)
}

func (s *storage) Set(k string, v interface{}) {
	d := &value{data: v, etime: time.Now().Add(defaultTTL)}
	s.storage.Store(k, d)
}

func (s *storage) SetEX(k string, v interface{}, ttl time.Duration) {
	d := &value{data: v, etime: time.Now().Add(ttl)}
	s.storage.Store(k, d)
}

func (s *storage) SetNX(k string) bool {
	d := &value{data: "", etime: time.Now().Add(infTTL)}
	_, loaded := s.storage.LoadOrStore(k, d)
	return !loaded
}

func (s *storage) TTL(k string) (time.Duration, error) {
	if v, err := s.get(k); err != nil {
		return 0, err
	} else {
		return v.etime.Sub(time.Now()), nil
	}
}

func (s *storage) adjTTL(k string, ttl time.Duration) error {
	if v, err := s.get(k); err != nil {
		return err
	} else {
		v.etime = v.etime.Add(ttl)
		s.storage.Store(k, v)
		return nil
	}
}

func (s *storage) IncTTL(k string, ttl time.Duration) error {
	return s.adjTTL(k, ttl)
}

func (s *storage) DecTTL(k string, ttl time.Duration) error {
	return s.adjTTL(k, -ttl)
}

func NewKVStorage(ttl time.Duration) KVStorage {
	s := &storage{ttl: ttl}
	go s.clean()
	return s
}
