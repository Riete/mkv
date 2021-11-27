package mkv

import (
	"time"
)

func Get(k string) (interface{}, error) {
	return defaultStorage.Get(k)
}

func Delete(k string) {
	defaultStorage.Delete(k)
}

func Set(k string, v interface{}) {
	defaultStorage.Set(k, v)
}

func SetNX(k string) bool {
	return defaultStorage.SetNX(k)
}

func SetEX(k string, v interface{}, ttl time.Duration) {
	defaultStorage.SetEX(k, v, ttl)
}

func TTL(k string) (time.Duration, error) {
	return defaultStorage.TTL(k)
}

func IncTTL(k string, ttl time.Duration) error {
	return defaultStorage.IncTTL(k, ttl)
}

func DecTTL(k string, ttl time.Duration) error {
	return defaultStorage.DecTTL(k, ttl)
}
