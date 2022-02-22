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

func SetIfNotExist(k string, v interface{}) bool {
	return defaultStorage.SetIfNotExist(k, v)
}

func SetWithExTime(k string, v interface{}, ttl time.Duration) {
	defaultStorage.SetWithExTime(k, v, ttl)
}

func Keys() []string {
	return defaultStorage.Keys()
}
