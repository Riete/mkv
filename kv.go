package mkv

func Get(k string) (string, error) {
	return defaultStorage.Get(k)
}

func Delete(k string) {
	defaultStorage.Delete(k)
}

func Set(k string, v string) {
	defaultStorage.Set(k, v)
}

func SetNX(k string) bool {
	return defaultStorage.SetNX(k)
}
