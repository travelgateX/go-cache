package cache

type err string

func (e err) Error() string { return string(e) }

// ErrTimeout is the value returned by Gets to cache with a given timeout
const ErrTimeout = err("timeout")
