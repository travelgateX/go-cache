package cache

type err string

func (e err) Error() string { return string(e) }

// ErrTimeout is the value returned by Gets to cache with a given timeout
const ErrTimeout = err("timeout")

// ErrWrongMFetchResult happens when a MFetchFunc doesn't return results for every value requested
const ErrWrongMFetchResult = err("MFetchFunc error: keySuffixes and values returned doesn't have the same length")
