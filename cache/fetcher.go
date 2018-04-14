package cache

import (
	"errors"
	"sync/atomic"
	"time"

	hclru "github.com/hashicorp/golang-lru"
	"golang.org/x/sync/singleflight"
)

// FetcherLRU wraps https://github.com/hashicorp/golang-lru providing ttl, stats and singleflight
//
// Satisfies an use case where the cache is filled by demand: the gets are done by key and an OnFetch func
// that is executed when the key is not found or has expired, the OnFetch Result will be stored in cache
// for that key. Only one call to OnFetch is done at the same time
//
// Gets on expired keys cause an update but are non-blocking, the old value is returned
// Gets on missing keys are blocked until the OnFetch finishes
type FetcherLRU struct {
	cache *hclru.Cache
	ttl   time.Duration
	group singleflight.Group
	stats Stats
}

// Stats stores cache statistics
type Stats struct {
	Hits      int64
	Misses    int64
	Evictions int64
}

// New creates a new instance of FetcherLRU
func New(size int, ttl time.Duration) (*FetcherLRU, error) {
	return NewWithEvict(size, ttl, nil)
}

// EvictFunc is a callback func executed on an item eviction
type EvictFunc func(key interface{}, value interface{})

// NewWithEvict creates a new instance of FetcherLRU with the given eviction
// callback.
func NewWithEvict(size int, ttl time.Duration, onEvicted EvictFunc) (*FetcherLRU, error) {
	if size <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if ttl < 0 {
		return nil, errors.New("must provide a non negative ttl")
	}
	lru, err := hclru.NewWithEvict(size, onEvicted)
	if err != nil {
		return nil, err
	}
	return &FetcherLRU{cache: lru, ttl: ttl}, nil
}

// FetchFunc result is stored in cache, if value is nil, it will be considered as empty and
// an empty item will be stored. When err is non-nil, the cache won't store anything and the
// error will be scalated to the function calling fetch
type FetchFunc func() (value interface{}, err error)

// GetOrFetch looks up a key's value from the cache, returns nil as the value
// to represent cached nil values
func (c *FetcherLRU) GetOrFetch(key string, onFetch FetchFunc) (interface{}, error) {
	var i *item
	var ok bool
	if i, ok = c.get(key); ok {
		// key found in cache
		atomic.AddInt64(&c.stats.Hits, 1)
		// check and handle expiration, if the item is already in process
		// to be updated, just return the value obtained from cache despite being old
		if !i.updating && i.Expired() {
			i.Lock()
			// only the first thread has to request a fetch for an update; a double lock
			// is used to ensure it
			if !i.updating && i.Expired() {
				i.updating = true
				go func() {
					_, err := c.fetch(key, onFetch)
					if err != nil {
						// set loading to false only in case where the fetch failed,
						// the next look up will retry the fetch. Note that the expired item
						// is not deleted
						i.updating = false
					}
				}()
			}
			i.Unlock()
		}
	} else {
		// key not found
		atomic.AddInt64(&c.stats.Misses, 1)
		// group call per key
		v, err, _ := c.group.Do(key, func() (interface{}, error) {
			// double check
			if i, ok := c.peek(key); ok {
				return i, nil
			}
			return c.fetch(key, onFetch)
		})
		if err != nil {
			// err is the same produced by the OnFetch func
			return nil, err
		}
		// v will always be an item if not error
		i = v.(*item)
	}
	if i.IsEmpty() {
		return nil, nil
	}
	return i.value, nil
}

// fetches a key's value and puts it on cache
// in case the Item is not found, an empty Item is put in cache
func (c *FetcherLRU) fetch(key string, onFetch FetchFunc) (*item, error) {
	v, err := onFetch()
	if err != nil {
		return nil, err
	}
	var i *item
	if v == nil {
		i = newEmptyItem(c.ttl)
	} else {
		i = newItem(v, c.ttl)
	}
	eviction := c.cache.Add(key, i)
	if eviction {
		atomic.AddInt64(&c.stats.Evictions, 1)
	}
	return i, nil
}

// get looks up a key's value from the cache updating its recent-ness
func (c *FetcherLRU) get(key string) (*item, bool) {
	return gassert(key, c.cache.Get)
}

// peek looks up a key's value from the cache without updating its recent-ness
func (c *FetcherLRU) peek(key string) (*item, bool) {
	return gassert(key, c.cache.Peek)
}

// gassert is a helper method for those getters that returns an interface value and requires
// a type assertion to item
func gassert(key string, f func(key interface{}) (value interface{}, ok bool)) (*item, bool) {
	v, ok := f(key)
	if !ok {
		return nil, false
	}
	if i, ok := v.(*item); ok {
		return i, true
	}
	return nil, false
}

// Result holds the results of Get, so they can be passed
// on a channel.
type Result struct {
	Val interface{}
	Err error
}

// GetChan is the same as GetOrFetch but returning a channel
// with the result when its ready
func (c *FetcherLRU) GetChan(key string, onFetch FetchFunc) <-chan Result {
	ch := make(chan Result, 1)
	go func() {
		r := Result{}
		r.Val, r.Err = c.GetOrFetch(key, onFetch)
		ch <- r
		close(ch)
	}()
	return ch
}

type err string

func (e err) Error() string { return string(e) }

// ErrTimeout is the value returned by GetWithTimeout when the onFetch func lasts more
// than a given duration, the OnFetch func is not cancelled.
const ErrTimeout = err("timeout")

// GetWithTimeout is the same as GetOrFetch but returns a timeout
// error if the result is not ready on a given duration
func (c *FetcherLRU) GetWithTimeout(key string, onFetch FetchFunc, timeout time.Duration) (interface{}, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-c.GetChan(key, onFetch):
		return r.Val, r.Err
	case <-timer.C:
	}
	return nil, ErrTimeout
}

// Stats returns cache's Statistics
func (c *FetcherLRU) Stats() Stats {
	return c.stats
}

// Add forces an addition for a key's value
func (c *FetcherLRU) Add(key string, value interface{}) {
	var i *item
	if value == nil {
		i = newEmptyItem(c.ttl)
	} else {
		i = newItem(value, c.ttl)
	}
	evicted := c.cache.Add(key, i)
	c.group.Forget(key)
	if evicted {
		atomic.AddInt64(&c.stats.Evictions, 1)
	}
}

// GetWithoutSingleFlight for those cases where the group lock is not needed
func (c *FetcherLRU) GetWithoutSingleFlight(key string, onFetch FetchFunc) (interface{}, error) {
	var i *item
	var ok bool
	if i, ok = c.get(key); ok {
		atomic.AddInt64(&c.stats.Hits, 1)
		// check and handle expiration
		if !i.updating && i.Expired() {
			i.Lock()
			// only the first thread has to request a fetch for an update
			if !i.updating && i.Expired() {
				i.updating = true
				go func() {
					_, err := c.fetch(key, onFetch)
					if err != nil {
						// set loading to false only in case that the fetch failed,
						// the rest of threads with the old Item doesn't need to request for an update
						i.updating = false
					}
				}()
			}
			i.Unlock()
		}
	} else {
		atomic.AddInt64(&c.stats.Misses, 1)
		var err error
		i, err = c.fetch(key, onFetch)
		if err != nil {
			// err is the same produced by fetch
			return nil, err
		}
	}
	if i.IsEmpty() {
		return nil, nil
	}
	return i.value, nil
}
