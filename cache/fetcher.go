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
// Gets on missing keys are blocked until the OnFetch finishes
type FetcherLRU struct {
	cache                    *hclru.Cache
	ttl                      time.Duration
	group                    singleflight.Group
	stats                    StatsFetcherLRU
	blockOnUpdatingGoroutine bool
}

// New creates a new instance of FetcherLRU
func New(size int, ttl time.Duration, options ...FetcherLRUOption) (*FetcherLRU, error) {
	if size <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if ttl < 0 {
		return nil, errors.New("must provide a non negative ttl")
	}

	opts := applyOptions(options...)

	lru, err := hclru.NewWithEvict(size, opts.evictFunc)
	if err != nil {
		return nil, err
	}
	f := &FetcherLRU{cache: lru, ttl: ttl}
	if err != nil {
		return nil, err
	}
	f.blockOnUpdatingGoroutine = opts.blockOnUpdatingGoroutine

	return f, nil
}

func applyOptions(opts ...FetcherLRUOption) FetcherLRUOptions {
	options := defaultCacheOptions
	for _, o := range opts {
		o(&options)
	}
	return options
}

// FetcherLRUOptions are optional paratemer for a customized FetcherLRU instance
type FetcherLRUOptions struct {
	// evictFunc is a callback func executed on an item eviction
	// nil by default
	evictFunc func(key interface{}, value interface{})
	// blockOnUpdatingGoroutine makes the calling goroutine to block until the new value is fetched
	// false by default
	blockOnUpdatingGoroutine bool
}

// FetcherLRUOption is a function that sets some option on the FetcherLRU.
type FetcherLRUOption func(*FetcherLRUOptions)

// SetEvictFunc sets a callback func executed on an item eviction
func SetEvictFunc(evictFunc func(key interface{}, value interface{})) FetcherLRUOption {
	return func(o *FetcherLRUOptions) {
		o.evictFunc = evictFunc
	}
}

// SetBlockOnUpdatingGoroutine makes the calling goroutine to block until the new value is fetched
func SetBlockOnUpdatingGoroutine() FetcherLRUOption {
	return func(o *FetcherLRUOptions) {
		o.blockOnUpdatingGoroutine = true
	}
}

var defaultCacheOptions = FetcherLRUOptions{
	evictFunc:                nil,
	blockOnUpdatingGoroutine: false,
}

// FetchFunc result is stored in cache, if value is nil, it will be considered as empty and
// an empty item will be stored. When err is non-nil, the cache won't store anything and the
// error will be scalated to the function calling fetch
type FetchFunc func() (value interface{}, err error)

// GetOrFetch gets the cache's value for key or the onFetch's value result
func (c *FetcherLRU) GetOrFetch(key string, onFetch FetchFunc) (interface{}, error) {
	i, err := c.getOrFetchItem(key, onFetch)
	if err != nil {
		return nil, err
	}
	if i.IsEmpty() {
		return nil, nil
	}
	return i.value, nil
}

func (c *FetcherLRU) getOrFetchItem(key string, onFetch FetchFunc) (*item, error) {
	if i, ok := c.get(key); ok {
		i = c.handleHit(key, onFetch, i)
		return i, nil
	}
	return c.handleMiss(key, onFetch)
}

// handleHit takes the item found in cache and handles its expiration,
// the returned item can be the one found in cache or an updated one caused by expiration
func (c *FetcherLRU) handleHit(key string, onFetch FetchFunc, i *item) *item {
	atomic.AddInt64(&c.stats.Hits, 1)
	if i.updating || !i.Expired() {
		return i
	}

	i.Lock()
	var tmpi *item
	// double lock check
	if !i.updating {
		// tmpi can be an updated item or the same as 'i' depending on the cache configuration
		tmpi = c.handleExpiration(key, onFetch, i)
	}
	i.Unlock()
	i = tmpi

	return i
}

// check and handle expiration, only the first routine will update the key;
// there is two type of update, force the first routine return the updated value, or return the
// expired key while updates is done in background
func (c *FetcherLRU) handleExpiration(key string, onFetch FetchFunc, i *item) *item {
	i.updating = true
	if c.blockOnUpdatingGoroutine {
		retrieved, err := c.fetch(key, onFetch)
		if err != nil {
			// set loading to false only in case where the fetch failed,
			// the next lookup will retry the fetch. Note that the expired item
			// is not deleted, old value is returned
			i.updating = false
			return i
		}
		return retrieved
	}

	go func() {
		_, err := c.fetch(key, onFetch)
		if err != nil {
			// set loading to false only in case where the fetch failed,
			// the next look up will retry the fetch. Note that the expired item
			// is not deleted
			i.updating = false
		}
	}()

	return i
}

func (c *FetcherLRU) handleMiss(key string, onFetch FetchFunc) (*item, error) {
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
	return v.(*item), nil
}

// fetches a key's value and puts it on cache
// in case the Item is not found, an empty Item is put in cache
func (c *FetcherLRU) fetch(key string, onFetch FetchFunc) (*item, error) {
	v, err := onFetch()
	if err != nil {
		return nil, err
	}
	return c.add(key, v), nil
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

// GetWithTimeout is the same as GetOrFetch but returns a timeout error if the result is not ready on a given duration,
// the FetchFunc won't be cancelled
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

// MFetchFunc expects the values returned to be in the same order as the keySuffixes requested
type MFetchFunc func(keyPrefix string, keySuffixes []string) (values []interface{}, err error)

// MGetOrFetch gets the cache's values for keys or onFetch's values result, repeated keys are handled to only be called once
// to the onFetch func
func (c *FetcherLRU) MGetOrFetch(keyPrefix string, keySuffixes []string, onFetch MFetchFunc) ([]interface{}, error) {
	ret := make([]interface{}, len(keySuffixes))

	keysToFetchLenPrediction := len(keySuffixes)/2 + 1 // +1 for len 1 keys
	keysToFetch := make([]string, 0, keysToFetchLenPrediction)
	type entry struct {
		key           string
		item          *item
		returnIndexes []int
	}
	entries := make(map[string]*entry, keysToFetchLenPrediction)

	var mustBlockFetch bool
	var anyUpdate bool
	for i, keySuffix := range keySuffixes {
		// already processed keySuffixes use case
		if e, ok := entries[keySuffix]; ok {
			// if item is nil it could be empty and not updated, or empty and updated. Appending the index satisfies the case where
			// it is updated, and leaving it as nil satisfies the case where empty is a valid value and doesn't need to be updated
			if c.blockOnUpdatingGoroutine || e.item == nil {
				e.returnIndexes = append(e.returnIndexes, i)
			}

			if e.item != nil {
				// append the item to the return slice to satisfy the case where the item is being
				// updated by another goroutine. This value will be replaced with the updated value in
				// case it has to be fetched
				ret[i] = e.item.value
			}
			continue
		}

		key := keyPrefix + keySuffix
		if item, ok := c.get(key); ok {
			// key found in cache
			atomic.AddInt64(&c.stats.Hits, 1)
			// check and handle expiration, if the item is already in process
			// to be updated, just return the value obtained from cache despite being old
			if !item.updating && item.Expired() {
				item.Lock()
				// only the first thread has to request a fetch for an update; a double lock
				// is used to ensure it
				if !item.updating {
					item.updating = true
					anyUpdate = true
					keysToFetch = append(keysToFetch, keySuffix)
					entries[keySuffix] = &entry{key, item, []int{i}}
				} else {
					entries[keySuffix] = &entry{item: item}
				}
				item.Unlock()
			} else {
				entries[keySuffix] = &entry{item: item}
			}
			ret[i] = item.value
		} else {
			// key not found
			atomic.AddInt64(&c.stats.Misses, 1)
			keysToFetch = append(keysToFetch, keySuffix)
			entries[keySuffix] = &entry{key, nil, []int{i}}
			mustBlockFetch = true
		}
	}

	if len(keysToFetch) == 0 {
		return ret, nil
	}

	// when all elements to fetch are in the update use case, updates can be done asynchronously,
	// expired values are returned while the update is in progress
	if !c.blockOnUpdatingGoroutine && !mustBlockFetch {
		go func() {
			values, err := onFetch(keyPrefix, keysToFetch)
			if err != nil {
				for _, entry := range entries {
					if entry.item != nil {
						entry.item.updating = false
					}
				}
			}
			if len(values) == len(keysToFetch) {
				for i, v := range values {
					keySuffix := keysToFetch[i]
					entry := entries[keySuffix]
					c.add(entry.key, v)
				}
			}
		}()
		return ret, nil
	}

	// blocking fetch
	values, err := onFetch(keyPrefix, keysToFetch)
	if err != nil {
		if anyUpdate {
			for _, v := range entries {
				if v.item != nil {
					v.item.updating = false
				}
			}
		}
		return ret, err
	}
	if len(values) != len(keysToFetch) {
		return ret, ErrWrongMFetchResult
	}
	for i, v := range values {
		keySuffix := keysToFetch[i]
		entry := entries[keySuffix]
		c.add(entry.key, v)
		for _, i := range entry.returnIndexes {
			ret[i] = v
		}
	}

	return ret, nil
}

// Add forces an addition for a key's value
func (c *FetcherLRU) Add(key string, value interface{}) {
	c.add(key, value)
}

func (c *FetcherLRU) add(key string, value interface{}) *item {
	var i *item
	if value == nil {
		i = newEmptyItem(c.ttl)
	} else {
		i = newItem(value, c.ttl)
	}
	evicted := c.cache.Add(key, i)
	if evicted {
		atomic.AddInt64(&c.stats.Evictions, 1)
	}
	return i
}

type StatsFetcherLRU struct {
	Hits      int64
	Misses    int64
	Evictions int64
}

// Stats returns a snapshot of the current cache's Stats
func (c *FetcherLRU) Stats() StatsFetcherLRU {
	return c.stats
}
