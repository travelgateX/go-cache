# go-cache

This repository contains caching related go packages

### FetcherLRU
In memory LRU cache that satifies an specific use case where the cache is filled by demand by an OnFetch func:

```golang
type FetchFunc func() (value interface{}, err error)
```

The getter method for this cache requires a key and a FetchFunc that will be executed when the key is not found or has expired, the OnFetch result will be stored in cache for that key.

- Its concurrent safe
- Gets on missing keys are blocked until the OnFetch finishes
- Ensures that only one OnFetch func is executed per key at the same time
- Empty items are cached (nil values returned by a FetchFunc)
- Gets on expired keys
    - default cause an update but are non-blocking, the old value is returned
    - with 'SetBlockOnUpdatingGoroutine' option, the goroutine calling for an update will block, meanwhile the value is updating, other goroutines will get the expired value
- When an error occurs during a fetch func, if the key is expired it returns the old value, otherwise it returns nil value and scalates the error obtained in the fetch func 
- Easy to implement :)

Example of use where a service is wrapped into a cache layer using FetcherLRU:

```golang
    type Service interface {
        ExpensiveCall(k string) (v interface{}, err error)
    }

    type CachedService struct {
        c *FetcherLru
        s Service
    }

    var _ Service = &CachedService{}

    func (s *CachedService) ExpensiveCall(k string) (interface{}, error) {
        onFetch := func() (interface{}, error) {
            return s.s.ExpensiveCall(k)
        }
        
        return s.c.GetOrFetch(k, onFetch)
    }
```

