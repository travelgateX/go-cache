package cache

import (
	"sync"
	"time"
)

// item is the instance stored in cache which wraps the real value
type item struct {
	// value is the actual value that will be retrieved from cache
	value interface{}
	// empty indicates if the Item represents an empty value
	empty bool
	// date of expiration in nanoseconds
	expiration int64
	// sync control used when an Item has expired and has to be updated,
	// only one thread should be updating and when it does, the updating flag
	// is set to true so the rest of threads know that this condition has been
	// already treated
	sync.Mutex
	updating bool
}

func newItem(value interface{}, minutes time.Duration) *item {
	return &item{
		value:      value,
		expiration: expirationTime(minutes),
	}
}

func newEmptyItem(ttl time.Duration) *item {
	return &item{
		value:      nil,
		expiration: expirationTime(ttl),
		empty:      true,
	}
}

func expirationTime(ttl time.Duration) int64 {
	timeExpiration := time.Now().Add(ttl)
	time := timeExpiration.UnixNano()
	return time
}

// IsEmpty indicates if its an empty item
func (i *item) IsEmpty() bool {
	return i.empty
}

// Expired indicates if the item has expired
func (i *item) Expired() bool {
	if i.expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > i.expiration
}
