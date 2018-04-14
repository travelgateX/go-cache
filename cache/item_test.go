package cache

import (
	"testing"
	"time"
)

func TestExpiration(t *testing.T) {
	expiration := time.Millisecond * 500
	i := newItem(nil, expiration)
	if i.Expired() {
		t.Fatal("item shouldn't be expired")
	}
	time.Sleep(expiration)
	if !i.Expired() {
		t.Fatal("item should be expired")
	}
}
