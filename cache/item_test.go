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

func BenchmarkExpired_UnixNano(b *testing.B) {
	ttl := time.Millisecond
	exp := time.Now().Add(ttl).UnixNano()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Now().UnixNano() > exp
	}
}

func BenchmarkExpired_Sub(b *testing.B) {
	ttl := time.Millisecond
	exp := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Now().Sub(exp) > ttl
	}
}

func BenchmarkExpiration_UnixNano(b *testing.B) {
	ttl := time.Millisecond
	for i := 0; i < b.N; i++ {
		time.Now().Add(ttl).UnixNano()
	}
}

func BenchmarkExpiration_Now(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now()
	}
}
