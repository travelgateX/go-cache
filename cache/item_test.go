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

// BenchmarkExpired_UnixNano-4     20000000                55.9 ns/op             0 B/op          0 allocs/op
func BenchmarkExpired_UnixNano(b *testing.B) {
	ttl := time.Millisecond
	exp := time.Now().Add(ttl).UnixNano()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Now().UnixNano() > exp
	}
}

// BenchmarkExpired_Sub-4          20000000                58.2 ns/op             0 B/op          0 allocs/op
func BenchmarkExpired_Sub(b *testing.B) {
	ttl := time.Millisecond
	exp := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Now().Sub(exp) > ttl
	}
}

// BenchmarkExpiration_UnixNano-4          20000000                67.1 ns/op             0 B/op          0 allocs/op
func BenchmarkExpiration_UnixNano(b *testing.B) {
	ttl := time.Millisecond
	for i := 0; i < b.N; i++ {
		time.Now().Add(ttl).UnixNano()
	}
}

// BenchmarkExpiration_Now-4               20000000                57.2 ns/op             0 B/op          0 allocs/op
func BenchmarkExpiration_Now(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now()
	}
}
