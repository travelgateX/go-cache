package cache

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func keyFetchFunc(key string, exes *int, sleep time.Duration) FetchFunc {
	return func() (value interface{}, err error) {
		if exes != nil {
			*exes++
		}
		if sleep != 0 {
			time.Sleep(sleep)
		}
		return key, nil
	}
}

func assertFoundGet(t *testing.T, expected, retrieved interface{}, err error) {
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if retrieved != expected {
		t.Errorf("retrieved %v expected to be %v", retrieved, expected)
	}
}

func assertStats(t *testing.T, s Stats, hits, misses, evictions int64) {
	if s.Hits != hits {
		t.Errorf("expected %v hits, found %v", hits, s.Hits)
	}
	if s.Misses != misses {
		t.Errorf("expected %v Misses, found %v", misses, s.Misses)
	}
	if s.Evictions != evictions {
		t.Errorf("expected %v Evictions, found %v", evictions, s.Evictions)
	}
}

func TestPutGet(t *testing.T) {
	c, err := New(1, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	k := "10"
	exes := 0
	v, err := c.GetOrFetch(k, keyFetchFunc(k, &exes, 0))
	assertFoundGet(t, k, v, err)
	c.GetOrFetch(k, keyFetchFunc(k, &exes, 0))
	assertStats(t, c.Stats(), 1, 1, 0)
	if exes != 1 {
		t.Error("only 1 execution expected")
	}
}

func TestGetNotFound(t *testing.T) {
	c, err := New(1, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	f := func() (value interface{}, err error) {
		return nil, nil
	}
	v, err := c.GetOrFetch("10", f)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if v != nil {
		t.Fatal("not found expected")
	}

	// cached empty item
	c.GetOrFetch("10", f)
	assertStats(t, c.Stats(), 1, 1, 0)
}

func TestGetError(t *testing.T) {
	c, err := New(1, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	f := func() (value interface{}, err error) {
		return nil, errors.New("error")
	}

	_, err = c.GetOrFetch("10", f)
	if err == nil {
		t.Fatal("error expected")
	}

	c.GetOrFetch("10", f)
	assertStats(t, c.Stats(), 0, 2, 0)
}

func TestTTL(t *testing.T) {
	t.Parallel()
	c, err := New(1, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	k := "10"
	exes := 0
	v, err := c.GetOrFetch(k, keyFetchFunc(k, &exes, 0))
	assertFoundGet(t, k, v, err)

	time.Sleep(500 * time.Millisecond)

	// force an update
	v, err = c.GetOrFetch(k, keyFetchFunc("20", &exes, 0))
	// old value is retrieved
	assertFoundGet(t, k, v, err)

	time.Sleep(100 * time.Millisecond)

	// the value should be updated now
	v, err = c.GetOrFetch(k, nil)
	assertFoundGet(t, "20", v, err)
	assertStats(t, c.Stats(), 2, 1, 0)
	if exes != 2 {
		t.Error("only 2 execution expected")
	}
}

func TestSingleflight(t *testing.T) {
	t.Parallel()
	c, err := New(1, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	exes := 0
	f := keyFetchFunc("10", &exes, time.Second)

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			c.GetOrFetch("10", f)
			wg.Done()
		}()
	}
	wg.Wait()

	assertStats(t, c.Stats(), 0, 10, 0)
	// only 1 execution
	if exes != 1 {
		t.Error("only 1 execution expected")
	}
}

func TestTimeout(t *testing.T) {
	t.Parallel()
	c, err := New(1, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	exes := 0
	// fetch of 200 ms
	f := keyFetchFunc("10", &exes, 200*time.Millisecond)
	// timeout of 400 ms
	v, err := c.GetWithTimeout("10", f, 400*time.Millisecond)
	assertFoundGet(t, "10", v, err)
	// timeout of 100 ms
	v, err = c.GetWithTimeout("20", f, 100*time.Millisecond)
	if err != ErrTimeout {
		t.Error("timeout expected")
	}

	assertStats(t, c.Stats(), 0, 2, 0)
	if exes != 2 {
		t.Error("2 execution expected")
	}
}

func benchmark(b *testing.B, useSingleFlight bool, fetchDuration time.Duration, parallelism int) {
	c, err := New(100, 500*time.Millisecond)
	if err != nil {
		b.Fatalf("unexpected error %v", err)
	}

	f := keyFetchFunc("", nil, fetchDuration)
	jobs := make(chan int, parallelism)
	for w := 0; w < parallelism; w++ {
		go worker(c, f, jobs, useSingleFlight)
	}

	p := -1
	for n := 0; n < b.N; n++ {
		if n%parallelism == 0 {
			p++
		}
		jobs <- p
	}
	close(jobs)
}

func worker(c *FetcherLRU, f FetchFunc, js <-chan int, useSingleFlight bool) {
	for j := range js {
		if useSingleFlight {
			c.GetOrFetch(strconv.Itoa(j), f)
		} else {
			c.GetWithoutSingleFlight(strconv.Itoa(j), f)
		}
	}
}

func BenchmarkSleep10Par0(b *testing.B)       { benchmark(b, false, 10*time.Millisecond, 1) }
func BenchmarkSingleSleep10Par0(b *testing.B) { benchmark(b, true, 10*time.Millisecond, 1) }

func BenchmarkSleep10Par10(b *testing.B)       { benchmark(b, false, 10*time.Millisecond, 10) }
func BenchmarkSingleSleep10Par10(b *testing.B) { benchmark(b, true, 10*time.Millisecond, 10) }

func BenchmarkSleep10Par100(b *testing.B)       { benchmark(b, false, 10*time.Millisecond, 100) }
func BenchmarkSingleSleep10Par100(b *testing.B) { benchmark(b, true, 10*time.Millisecond, 100) }

func BenchmarkSleep10Par1000(b *testing.B)       { benchmark(b, false, 10*time.Millisecond, 1000) }
func BenchmarkSingleSleep10Par1000(b *testing.B) { benchmark(b, true, 10*time.Millisecond, 1000) }

func BenchmarkSleep100Par0(b *testing.B)       { benchmark(b, false, 100*time.Millisecond, 1) }
func BenchmarkSingleSleep100Par0(b *testing.B) { benchmark(b, true, 100*time.Millisecond, 1) }

func BenchmarkSleep100Par10(b *testing.B)       { benchmark(b, false, 100*time.Millisecond, 10) }
func BenchmarkSingleSleep100Par10(b *testing.B) { benchmark(b, true, 100*time.Millisecond, 10) }

func BenchmarkSleep100Par100(b *testing.B)       { benchmark(b, false, 100*time.Millisecond, 100) }
func BenchmarkSingleSleep100Par100(b *testing.B) { benchmark(b, true, 100*time.Millisecond, 100) }

func BenchmarkSleep100Par1000(b *testing.B)       { benchmark(b, false, 100*time.Millisecond, 1000) }
func BenchmarkSingleSleep100Par1000(b *testing.B) { benchmark(b, true, 100*time.Millisecond, 1000) }

func BenchmarkSleep200Par0(b *testing.B)       { benchmark(b, false, 200*time.Millisecond, 1) }
func BenchmarkSingleSleep200Par0(b *testing.B) { benchmark(b, true, 200*time.Millisecond, 1) }

func BenchmarkSleep200Par10(b *testing.B)       { benchmark(b, false, 200*time.Millisecond, 10) }
func BenchmarkSingleSleep200Par10(b *testing.B) { benchmark(b, true, 200*time.Millisecond, 10) }

func BenchmarkSleep200Par100(b *testing.B)       { benchmark(b, false, 200*time.Millisecond, 100) }
func BenchmarkSingleSleep200Par100(b *testing.B) { benchmark(b, true, 200*time.Millisecond, 100) }

func BenchmarkSleep200Par1000(b *testing.B)       { benchmark(b, false, 200*time.Millisecond, 1000) }
func BenchmarkSingleSleep200Par1000(b *testing.B) { benchmark(b, true, 200*time.Millisecond, 1000) }

func benchmarkExpensive(b *testing.B, useSingleFlight bool, parallelism int) {
	c, err := New(100, 500*time.Millisecond)
	if err != nil {
		b.Fatalf("unexpected error %v", err)
	}

	f := func() (interface{}, error) {
		return Fib(20), nil
	}

	jobs := make(chan int, parallelism)
	for w := 0; w < parallelism; w++ {
		go worker(c, f, jobs, useSingleFlight)
	}

	p := -1
	for n := 0; n < b.N; n++ {
		if n%parallelism == 0 {
			p++
		}
		jobs <- p
	}
	close(jobs)
}

func Fib(n int) int {
	if n < 2 {
		return n
	}
	return Fib(n-1) + Fib(n-2)
}

func BenchmarkFibPar0(b *testing.B)       { benchmarkExpensive(b, false, 1) }
func BenchmarkFibSinglePar0(b *testing.B) { benchmarkExpensive(b, true, 1) }

func BenchmarkFibPar10(b *testing.B)       { benchmarkExpensive(b, false, 10) }
func BenchmarkFibSinglePar10(b *testing.B) { benchmarkExpensive(b, true, 10) }

func BenchmarkFibPar100(b *testing.B)       { benchmarkExpensive(b, false, 100) }
func BenchmarkFibSinglePar100(b *testing.B) { benchmarkExpensive(b, true, 100) }

func BenchmarkFibPar1000(b *testing.B)        { benchmarkExpensive(b, false, 1000) }
func BenchmarkFibSinglePar1000(b *testing.B)  { benchmarkExpensive(b, true, 1000) }
func BenchmarkFibPar10000(b *testing.B)       { benchmarkExpensive(b, false, 10000) }
func BenchmarkFibSinglePar10000(b *testing.B) { benchmarkExpensive(b, true, 10000) }
