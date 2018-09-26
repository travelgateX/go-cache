package cache

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func keyFetchFunc(key string, exes *int, sleep time.Duration) FetchFunc {
	return func() (interface{}, error) {
		if exes != nil {
			*exes++
		}
		if sleep != 0 {
			time.Sleep(sleep)
		}
		return key, nil
	}
}

func keyMFetchFunc(exes *int, sleep time.Duration) MFetchFunc {
	return func(bk string, keys []string) ([]interface{}, error) {
		if exes != nil {
			*exes++
		}
		if sleep != 0 {
			time.Sleep(sleep)
		}
		ret := make([]interface{}, len(keys))
		for i := range keys {
			ret[i] = keys[i]
		}
		return ret, nil
	}
}

func stringsToEmptyInterface(strs []string) []interface{} {
	ret := make([]interface{}, len(strs))
	for i, str := range strs {
		ret[i] = str
	}
	return ret
}

func assertFoundGet(t *testing.T, expected, retrieved interface{}, err error) {
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if retrieved != expected {
		t.Errorf("retrieved %v expected to be %v", retrieved, expected)
	}
}

func assertFoundMGet(t *testing.T, expected, retrieved []interface{}, err error) {
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if len(expected) != len(retrieved) {
		t.Fatalf("expected and retrieve values have different len: %v, %v", len(expected), len(retrieved))
	}

	for i, e := range expected {
		if e != retrieved[i] {
			t.Errorf("retrieved %v expected to be %v", retrieved[i], e)
		}
	}
}

func assertStats(t *testing.T, s StatsFetcherLRU, hits, misses, evictions int64) {
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
	assertStats(t, c.Stats(), 0, 1, 0)
	c.GetOrFetch(k, keyFetchFunc(k, &exes, 0))
	assertStats(t, c.Stats(), 1, 1, 0)
	if exes != 1 {
		t.Error("only 1 execution expected, found " + strconv.Itoa(exes))
	}
}

func TestMPutMGet(t *testing.T) {
	c, err := New(4, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	ks := []string{"10", "11", "12", "13", "13", "12", "12"}
	bk := "baseKey"
	exes := 0
	retrieved, err := c.MGetOrFetch(bk, ks, keyMFetchFunc(&exes, 0))
	expected := stringsToEmptyInterface(ks)
	assertFoundMGet(t, expected, retrieved, err)
	t.Logf("expected: %v, retrieved: %v", expected, retrieved)
	assertStats(t, c.Stats(), 0, 4, 0)
	c.MGetOrFetch(bk, ks, keyMFetchFunc(&exes, 0))
	assertStats(t, c.Stats(), 4, 4, 0)
	if exes != 1 {
		t.Error("only 1 execution expected, found " + strconv.Itoa(exes))
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

func TestMGetError(t *testing.T) {
	c, err := New(4, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	f := func(bk string, ks []string) (value []interface{}, err error) {
		return nil, errors.New("error")
	}
	ks := []string{"10", "11", "12", "13", "13", "12", "12"}
	bk := "baseKey"
	_, err = c.MGetOrFetch(bk, ks, f)
	if err == nil {
		t.Fatal("error expected")
	}

	c.MGetOrFetch(bk, ks, f)
	assertStats(t, c.Stats(), 0, 8, 0)
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
		t.Error("only 2 execution expected, found " + strconv.Itoa(exes))
	}
}

func TestMTTL(t *testing.T) {
	t.Parallel()
	c, err := New(4, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	ks := []string{"10", "11", "12", "13", "13", "12", "12"}
	bk := "baseKey"
	exes := 0
	retrieved, err := c.MGetOrFetch(bk, ks, keyMFetchFunc(&exes, 0))
	expected := stringsToEmptyInterface(ks)
	assertFoundMGet(t, expected, retrieved, err)

	time.Sleep(500 * time.Millisecond)

	// force an update
	updates := []interface{}{"28", "24", "12", "D"}
	f := func(bk string, ks []string) (value []interface{}, err error) {
		exes++
		return updates, nil
	}
	retrieved, err = c.MGetOrFetch(bk, ks, f)
	// old value is retrieved
	assertFoundMGet(t, retrieved, retrieved, err)

	time.Sleep(100 * time.Millisecond)

	// the value should be updated now
	updateExpected := []interface{}{"28", "24", "12", "D", "D", "12", "12"}
	retrieved, err = c.MGetOrFetch(bk, ks, nil)
	assertFoundMGet(t, updateExpected, retrieved, err)
	assertStats(t, c.Stats(), 8, 4, 0)
	if exes != 2 {
		t.Error("only 2 execution expected, found " + strconv.Itoa(exes))
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

func TestGetOrFetchForceLatest(t *testing.T) {
	t.Parallel()
	firstRoutineReturnLatestValueWhenKeyExpire(t, 1000)
}

func firstRoutineReturnLatestValueWhenKeyExpire(t *testing.T, routineCount int) {
	c, err := New(1, 1*time.Second, ForceLatestValue(true))
	if err != nil {
		t.Fatalf("unexpected error %oldValue", err)
	}

	//return the same value as key
	keyFunc := func(key string, expected interface{}) FetchFunc {
		ret := expected
		return func() (interface{}, error) {
			return ret, nil
		}
	}

	//get value before expired
	k := "10"
	oldValue, err := c.GetOrFetch(k, keyFunc(k, k))
	assertFoundGet(t, k, oldValue, err)
	//let the value be expired
	time.Sleep(1 * time.Second)

	expected := "20"
	firstWorking := make(chan bool)
	firstFinished := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(routineCount)

	// func that simulate a expensive work by the first routine
	waitFunc := func(key string, expected interface{}, wg *sync.WaitGroup) FetchFunc {
		ret := expected
		return func() (interface{}, error) {
			// notify the other routines that first routine are working,they are ready to go
			// since this routine is processing, other routines will get the old value
			firstWorking <- true
			//wait for other routines to finish in order to ensure that they are all returning the old value
			wg.Wait()
			return ret, nil
		}
	}

	go func() {
		v, err := c.GetOrFetch(k, waitFunc(k, expected, wg))
		if err != nil {
			t.Errorf("unexpected error:%v", err)
		} else {
			if expected != v {
				t.Errorf("expected:%v but return %v", expected, v)
			}
			//signal other routines that first routine updated the value
			firstFinished <- true
		}
	}()

	//block until the first routine is executing waitFunc, to ensure that there is one routine updating the key
	<-firstWorking
	for i := 1; i <= routineCount; i++ {
		go func() {
			defer wg.Done()
			//must return old value
			v, err := c.GetOrFetch(k, nil)

			if err != nil {
				t.Errorf("unexpected error:%v", err)
			} else {
				if oldValue != v {
					t.Errorf("expected:%v but return %v", oldValue, v)
				}

			}

		}()
	}
	wg.Wait()

	//After the key has updated, now the key is NOT expired, we check if all routine return the updated value
	<-firstFinished
	wg = new(sync.WaitGroup)
	wg.Add(routineCount)
	for i := 1; i <= routineCount; i++ {
		go func() {
			defer wg.Done()
			//must return new value
			v, err := c.GetOrFetch(k, nil)
			if err != nil {
				t.Errorf("unexpected error:%v", err)
			} else {
				if expected != v {
					t.Errorf("expected:%v but return %v", expected, v)
				}

			}

		}()
	}
	wg.Wait()
}

func TestMGetOrFetchForceLatest(t *testing.T) {
	t.Parallel()
	MFirstRoutineReturnLatestValueWhenKeyExpire(t, 10000)
}

func MFirstRoutineReturnLatestValueWhenKeyExpire(t *testing.T, routineCount int) {
	c, err := New(5, 1*time.Second, ForceLatestValue(true))
	if err != nil {
		t.Fatalf("unexpected error %oldValue", err)
	}

	//return the same values as keys
	MKeyFunc := func(keyPrefix string, keySuffix []string) MFetchFunc {
		return func(keyPrefix string, keySuffix []string) ([]interface{}, error) {
			ret := make([]interface{}, 0, len(keySuffix))
			for i := range keySuffix {
				ret = append(ret, keySuffix[i])
			}
			return ret, nil
		}
	}

	//get value before expired
	ks := []string{"10", "11", "12", "13", "13", "12", "12"}
	bk := "baseKey"
	retrieved, err := c.mGetOrFetchForceLatest(bk, ks, MKeyFunc(bk, ks))
	oldExpected := stringsToEmptyInterface(ks)
	assertFoundMGet(t, oldExpected, retrieved, err)
	//let the values be expired
	time.Sleep(1 * time.Second)

	// now we test when key expired the first must wait to return all keys
	newExpected := []interface{}{"10new", "11new", "12new", "13new", "13new", "12new", "12new"}
	firstWorking := make(chan bool)
	firstFinished := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(routineCount)

	MWaitFunc := func(keyPrefix string, keySuffix []string) MFetchFunc {
		return func(keyPrefix string, keySuffix []string) ([]interface{}, error) {
			ret := make([]interface{}, 0, len(keySuffix))
			for i := range keySuffix {
				ret = append(ret, keySuffix[i]+"new")
			}
			t.Log("generated key of first routine:", ret)
			firstWorking <- true
			//wait for other routines to finish in order to ensure that they are all returning the old value
			wg.Wait()
			return ret, nil
		}
	}

	go func() {
		retrieved, err := c.mGetOrFetchForceLatest(bk, ks, MWaitFunc(bk, ks))
		if err != nil {
			t.Errorf("unexpected error:%v", err)
		} else {
			t.Log("Result of first routine:", retrieved)
			assertFoundMGet(t, newExpected, retrieved, err)
			//signal other routines that first routine updated the value
			firstFinished <- true
		}
	}()

	//block until the first routine is executing waitFunc, to ensure that there is one routine updating the key
	<-firstWorking
	for i := 1; i <= routineCount; i++ {
		go func() {
			defer wg.Done()
			//must return old values
			retrieved, err := c.mGetOrFetchForceLatest(bk, ks, MWaitFunc(bk, ks))
			if err != nil {
				t.Errorf("unexpected error:%v", err)
			} else {
				assertFoundMGet(t, oldExpected, retrieved, err)
			}

		}()
	}
	wg.Wait()
	<-firstFinished

	wg = new(sync.WaitGroup)
	wg.Add(routineCount)
	for i := 1; i <= routineCount; i++ {
		go func() {
			defer wg.Done()
			//must return old values
			retrieved, err := c.mGetOrFetchForceLatest(bk, ks, MWaitFunc(bk, ks))
			if err != nil {
				t.Errorf("unexpected error:%v", err)
			} else {
				assertFoundMGet(t, newExpected, retrieved, err)
			}

		}()
	}
	wg.Wait()
}
