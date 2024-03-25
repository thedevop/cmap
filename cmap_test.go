// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 thedevop
// SPDX-FileContributor: thedevop

package cmap

import (
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentMapNew(t *testing.T) {
	c := New[int, bool]()
	cm := c.cm.Load()

	require.Equal(t, uint8(0), cm.B)
	require.Equal(t, 1, len(cm.buckets))
	require.Nil(t, cm.newcm.Load())
	require.Equal(t, int64(0), cm.bmigrated.Load())
	require.Equal(t, int64(0), cm.noverflow.Load())
	require.Equal(t, uint32(0), cm.resize.Load())
}

func TestConcurrentMapNewSize(t *testing.T) {
	c := NewSize[int, bool](1000)
	cm := c.cm.Load()

	require.Equal(t, uint8(8), cm.B)
	require.Equal(t, 256, len(cm.buckets))
	require.Nil(t, cm.newcm.Load())
	require.Equal(t, int64(0), cm.bmigrated.Load())
	require.Equal(t, int64(0), cm.noverflow.Load())
	require.Equal(t, uint32(0), cm.resize.Load())
}

func TestConcurrentMapSet(t *testing.T) {
	tests := []struct {
		key   string
		value int
	}{
		{"one", 1},
		{"two", 2},
		{"three", 3},
	}
	c := NewSize[string, int](0)

	for i, tt := range tests {
		c.Set(tt.key, tt.value)
		require.Equal(t, tt.key, c.cm.Load().buckets[0].key[i])
		require.Equal(t, tt.value, c.cm.Load().buckets[0].value[i])
	}

	c.Set("one", 111)
	v, ok := c.Get("one")
	require.True(t, ok)
	require.Equal(t, 111, v)
}

func TestConcurrentMapInsert(t *testing.T) {
	tests := []struct {
		key      string
		value    int
		expectOk bool
		expect   int
	}{
		{"one", 1, true, 1},
		{"two", 2, true, 2},
		{"three", 3, true, 3},
		{"one", 111, false, 1},
		{"two", 222, false, 2},
		{"three", 333, false, 3},
	}
	c := NewSize[string, int](0)

	for _, tt := range tests {
		ok := c.Insert(tt.key, tt.value)
		require.Equal(t, tt.expectOk, ok)
		v, ok := c.Get(tt.key)
		require.True(t, ok)
		require.Equal(t, tt.expect, v)
	}
}

func TestConcurrentMapAddResize(t *testing.T) {
	c := New[int, bool]()
	cm := c.cm.Load()
	B := cm.B
	fillBucket(0, 2, cm)
	c.count.Add(16)
	require.Nil(t, cm.newcm.Load())

	c.Set(math.MaxInt, true)
	require.NotNil(t, cm.newcm.Load())
	newcm := c.cm.Load()
	for newcm == cm {
		runtime.Gosched()
		newcm = c.cm.Load()
	}

	require.Equal(t, B+1, newcm.B)
}

func TestConcurrentMapAddResizeSameSize(t *testing.T) {
	c := New[int, bool]()
	cm := c.cm.Load()
	B := cm.B
	fillBucket(0, 2, cm)
	c.count.Add(16)
	require.Nil(t, cm.newcm.Load())

	b := cm.buckets[0]
	for b != nil {
		for _, key := range b.key {
			c.Del(key)
		}
		b = b.overflow
	}

	c.Set(math.MaxInt, true)
	require.NotNil(t, cm.newcm.Load())
	newcm := c.cm.Load()
	for newcm == cm {
		runtime.Gosched()
		newcm = c.cm.Load()
	}

	require.Equal(t, B, newcm.B)
}

func TestConcurrentMapGet(t *testing.T) {
	tests := []struct {
		key   string
		value int
	}{
		{"one", 1},
		{"two", 2},
		{"three", 3},
	}

	c := NewSize[string, int](0)

	for _, tt := range tests {
		c.Set(tt.key, tt.value)
		v, ok := c.Get(tt.key)
		require.True(t, ok)
		require.Equal(t, tt.value, v)
	}
}

func TestConcurrentMapDel(t *testing.T) {
	tests := []struct {
		key   string
		value int
	}{
		{"one", 1},
		{"two", 2},
		{"three", 3},
	}
	c := NewSize[string, int](0)

	var emptyKey string
	var emptyValue int
	for i, tt := range tests {
		c.Set(tt.key, tt.value)
		require.Equal(t, tt.key, c.cm.Load().buckets[0].key[i])
		require.Equal(t, tt.value, c.cm.Load().buckets[0].value[i])
	}

	for i, tt := range tests {
		v, ok := c.Del(tt.key)
		require.True(t, ok)
		require.Equal(t, tt.value, v)
		require.Equal(t, emptyKey, c.cm.Load().buckets[0].key[i])
		require.Equal(t, emptyValue, c.cm.Load().buckets[0].value[i])
	}
}

func TestCMapDelClearTophash(t *testing.T) {
	c := New[int, bool]()
	cm := c.cm.Load()
	fillBucket(0, 2, cm)
	b := cm.buckets[0]
	require.NotNil(t, b.overflow)
	for b != nil {
		for i, hash := range b.topHash {
			require.False(t, isEmpty(hash))
			require.NotZero(t, b.key[i])
			require.NotZero(t, b.value[i])
		}
		b = b.overflow
	}

	b = cm.buckets[0]
	c.Del(b.key[6])
	require.EqualValues(t, emptyOne, b.topHash[6])
	c.Del(b.key[7])
	require.EqualValues(t, emptyOne, b.topHash[7])

	b = cm.buckets[0].overflow
	c.Del(b.key[6])
	require.EqualValues(t, emptyOne, b.topHash[6])
	c.Del(b.key[7])
	require.EqualValues(t, emptyRest, b.topHash[6])
	require.EqualValues(t, emptyRest, b.topHash[7])
	for i := range b.topHash {
		c.Del(b.key[i])
	}
	require.EqualValues(t, emptyRest, b.topHash[0])

	b = cm.buckets[0]
	c.Del(b.key[5])
	require.EqualValues(t, emptyRest, b.topHash[5])
	require.EqualValues(t, emptyRest, b.topHash[6])
	require.EqualValues(t, emptyRest, b.topHash[7])
}

func TestConcurrentMap(t *testing.T) {
	const threads = 4
	count := 100000

	c := NewSize[int, bool](0)

	cmapAdd(c, threads, count)

	for i := 0; i < count; i++ {
		v, ok := c.Get(i)
		require.True(t, ok, "index %d", i)
		require.Equal(t, true, v, "index %d", i)
	}

	cmapDel(c, threads, count)

	for i := 0; i < count; i++ {
		v, ok := c.Get(i)
		require.False(t, ok, "index %d", i)
		require.Equal(t, false, v, "index %d", i)
	}

	cmapAdd(c, threads, count)
	for i := 0; i < count; i++ {
		v, ok := c.Get(i)
		require.True(t, ok, "index %d", i)
		require.Equal(t, true, v, "index %d", i)
	}
}

func TestConcurrentMapWalk(t *testing.T) {
	const threads = 4
	count := 100000

	c := NewSize[int, bool](count)
	results := make(map[int]bool)
	fn := func(key int, value bool) bool {
		results[key] = value
		return true
	}

	c.Walk(fn)
	require.Equal(t, 0, len(results))
	for i := 0; i < count; i++ {
		require.False(t, results[i])
	}

	cmapAdd(c, threads, count)

	c.Walk(fn)
	require.Equal(t, count, len(results))
	for i := 0; i < count; i++ {
		require.True(t, results[i])
	}
}

func cmapAdd(c *ConcurrentMap[int, bool], threads, count int) {
	v := true
	seg := count / threads

	wg := sync.WaitGroup{}
	wg.Add(threads)
	for t := 0; t < threads; t++ {
		go func(n int) {
			defer wg.Done()
			start := n * seg
			end := start + seg
			if n == threads-1 {
				end = count
			}
			for x := start; x < end; x++ {
				c.Set(x, v)
			}
		}(t)
	}
	wg.Wait()
}

func cmapDel(c *ConcurrentMap[int, bool], threads, count int) {
	seg := count / threads

	wg := sync.WaitGroup{}
	wg.Add(threads)
	for t := 0; t < threads; t++ {
		go func(n int) {
			defer wg.Done()
			start := n * seg
			end := start + seg
			if n == threads-1 {
				end = count
			}
			for x := start; x < end; x++ {
				c.Del(x)
			}
		}(t)
	}
	wg.Wait()
}

func TestOverLoadFactor(t *testing.T) {
	tests := []struct {
		n      int
		b      uint8
		expect bool
	}{
		{1, 0, false},
		{9, 0, true},
		{14, 1, true},
		{27, 2, true},
		{53, 3, true},
		{105, 4, true},
		{209, 5, true},
	}

	for i, tt := range tests {
		r := overLoadFactor(tt.n, tt.b)
		require.Equal(t, tt.expect, r, "index %d", i)
	}
}

func TestBucketSize(t *testing.T) {
	tests := []struct {
		count  int
		expect uint8
	}{
		{count: -1, expect: 0},
		{count: math.MinInt64, expect: 0},
		{count: 0, expect: 0},
		{count: 1, expect: 0},
		{count: 9, expect: 1},
		{count: 14, expect: 2},
		{count: 27, expect: 3},
		{count: 53, expect: 4},
		{count: 1000000, expect: 18},
		{count: math.MaxInt, expect: 61},
	}
	for i, tt := range tests {
		b := bucketSize(tt.count)
		require.Equal(t, tt.expect, b, "index %d", i)
	}
}

func TestConcurrentMapPointerValue(t *testing.T) {
	c := NewSize[string, *int](1)
	key := "test"
	n := 1
	expect := 10
	c.Set(key, &n)
	v1, ok := c.Get(key)
	require.True(t, ok)
	require.Equal(t, n, *v1)
	*v1 = expect
	v2, _ := c.Get(key)
	require.Equal(t, expect, *v2)
	require.Equal(t, n, *v2)
	require.Equal(t, v1, v2)
}

func fillBucket(bucketN, n int, cm *CMap[int, bool]) {
	b := cm.buckets[bucketN]
	keys := make(map[int]bool)

	for bucketCnt := 0; bucketCnt < n; bucketCnt++ {
		for i := range b.topHash {
			var key int
			var top uint8
			for {
				key = rand.Intn(100000)
				if exist := keys[key]; exist {
					continue
				}
				fullhash := cm.hash(key)
				top = tophash(fullhash)
				if uintptr(bucketN) == fullhash&cm.mask {
					break
				}
			}
			b.topHash[i] = top
			b.key[i] = key
			b.value[i] = true
			keys[key] = true
		}

		if bucketCnt < n-1 {
			b.overflow = newBucket[int, bool]()
			cm.noverflow.Add(1)
			b = b.overflow
		}
	}

}
