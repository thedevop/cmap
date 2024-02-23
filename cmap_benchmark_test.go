// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 thedevop
// SPDX-FileContributor: thedevop

package cmap

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

func BenchmarkCMap(b *testing.B) {
	count := 1048576
	cpus := runtime.GOMAXPROCS(0) * 2

	c := New[int, bool]()

	for thread := 1; thread <= cpus; thread = thread << 1 {
		c = New[int, bool]()
		b.Run(fmt.Sprintf("Set threads: %v", thread), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cmapMix(c, count/thread, 0, thread, 0)
			}
		})
	}

	for thread := 1; thread <= cpus; thread = thread << 1 {
		b.Run(fmt.Sprintf("Get threads: %v", thread), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cmapMix(c, count/thread, thread, 0, 0)
			}
		})
	}

	for thread := 1; thread <= cpus; thread = thread << 1 {
		b.Run(fmt.Sprintf("Del threads: %v", thread), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				c = NewSize[int, bool](count / thread)
				for n := 0; n < count/thread; n++ {
					c.Set(n, true)
				}
				b.StartTimer()

				cmapMix(c, count/thread, 0, 0, thread)
			}
		})
	}

	c = NewSize[int, bool](count / cpus)
	cmapMix(c, count/cpus/2, 1, 0, 0)
	b.Run(fmt.Sprintf("Mixed threads: %v", cpus), func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cmapMix(c, count/cpus, cpus, 1, 0)
		}
	})

}

func BenchmarkGMap(b *testing.B) {
	count := 1048576
	cpus := runtime.GOMAXPROCS(0) * 2

	m := make(map[int]bool)

	for thread := 1; thread <= cpus; thread = thread << 1 {
		b.Run(fmt.Sprintf("Set threads: %v", thread), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				gmapMix(m, count/thread, 0, thread, 0)
			}
		})
	}

	for thread := 1; thread <= cpus; thread = thread << 1 {
		b.Run(fmt.Sprintf("Get threads: %v", thread), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				gmapMix(m, count/thread, thread, 0, 0)
			}
		})
	}

	for thread := 1; thread <= cpus; thread = thread << 1 {
		b.Run(fmt.Sprintf("Del threads: %v", thread), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				m = make(map[int]bool, count/thread)
				for n := 0; n < count/thread; n++ {
					m[n] = true
				}
				b.StartTimer()

				gmapMix(m, count/thread, 0, 0, thread)
			}
		})
	}

	m = make(map[int]bool, cpus)
	gmapMix(m, count/cpus/2, 1, 0, 0)
	b.Run(fmt.Sprintf("Mixed threads: %v", cpus), func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			gmapMix(m, count/cpus, cpus, 1, 0)
		}
	})
}

func cmapMix(c *ConcurrentMap[int, bool], count, gthreads, sthreads, dthreads int) {
	wg := &sync.WaitGroup{}
	wg.Add(gthreads + sthreads + dthreads)

	for thread := 0; thread < gthreads; thread++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(1))
			for i := 0; i < count; i++ {
				k := r.Intn(count)
				v, ok := c.Get(k)
				_, _ = v, ok
			}
		}()
	}

	for thread := 0; thread < sthreads; thread++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(1))
			for i := 0; i < count; i++ {
				k := r.Intn(count)
				c.Set(k, true)
			}
		}()
	}

	for thread := 0; thread < dthreads; thread++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(1))
			for i := 0; i < count; i++ {
				k := r.Intn(count)
				v, ok := c.Del(k)
				_, _ = v, ok
			}
		}()
	}
	wg.Wait()

}

func gmapMix(m map[int]bool, count, gthreads, sthreads, dthreads int) {
	l := &sync.Mutex{}

	wg := &sync.WaitGroup{}
	wg.Add(gthreads + sthreads + dthreads)
	for thread := 0; thread < gthreads; thread++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(1))
			for i := 0; i < count; i++ {
				k := r.Intn(count)
				l.Lock()
				v, ok := m[k]
				l.Unlock()
				_, _ = v, ok
			}
		}()
	}

	for thread := 0; thread < sthreads; thread++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(1))
			for i := 0; i < count; i++ {
				k := r.Intn(count)
				l.Lock()
				m[k] = true
				l.Unlock()
			}
		}()
	}

	for thread := 0; thread < dthreads; thread++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(1))
			for i := 0; i < count; i++ {
				k := r.Intn(count)
				l.Lock()
				delete(m, k)
				l.Unlock()
			}
		}()
	}

	wg.Wait()
}
