// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 thedevop
// SPDX-FileContributor: thedevop

package cmap

import (
	"math/bits"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const (
	bucketCnt = 8

	loadFactorDen = 2
	loadFactorNum = loadFactorDen * bucketCnt * 13 / 16

	emptyRest      = 0 // this cell is empty, and there are no more non-empty cells at higher indexes or overflows.
	emptyOne       = 1 // this cell is empty
	evacuatedEmpty = 2 // cell is empty, bucket is evacuated.
	minTopHash     = 5 // minimum tophash for a normal filled cell.

	flagAny  = 0
	flagIfNx = 1 // only if key does not exist
	flagIfEx = 2 // only if key already exist
)

// ConcurrentMap is hashmap that's safe for concurrent read/write access. Its implementation is
// very similar to the native Go maps. It is backed by a hasmap. Each bucket contains a spinlock.
// Only the bucket that contains the key is locked during read/write. So larger the hahsmap, the
// less the lock contention.
type ConcurrentMap[K comparable, V any] struct {
	cm    atomic.Pointer[CMap[K, V]]
	count atomic.Int64
}

// New returns a new ConcurrentMap.
func New[K comparable, V any]() *ConcurrentMap[K, V] {
	return NewSize[K, V](0)
}

// NewSize returns a new ConcurrentMap, the returned ConcurrentMap will be large enough to hold at
// lease n elements without resizing.
func NewSize[K comparable, V any](n int) *ConcurrentMap[K, V] {
	b := bucketSize(n)
	hashFn := selectHash[K]()

	cm := newCmap[K, V](b, hashFn)

	cc := &ConcurrentMap[K, V]{}
	cc.cm.Store(cm)

	return cc
}

// Get returns the value of the key. If found, ok is true.
func (cc *ConcurrentMap[K, V]) Get(key K) (v V, ok bool) {
	return cc.mapget(key)
}

// Value is similar to Get, but only returns the value without the bool flag.
func (cc *ConcurrentMap[K, V]) Value(key K) (v V) {
	v, _ = cc.mapget(key)
	return
}

// Set the key/value. If the key existed, updates the value.
func (cc *ConcurrentMap[K, V]) Set(key K, value V) {
	cc.mapset(key, value, flagAny)
}

// Insert the key/value only if the key doesn't already exist. Return true if succesful.
func (cc *ConcurrentMap[K, V]) Insert(key K, value V) bool {
	return cc.mapset(key, value, flagIfNx)
}

// Del, deletes the key/value. Returns the value of the key prior to deletion,
// if found, ok is true.
func (cc *ConcurrentMap[K, V]) Del(key K) (v V, ok bool) {
	return cc.mapdel(key)
}

// Walk iterates through the ConcurrentMap and calls fn sequentially for each key/value present
// in the map. Given the concurrent access nature, Walk will commit to:
// 1. Only iterate through the current map. Keys only exist in new map during resize won't be included.
// 2. The current value of the key when Walk reaches that element.
// 3. If a key is deleted before Walk reach that element, it won't be included.
// 4. If a key is added before Walk reach that element, it will be included, otherwise no.
// Walk does not provide any order guarantees.
func (cc *ConcurrentMap[K, V]) Walk(fn WalkFn[K, V]) {
	cc.walk(fn)
}

// Len returns number of elements in the map.
func (cc *ConcurrentMap[K, V]) Len() int {
	return int(cc.count.Load())
}

func (cc *ConcurrentMap[K, V]) mapset(key K, value V, flag int) bool {
	cm := cc.cm.Load()

	fullhash := cm.hash(key)
	top := tophash(fullhash)
	bucket := cm.buckets[fullhash&cm.mask]

	bucket.Lock()

start:
	newcm := cm.newcm.Load()
	if newcm != nil { // resize in progress
		if !bucket.migrated() { // help evcuate
			bMigrated := cm.migrateTo(newcm, bucket)
			if bMigrated == len(cm.buckets) {
				cc.cm.CompareAndSwap(cm, newcm)
			}
		}

		for bucket.migrated() { // bucket already evcuated, find new bucket in the latest cmap
			bucket.Unlock()
			cm = cm.newcm.Load()
			// Not loading latest newcm to prevent another resize while resize is in progress

			fullhash = cm.hash(key)
			top = tophash(fullhash)

			bucket = cm.buckets[fullhash&cm.mask]
			bucket.Lock()
		}
	}

	var insertI int
	var insertB *Bucket[K, V]
	b := bucket
bucketLoop:
	for {
		for i, hash := range b.topHash {
			if hash != top {
				if isEmpty(hash) && insertB == nil {
					insertI = i
					insertB = b
				}
				if hash == emptyRest {
					break bucketLoop
				}
				continue
			}

			if b.key[i] == key {
				if flag == flagIfNx { // insert only if key doesn't exist
					bucket.Unlock()
					return false
				}
				b.value[i] = value
				bucket.Unlock()
				return true
			}
		}

		if b.overflow == nil {
			break
		}
		b = b.overflow
	}

	if newcm == nil && (overLoadFactor(cc.Len()+1, cm.B) || tooManyOverflow(cm.noverflow.Load(), cm.B)) {
		cc.resize()
		goto start
	}

	if insertB == nil {
		insertB = newBucket[K, V]()
		b.overflow = insertB
		insertI = 0
		cm.noverflow.Add(1)
	}

	insertB.topHash[insertI] = top
	insertB.key[insertI] = key
	insertB.value[insertI] = value
	bucket.Unlock()
	cc.count.Add(1)
	return true
}

func (cc *ConcurrentMap[K, V]) mapget(key K) (v V, ok bool) {
	cm := cc.cm.Load()

	fullhash := cm.hash(key)
	top := tophash(fullhash)
	bucket := cm.buckets[fullhash&cm.mask]

	// bucket.RLock()
	bucket.Lock()

	newcm := cm.newcm.Load()
	if newcm != nil { // resize in progress
		if !bucket.migrated() { // help to evcuate
			bMigrated := cm.migrateTo(newcm, bucket)
			if bMigrated == len(cm.buckets) {
				cc.cm.CompareAndSwap(cm, newcm)
			}
		}

		for bucket.migrated() {
			bucket.Unlock()
			cm = cm.newcm.Load()

			fullhash = cm.hash(key)
			top = tophash(fullhash)

			bucket = cm.buckets[fullhash&cm.mask]
			bucket.Lock()
		}
	}

	for b := bucket; b != nil; b = b.overflow {
		for i, hash := range b.topHash {
			if hash != top {
				if hash == emptyRest {
					bucket.Unlock()
					return
				}
				continue
			}

			if b.key[i] == key {
				bucket.Unlock()
				return b.value[i], true
			}
		}
	}
	bucket.Unlock()

	return
}

func (cc *ConcurrentMap[K, V]) mapdel(key K) (v V, ok bool) {
	cm := cc.cm.Load()
	fullhash := cm.hash(key)
	top := tophash(fullhash)
	bucket := cm.buckets[fullhash&cm.mask]

	bucket.Lock()

	newcm := cm.newcm.Load()
	if newcm != nil {
		if !bucket.migrated() { // help to evcuate
			bMigrated := cm.migrateTo(newcm, bucket)
			if bMigrated == len(cm.buckets) {
				cc.cm.CompareAndSwap(cm, newcm)
			}
		}

		for bucket.migrated() {
			bucket.Unlock()
			cm = cm.newcm.Load()

			fullhash = cm.hash(key)
			top = tophash(fullhash)

			bucket = cm.buckets[fullhash&cm.mask]
			bucket.Lock()
		}
	}

	bOrig := bucket

bucketLoop:
	for b := bucket; b != nil; b = b.overflow {
		for i, hash := range b.topHash {
			if hash != top {
				if hash == emptyRest {
					break bucketLoop
				}
				continue
			}

			if b.key[i] != key {
				continue
			}

			v, ok = b.value[i], true
			cc.clear(b, i, emptyOne)
			cc.count.Add(-1)

			if i == bucketCnt-1 {
				if b.overflow != nil && b.overflow.topHash[0] != emptyRest {
					break bucketLoop
				}
			} else {
				if b.topHash[i+1] != emptyRest {
					break bucketLoop
				}
			}

			for {
				b.topHash[i] = emptyRest
				if i == 0 {
					if b == bOrig {
						break bucketLoop
					}
					c := b
					for b = bOrig; b.overflow != c; b = b.overflow {
					}
					i = bucketCnt - 1
				} else {
					i--
				}
				if b.topHash[i] != emptyOne {
					break bucketLoop
				}
			}
		}
	}
	bucket.Unlock()
	return
}

func (cc *ConcurrentMap[K, V]) walk(fn WalkFn[K, V]) {
	var keys []K
	var values []V

	cm := cc.cm.Load()
	newcm := cm.newcm.Load()

	for _, bucket := range cm.buckets {
		bucket.Lock()

		if bucket.migrated() {
			if newcm == nil {
				newcm = cm.newcm.Load()
			}
		}

	bucketLoop:
		for b := bucket; b != nil; b = b.overflow {
			for i, hash := range b.topHash {
				if isEmpty(hash) {
					if hash == emptyRest {
						break bucketLoop
					}
					continue
				}

				keys = append(keys, b.key[i])

				if bucket.migrated() {
					v, _ := cc.mapget(b.key[i])
					values = append(values, v)
					continue
				}

				values = append(values, b.value[i])
			}
		}

		bucket.Unlock()

		for i, key := range keys {
			if !fn(key, values[i]) {
				return
			}
		}

		keys = keys[:0]
		values = values[:0]
	}
}

// resize the cmap
func (cc *ConcurrentMap[K, V]) resize() {
	cm := cc.cm.Load()

	if !cm.resize.CompareAndSwap(0, 1) { // only allow 1 resize, avoid allocation if already started
		return
	}

	incr := uint8(1)
	if !overLoadFactor(cc.Len()+1, cm.B) {
		incr = 0
	}

	newcm := newCmap[K, V](cm.B+incr, cm.hashFn)
	cm.newcm.Store(newcm)

	go func() { // helper to migrate if there are not much read/write
		for _, b := range cm.buckets {
			b.Lock()
			bMigrated := cm.migrateTo(newcm, b)
			b.Unlock()

			if bMigrated == len(cm.buckets) {
				break
			}
		}

		cc.cm.CompareAndSwap(cm, newcm)
	}()
}

// clear an element, used to delete an element
func (cc *ConcurrentMap[K, V]) clear(b *Bucket[K, V], idx int, hash uint8) {
	var emptyKey K
	var emptyValue V
	b.topHash[idx] = hash
	b.key[idx] = emptyKey
	b.value[idx] = emptyValue
}

// CMap holds a hashmap
type CMap[K comparable, V any] struct {
	newcm     atomic.Pointer[CMap[K, V]] // new cmap when resizing
	hashFn    HashFn                     // hash function
	buckets   []*Bucket[K, V]            // buckets
	mask      uintptr                    // mask used to determine bucket number
	seed      uintptr                    // hash seed
	bmigrated atomic.Int64               // number of buckets migrated to new cmap
	noverflow atomic.Int64               // number of overflow buckets
	resize    atomic.Uint32              // CAS lock for resize, only allow 1 resize
	B         uint8                      // log_2 of # of buckets (can hold up to loadFactor * 2^B items)
}

func newCmap[K comparable, V any](B uint8, hashFn HashFn) *CMap[K, V] {
	cm := &CMap[K, V]{
		mask:    bucketMask(B),
		seed:    uintptr(rand.Uint64()),
		hashFn:  hashFn,
		buckets: make([]*Bucket[K, V], bucketShift(B)),
		B:       B,
	}

	for i := range cm.buckets {
		cm.buckets[i] = newBucket[K, V]()
	}
	return cm
}

// hash returns the hash of the key
func (cm *CMap[K, V]) hash(key K) uintptr {
	return cm.hashFn(noescape(unsafe.Pointer(&key)), cm.seed)
}

// migrateTo, migrates a bucket to the destination cmap. It will not trigger destination cmap resize.
// Caller must obtain write lock on the oldbucket. return total migrated buckets.
func (cm *CMap[K, V]) migrateTo(dstCm *CMap[K, V], srcB *Bucket[K, V]) int {
	if srcB.migrated() {
		return int(cm.bmigrated.Load())
	}

srcBucketLoop:
	for ; srcB != nil; srcB = srcB.overflow {
		for srcIdx, srcHash := range srcB.topHash {
			srcB.topHash[srcIdx] = evacuatedEmpty

			if isEmpty(srcHash) {
				if srcHash == emptyRest {
					break srcBucketLoop
				}
				continue
			}

			fullhash := dstCm.hash(srcB.key[srcIdx])
			top := tophash(fullhash)
			bucket := dstCm.buckets[fullhash&dstCm.mask]

			// add to new cmap, will not resize when one resize is progressing
			bucket.Lock()

			var insertI int
			var insertB *Bucket[K, V]
			b := bucket
		dstBucketLoop:
			for {
				for dstIdx, dstHash := range b.topHash {
					if dstHash != top {
						if isEmpty(dstHash) && insertB == nil {
							insertI = dstIdx
							insertB = b
						}
						if dstHash == emptyRest {
							break dstBucketLoop
						}
						continue
					}
				}

				overflow := b.overflow
				if overflow == nil {
					break
				}
				b = overflow
			}

			if insertB == nil {
				insertB = newBucket[K, V]()
				b.overflow = insertB
				insertI = 0
			}

			insertB.topHash[insertI] = top
			insertB.key[insertI] = srcB.key[srcIdx]
			insertB.value[insertI] = srcB.value[srcIdx]
			bucket.Unlock()

			// not clearing out key/value, as when migration is done, the entire old cmap will be GCed
		}
	}
	return int(cm.bmigrated.Add(1))
}

type Bucket[K comparable, V any] struct {
	topHash  [bucketCnt]uint8
	key      [bucketCnt]K
	value    [bucketCnt]V
	overflow *Bucket[K, V]
	// sync.RWMutex
	spinlock
}

func newBucket[K comparable, V any]() *Bucket[K, V] {
	return &Bucket[K, V]{}
}

// migrated returns true if the bucket is already migrated
func (b *Bucket[K, V]) migrated() bool {
	return b.topHash[0] == evacuatedEmpty
}

// bucketShift returns 1<<b, optimized for code generation.
func bucketShift(b uint8) uintptr {
	// Masking the shift amount allows overflow checks to be elided.
	return uintptr(1) << (b & (bits.UintSize - 1))
}

// bucketMask returns 1<<b - 1, optimized for code generation.
func bucketMask(b uint8) uintptr {
	return bucketShift(b) - 1
}

// tophash returns the highest 8 bits of the hash
func tophash(hash uintptr) uint8 {
	top := uint8(hash >> (bits.UintSize - 8))
	if top < minTopHash {
		top += minTopHash
	}
	return top
}

// isEmpty returns true if slot is empty
func isEmpty(x uint8) bool {
	return x <= emptyOne
}

// overLoadFactor reports whether count items placed in 1<<B buckets is over loadFactor.
// number of buckets = 2 ^ B
// number of buckets / 2 * 13
// log_2 of # of buckets (can hold up to loadFactor * 2^B items)
func overLoadFactor(count int, B uint8) bool {
	return count > bucketCnt && uintptr(count) > loadFactorNum*(bucketShift(B)/loadFactorDen)
}

// bucketSize calculates the number of 2^n buckets
func bucketSize(count int) (n uint8) {
	for overLoadFactor(count, n) {
		n++
	}
	return
}

// tooManyOverflow if there are as many overflow buckets as buckets
func tooManyOverflow(noverflow int64, B uint8) bool {
	// return noverflow >= (uint64(1)<<B)/2
	return noverflow >= (int64(1) << B)
}

// WalkFn is  used when walking the map. Takes a key and value, returning if iteration should continue.
type WalkFn[K comparable, V any] func(key K, value V) bool
