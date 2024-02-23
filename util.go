// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 thedevop
// SPDX-FileContributor: thedevop

package cmap

//lint:file-ignore U1000 Ignore all unused code, links to Go runtime code

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

type spinlock struct {
	l atomic.Uint32
}

func (s *spinlock) Lock() {
	for !s.l.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
}

func (s *spinlock) Unlock() {
	s.l.Store(0)
}

type HashFn func(unsafe.Pointer, uintptr) uintptr

func selectHash[K comparable]() HashFn {
	a := any(make(map[K]struct{}))
	i := (*mapiface)(unsafe.Pointer(&a))
	return i.typ.hasher
}

type mapiface struct {
	typ *maptype
	val *hmap
}

// go/src/runtime/type.go
type maptype struct {
	typ    _type
	key    *_type
	elem   *_type
	bucket *_type
	// function for hashing keys (ptr to key, seed) -> hash
	hasher     HashFn
	keysize    uint8
	elemsize   uint8
	bucketsize uint16
	flags      uint32
}

// go/src/runtime/map.go
type hmap struct {
	count     int
	flags     uint8
	B         uint8
	noverflow uint16
	// hash seed
	hash0      uint32
	buckets    unsafe.Pointer
	oldbuckets unsafe.Pointer
	nevacuate  uintptr
	// true type is *mapextra
	// but we don't need this data
	extra unsafe.Pointer
}

// go/src/runtime/type.go
type tflag uint8
type nameOff int32
type typeOff int32

// go/src/runtime/type.go
type _type struct {
	size       uintptr
	ptrdata    uintptr
	hash       uint32
	tflag      tflag
	align      uint8
	fieldAlign uint8
	kind       uint8
	equal      func(unsafe.Pointer, unsafe.Pointer) bool
	gcdata     *byte
	str        nameOff
	ptrToThis  typeOff
}

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

// noEscapePtr hides a pointer from escape analysis. See noescape.
// USE CAREFULLY!
//
//go:nosplit
func noEscapePtr[T any](p *T) *T {
	x := uintptr(unsafe.Pointer(p))
	return (*T)(unsafe.Pointer(x ^ 0))
}
