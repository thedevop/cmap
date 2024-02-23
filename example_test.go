// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 thedevop
// SPDX-FileContributor: thedevop

package cmap_test

import (
	"fmt"

	"github.com/thedevop/cmap"
)

func Example() {
	c := cmap.New[int, bool]()

	for i := 0; i < 5; i++ {
		c.Set(i, true) // Set key/value
	}

	key := 10
	v, ok := c.Get(10) // Get value of key
	if ok {
		fmt.Printf("get key: %d has value: %v\n", key, v)
	} else {
		fmt.Println("get did not find key", key)
	}

	key = 0
	v, ok = c.Del(key) // Delete element
	if ok {
		fmt.Printf("delete key: %d had value: %v\n", key, v)
	} else {
		fmt.Println("delete did not find key", key)
	}

	walkFn := func(key int, value bool) bool {
		fmt.Printf("found key %d with value %v\n", key, value)
		return true
	}

	c.Walk(walkFn) // Iterate the map

	// Output:
	// get did not find key 10
	// delete key: 0 had value: true
	// found key 1 with value true
	// found key 2 with value true
	// found key 3 with value true
	// found key 4 with value true
}
