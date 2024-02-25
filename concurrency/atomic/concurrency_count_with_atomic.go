package main

import (
	"sync"
	"sync/atomic"
)

func main() {
	counter := int32(0)
	ptr := &counter

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 100000; i++ {
			atomic.AddInt32(ptr, 1)
		}

		wg.Done()
	}()

	go func() {
		for i := 0; i < 100000; i++ {
			atomic.AddInt32(ptr, 1)
		}

		wg.Done()
	}()

	wg.Wait()
	println(counter)
}
