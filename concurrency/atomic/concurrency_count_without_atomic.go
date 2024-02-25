package main

import (
	"sync"
)

func main() {
	counter := int32(0)
	ptr := &counter

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 100000; i++ {
			*ptr++
		}

		wg.Done()
	}()

	go func() {
		for i := 0; i < 100000; i++ {
			*ptr++
		}

		wg.Done()
	}()

	wg.Wait()
	println(counter)
}
