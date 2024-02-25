package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

func main() {
	for {
		number1 := atomic.Int32{}
		number2 := atomic.Int32{}

		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			number1.Store(10)
			number2.Store(20)

			wg.Done()
		}()

		go func() {
			a := number1.Load()
			b := number2.Load()

			if a == 0 && b == 20 {
				fmt.Println("a is 0 and b is 20")
			}

			wg.Done()
		}()

		wg.Wait()
	}
}
