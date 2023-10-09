package main

import (
	"fmt"
	"time"
)

func main() {
	doWork := func(done <-chan any) {
		defer fmt.Println("doWork done")

		fmt.Println("doWork start")

		for {
			select {
			default:
				fmt.Println("do something...")
				time.Sleep(1 * time.Second)
			case <-done:
				return
			}
		}
	}

	done := make(chan any)
	go doWork(done)

	// 5초 대기
	time.Sleep(5 * time.Second)
	close(done)

	// 자식 고루틴이 종료될때까지 대기...
	time.Sleep(1 * time.Second)
}
