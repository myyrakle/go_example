package main

func gererateStream[T any](done <-chan struct{}, values ...T) <-chan T {
	stream := make(chan T)
	go func() {
		defer close(stream)
		for _, value := range values {
			select {
			case <-done:
				return
			case stream <- value:
			}
		}
	}()
	return stream
}

func multiply(done <-chan struct{}, valueStream <-chan int, multiplier int) <-chan int {
	multipliedStream := make(chan int)
	go func() {
		defer close(multipliedStream)
		for value := range valueStream {
			select {
			case <-done:
				return
			case multipliedStream <- value * multiplier:
			}
		}
	}()
	return multipliedStream
}

func add(done <-chan struct{}, valueStream <-chan int, additive int) <-chan int {
	addedStream := make(chan int)
	go func() {
		defer close(addedStream)
		for value := range valueStream {
			select {
			case <-done:
				return
			case addedStream <- value + additive:
			}
		}
	}()
	return addedStream
}

func main() {
	done := make(chan struct{})
	defer close(done)

	intStream := gererateStream(done, 1, 2, 3, 4, 5)

	multipliedStream := multiply(done, intStream, 2)
	addedStream := add(done, multipliedStream, 1)

	for result := range addedStream {
		println(result)
	}
}
