package main

import (
	"fmt"
	"os"
)

type Result[T any] struct {
	Err   error
	Value *T
}

func ReadFileAsync(filename string) chan Result[string] {
	result := make(chan Result[string])

	go func() {
		if data, err := os.ReadFile(filename); err == nil {
			stringData := string(data)

			result <- Result[string]{
				Value: &stringData,
			}
		} else {
			result <- Result[string]{
				Err: err,
			}
		}
	}()

	return result
}

func main() {
	result := <-ReadFileAsync("not_found.txt")

	if result.Err != nil {
		fmt.Println("Error:", result.Err)
	} else {
		fmt.Println("Value:", *result.Value)
	}
}
