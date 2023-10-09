package main

import (
	"fmt"
	"os"
)

func ReadFileAsync(filename string) chan string {
	result := make(chan string)

	go func() {
		if data, err := os.ReadFile(filename); err == nil {
			result <- string(data)
		} else {
			panic(err)
		}
	}()

	return result
}

func main() {
	result := <-ReadFileAsync("test.txt")
	fmt.Println(result)
}
