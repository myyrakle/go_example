package main

import (
	"fmt"
	db "just_test/db"
)

func main() {
	var database db.KvDB = &db.RedisDB{}

	database.Set("key", "value")
	result, err := database.Get("key")

	if err != nil {
		panic(err)
	}

	fmt.Println("result:", result)
}
