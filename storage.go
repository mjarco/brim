package main

import (
	"fmt"
)

type MigrationTask struct {
	src, dest, key, env string
}

func store(task clasifiedKey) {
	fmt.Println(task)
}