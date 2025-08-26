package main

import (
	"sync"
)

func sum(start int, end int) int {
	sum := 0

	for ; start <= end; start++ {
		sum += start
	}

	return sum
}

func main() {

	var mu sync.Mutex

	total := 0
	var done sync.WaitGroup

}
