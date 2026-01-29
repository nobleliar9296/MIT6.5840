// client.go
package main

import (
	"fmt"
	"net/rpc"
	"sync"
)

type Args struct{ A, B int }
type Empty struct{}

func main() {
	c, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// Quick multiply sanity check
	var prod int
	if err := c.Call("Arith.Multiply", &Args{7, 8}, &prod); err != nil {
		panic(err)
	}
	fmt.Println("Multiply:", prod) // 56

	const workers = 4
	var wg sync.WaitGroup
	var mu sync.Mutex
	collected := []int{}

	callNext := func() (int, error) {
		var out int
		err := c.Call("Arith.Next", &Empty{}, &out)
		return out, err
	}

	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				v, err := callNext()
				if err != nil {
					// handle or log; for demo, panic
					panic(err)
				}
				if v == -111 { // nothing left
					return
				}
				mu.Lock()
				collected = append(collected, v)
				mu.Unlock()
				fmt.Println("Next:", v)
			}
		}()
	}
	wg.Wait()

	fmt.Println("Collected:", collected)
}
