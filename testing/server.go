package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

var (
	mu   sync.Mutex
	nums = []int{1, 2, 3, 4, 5} // slice (reslice-able)
)

// Args used by Multiply
type Args struct {
	A, B int
}

// Empty used by Next (no inputs needed)
type Empty struct{}

// Arith is exported so net/rpc can expose its methods
type Arith int

// Multiply: standard RPC shape (args, reply) error
func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

// Next: pops the first number from nums, or returns -111 if empty
func (t *Arith) Next(_ *Empty, reply *int) error {
	mu.Lock()
	defer mu.Unlock()

	if len(nums) > 0 {
		*reply = nums[0]
		nums = nums[1:]
	} else {
		*reply = -111
	}
	return nil
}

func main() {
	if err := rpc.Register(new(Arith)); err != nil {
		panic(err)
	}

	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		panic(err)
	}
	fmt.Println("Server listening on :1234")
	rpc.Accept(l)
}
