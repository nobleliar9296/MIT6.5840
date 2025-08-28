package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ServerId struct {
	mu     sync.Mutex
	nextID int
}

type Queue struct {
	mq    sync.Mutex
	que   []Item
	doing []Item
}

// maintains the queue for
type Item struct {
	Filename string
	JobId    int
	ServerId int
	JobCat   int
	Time     time.Time // give ten seconds
	Assigned bool
	Done     bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-noble"
	s += strconv.Itoa(os.Getuid())
	return s
}
