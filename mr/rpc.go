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

type serverId struct {
	mu     sync.Mutex
	nextID int
}

type queue struct {
	mq  sync.Mutex
	que []item
}

// maintains the queue for
type item struct {
	filename string
	jobId    int
	serverId int
	jobCat   int
	time     int // give ten seconds
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
