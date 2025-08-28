package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
	"time"
)

const lease = 10 * time.Second

type Master struct {
	// Your definitions here.
	mu   sync.Mutex
	done bool
}

// Your code here -- RPC handlers for the worker to call.

// give each worker an id to identify them
func (m *ServerId) GetID(_ *struct{}, reply *int) error {
	m.mu.Lock()
	m.nextID++
	*reply = m.nextID
	fmt.Println("Assign id")
	m.mu.Unlock()
	return nil
}

func (que *Queue) GetWork(workerId *int, reply *Item) error {
	que.mq.Lock()
	defer que.mq.Unlock()

	for i := range que.que {
		it := &(que.que)[i]
		if it.ServerId != *workerId && !it.Assigned {
			it.ServerId = *workerId
			it.Time = time.Now().Add(lease)
			it.Assigned = true
			*reply = *it
			que.doing = append(que.doing, *it)
			fmt.Println("index i: ", i)
			fmt.Printf("%v\n", que.que)
			que.que = slices.Delete(que.que, i, i+1)
			return nil
		}
	}

	fmt.Println("end work")
	*reply = Item{Done: true}
	fmt.Println("end work", reply)

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println("master")
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server(que *Queue) {
	rpc.Register(m)

	// assign id to workers
	sid := &ServerId{}
	rpc.RegisterName("ID", sid)

	//get work
	if err := rpc.RegisterName("Q", que); err != nil {
		log.Fatalf("register Q: %v", err)
	}

	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{done: false}

	que := &Queue{que: make([]Item, 0), doing: make([]Item, 0)}

	// Your code here.
	for num, filename := range os.Args[1:] {

		// add it to the queue
		que.que = append(que.que, Item{filename, num, 0, 0, time.Time{}, false, false})

		fmt.Printf("%v \n", que)
		fmt.Println(filename)
	}

	m.server(que)
	return &m
}
