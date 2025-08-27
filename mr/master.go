package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const lease = 10 * time.Second

type Master struct {
	// Your definitions here.
	filename string
}

// Your code here -- RPC handlers for the worker to call.

// give each worker an id to identify them
func (m *serverId) GetID(_ *struct{}, reply *int) error {
	m.mu.Lock()
	m.nextID++
	*reply = m.nextID
	fmt.Println("Assign id")
	m.mu.Unlock()
	return nil
}

func (que *queue) getWork(workerId *int, reply *item) error {
	que.mq.Lock()
	defer que.mq.Unlock()

	for i := range *que.que {
		it := &(*que.que)[i]
		if it.serverId != *workerId {
			it.serverId = *workerId
			it.time = time.Now().Add(5 * time.Second)
			*reply = *it
			return nil
		}
	}

	return fmt.Errorf("no available work")
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
func (m *Master) server() {
	rpc.Register(m)

	// assign id to workers
	sid := &serverId{}
	rpc.RegisterName("ID", sid)

	//get work
	work := &getwork{}
	rpc.RegisterName("Work", work)

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
	m := Master{}

	que := queue{que: make([]item, 0)}

	// Your code here.
	for num, filename := range os.Args[1:] {

		// add it to the queue
		que.que = append(que.que, item{filename, num, 0, 0, 0})

		fmt.Printf("%v \n", st[num])
		fmt.Println(filename)
	}

	fmt.Println(len(st))

	m.server()
	return &m
}
