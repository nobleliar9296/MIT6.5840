package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const lease = 10 * time.Second

type Master struct {
	// Your definitions here.
	mu   sync.Mutex
	done bool
	l    net.Listener
	q    *Queue
}

// made a queue to keep track of jobs
type Queue struct {
	mq    sync.Mutex
	que   []Item // to assign
	doing []Item // check if they are still alive
	l     net.Listener
}

// maintains the queue with all the information needed
type Item struct {
	Filename string    // the name of the file
	JobId    int       // keep track of the job number
	ServerId int       // the server that gets the job
	JobCat   int       // -nreduce for mapf carries (total buckets) and positive for the nth reducef job
	Time     time.Time // give ten seconds
	Assigned bool      // has this job been assigned and check if it still exists and the lease hasn't expired
	Done     bool      // has the job been done, then it will be removed
}

// Your code here -- RPC handlers for the worker to call.

// give each worker an id to identify them
func (m *ServerId) GetID(_ *struct{}, reply *int) error {

	// if there are multiple workers calling to get Id we need to make sure only one is assigned to each
	m.mu.Lock()
	m.nextID++
	*reply = m.nextID

	// TODO delete before submitting
	fmt.Println("Assign id")
	m.mu.Unlock()
	return nil
}

func (m *Master) Finished(work *Work, _ *Empty) error {
	que := m.q

	// aquire the lock
	que.mq.Lock()
	defer que.mq.Unlock()

	fmt.Println("Finished:", work)
	fmt.Println(que.doing)

	for i := 0; i < len(que.doing); i++ {
		if work.JobId == que.doing[i].JobId && work.ServerId == que.doing[i].ServerId {
			// remove work that has been completed
			que.doing = append(que.doing[:i], que.doing[i+1:]...)

			// return
			return nil
		}
		if que.doing[i].Time.Before(time.Now()) {

			//TODO cleanup of files cause if crashed some will exist
			fmt.Println("lease expired:", que.doing[i])
			// lease expired and wasn't finished
			que.doing[i].Assigned = false

			// remove from doing and put in que and won't be assigned to the same worker
			que.que = append(que.que, que.doing[i])
			que.doing = append(que.doing[:i], que.doing[i+1:]...)
		}
	}

	return nil

}

func (m *Master) GetWork(workerId *int, reply *Work) error {
	que := m.q
	que.mq.Lock()
	defer que.mq.Unlock()

	allowReduce := true
	for _, it := range que.que {
		if it.JobCat < 0 { // map task pending
			allowReduce = false
			break
		}
	}
	if allowReduce {
		for idx, it := range que.doing {
			if it.JobCat < 0 { // map task in-flight
				allowReduce = false
			}

			// the job expired and the job wasn't finished put it on the pile in the front
			if it.Time.Before(time.Now()) {

				fmt.Println("lease expired:", it)
				// lease expired and wasn't finished
				it.Assigned = false
				// remove from doing and put in que and won't be assigned to the same worker
				que.que = append(que.que, it)
				que.doing = append(que.doing[:idx], que.doing[idx+1:]...)
			}
		}
	}

	for i := range que.que {
		it := &(que.que)[i]

		// map jobs
		if it.ServerId != *workerId && !it.Assigned && it.JobCat < 0 {

			// keep track of the job state
			it.ServerId = *workerId
			it.Time = time.Now().Add(lease)
			it.Assigned = true

			// assign values to the reply
			reply.Filename = it.Filename
			reply.JobId = it.JobId
			reply.JobCat = it.JobCat

			// add the assigned job to a new pile
			que.doing = append(que.doing, *it)
			fmt.Println("work it:", it)

			// Remove the assigned job from the pool
			que.que = append(que.que[:i], que.que[i+1:]...)

			// TODO delete debug statements
			fmt.Println("que______________________________________________________________________________")
			for _, pt := range que.que {
				fmt.Printf("%v\n", pt)
			}
			fmt.Println("end*************************************************************************")
			fmt.Println("doing______________________________________________________________________________")
			for _, pt := range que.doing {
				fmt.Printf("%v\n", pt)
			}
			fmt.Println("end doing*************************************************************************")

			// return after assigning work
			return nil

		}

		// reduce jobs
		if allowReduce && !it.Assigned && it.ServerId != *workerId && it.JobCat >= 0 {

			// keep track of the job state
			it.ServerId = *workerId
			it.Time = time.Now().Add(lease)
			it.Assigned = true

			// assign values to the reply
			reply.Filename = it.Filename
			reply.JobId = it.JobId
			reply.JobCat = it.JobCat

			// add the assigned job to a new pile
			que.doing = append(que.doing, *it)
			fmt.Println("work it:", it)

			// Remove the assigned job from the pool
			que.que = append(que.que[i+1:], que.que[:i]...)

			// return after assigning work
			return nil

		}
	}

	if len(que.que) == 0 && len(que.doing) == 0 {
		go func() {
			m.done = true
			m.l.Close()
		}()

		// TODO delete debug statement
		fmt.Println("end work")
		fmt.Println("end work", reply)
	}

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
func (m *Master) server() {
	rpc.Register(m)

	// assign id to workers
	sid := &ServerId{}
	rpc.RegisterName("ID", sid)

	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// assign the net listener so that we ca
	m.l = l

	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := m.done

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {

	que := &Queue{que: make([]Item, 0), doing: make([]Item, 0)}

	// Your code here.
	for num, filename := range os.Args[1:] {

		// add it to the queue
		que.que = append(que.que, Item{filename, num + 1, 0, -nReduce, time.Time{}, false, false})

		fmt.Printf("%v \n", que)
		fmt.Println(filename)
	}

	numJobs := len(que.que)

	temp := make([]Item, 0)

	for i := 1; i <= nReduce; i++ {
		temp = append(temp, Item{"mr-", numJobs, 0, i, time.Time{}, false, false})
	}

	que.que = append(que.que, temp...)

	m := Master{done: false, q: que}
	m.server()
	return &m
}
