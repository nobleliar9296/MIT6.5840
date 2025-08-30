package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// takes in the work and uses mapf to run it and store values in files named, mr-X-Y
// where x is the jobid and y is the bucket number
func maped(work *Work, mapf func(string, string) []KeyValue) {
	file, err := os.Open(work.Filename)
	if err != nil {
		log.Fatal("Cannot open %v", work.Filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	file.Close()

	kv := mapf(work.Filename, string(content))

	dict := make([][]KeyValue, 10)

	for _, word := range kv {
		temp := ihash(word.Key) % (-work.JobCat - 1)
		dict[temp] = append(dict[temp], word)
	}

	// make the number of buckets positive
	for i := 1; i <= -work.JobCat; i++ {
		outFile := "mr-" + strconv.Itoa(work.JobId) + "-" + strconv.Itoa(i) + ".json"

		file, err = os.Create(outFile)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		enc := json.NewEncoder(file)

		// write each entry to file could have used an array or made a buffer
		// TODO buffer optimization
		for _, kva := range dict[i-1] {
			if err := enc.Encode(&kva); err != nil {
				panic(err)
			}
		}
	}

}

// takes in the work and uses reducef to run it and store values from files mr-z-Y
// where z varies and Y is the nth reduce operation and stores the result in mr-out-Y
func reduced(work *Work, reducef func(string, []string) string) {

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// get id for your worker implementation
	id := 0
	call("ID.GetID", new(struct{}), &id)

	// TODO to delete
	fmt.Printf("workerid %d\n", id)

	// work that needs to be done
	reply := Work{ServerId: id}

	// infinite loop to keep working
	for {

		err := call("Master.GetWork", &id, &reply)

		if !err {
			// end the process if the master has quit
			break
		}

		// if the server is waiting for the job
		if reply.JobId < 0 {
			reply = Work{ServerId: id}
		} else {
			// means it is a map job
			if reply.JobCat < 0 {
				maped(&reply, mapf)
			} else {
				// otherwise we perform a reduce job
				reduced(&reply, reducef)
			}
		}

		// TODO delete
		if reply.JobId >= 0 {
			fmt.Printf("%v\n", reply)
		}
		time.Sleep(2 * time.Second)

		// call when the job is finished (removes it from the queue)
		err = call("Master.Finished", &reply, &Empty{})
		if !err {
			panic(err)
		}

		//TODO remove
		time.Sleep(2 * time.Second)
	}

	// end of Worker
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
