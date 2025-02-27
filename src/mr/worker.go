package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// TODO: generate a unique worker id?
	workerId := 1

	for {
		// Your worker implementation here.
		// TODO: Ask for task
		task, err := askForTask(workerId)
		if err != nil {
			// cannot receive any task.
			// There are 3 cases:
			//  - There are running map tasks that reduce tasks couldn't given => we should wait
			//  - All of the tasks being given to other workers => we should wait
			//  - There are no more task => we should stop
			// TODO: Just stop for now. Handle it later.
			break
		}

		fmt.Printf("received task %v\n", task.MapInputFile)

		// TODO: if this is a map task, run it

		// TODO: if this is a reduce task, run it
	}
}

func askForTask(workerId int) (Task, error) {
	workerArgs := workerId
	replyTask := Task{}

	ok := call("Coordinator.GiveTask", &workerArgs, &replyTask)
	if !ok {
		return replyTask, fmt.Errorf("cannot receive task from coordinator for worker: %v", workerArgs)
	}

	return replyTask, nil
}

// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
