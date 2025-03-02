package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

func intermediateFilename(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-tmpfile-%d-%d", mapId, reduceId)
}

func runMapTask(task Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.MapFilename)
	if err != nil {
		log.Fatalf("cannot open input map file, task: `%+v`", task)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read input map file, task: `%+v`", task)
	}
	file.Close()

	tmpfiles := []*os.File{}
	tmpEncoders := []*json.Encoder{}
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Cannot get current directory, task: `%+v`", task)
	}
	for reduceId := range task.NReduce {
		filename := intermediateFilename(task.Id, reduceId)
		tmpfile, err := os.CreateTemp(cwd, filename)
		if err != nil {
			log.Fatalf("cannot create temporary file, task: `%+v`", task)
		}
		defer os.Remove(tmpfile.Name())

		tmpfiles = append(tmpfiles, tmpfile)
		tmpEncoders = append(tmpEncoders, json.NewEncoder(tmpfile))
	}

	for _, kv := range mapf(task.MapFilename, string(content)) {
		reduceTaskId := ihash(kv.Key) % task.NReduce
		err := tmpEncoders[reduceTaskId].Encode(kv)
		if err != nil {
			log.Fatalf("cannot write to temporary file, task: `%+v`", task)
		}
	}

	// rename completed temp files
	for reduceId, tmpfile := range tmpfiles {
		filename := intermediateFilename(task.Id, reduceId)
		os.Rename(tmpfile.Name(), filename)
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// TODO: generate a unique worker id?
	workerId := 1

	for {
		rpcTask, err := queryTask(workerId)
		// Cannot get task from coordinator, assume it to be dead and quit
		if err != nil {
			return
		}
		log.Printf("received task %+v", rpcTask)

		switch rpcTask.Status {

		case Wait:
			// TODO: is 1 second enough?
			time.Sleep(time.Second)

		case Stopped:
			// coordinator stopped
			break

		case Ok:
			{
				task := rpcTask.Task
				switch task.Kind {

				case MapTask:
					runMapTask(task, mapf)
					err := reportTaskDone(task)
					if err != nil {
						// cannot report to coordinator, assume it to be dead and quit
						return
					}

				case ReduceTask:
					log.Fatalf("todo")

				}
			}
		}
	}
}

func reportTaskDone(task Task) error {
	reply := 0
	ok := call("Coordinator.FinishTask", &task, &reply)
	if !ok {
		return fmt.Errorf("cannot report finished task to worker, task: %+v", task)
	}
	return nil
}

func queryTask(workerId int) (RpcTask, error) {
	workerArgs := workerId
	rpcTask := RpcTask{}
	ok := call("Coordinator.DistributeTask", &workerArgs, &rpcTask)
	if !ok {
		return rpcTask, fmt.Errorf("cannot receive task from coordinator for worker: %+v", workerArgs)
	}
	return rpcTask, nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
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
