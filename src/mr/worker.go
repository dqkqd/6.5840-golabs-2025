package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func globIntermediateFilenames(reduceId int) ([]string, error) {
	return filepath.Glob(fmt.Sprintf("mr-tmpfile-*-%d", reduceId))
}

func reduceFilename(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
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

func runReduceTask(task Task, reducef func(string, []string) string) {
	filenames, err := globIntermediateFilenames(task.Id)
	if err != nil {
		log.Fatalf("cannot get intermediate filename to reduce, task: `%+v`", task)
	}

	kva := []KeyValue{}
	for _, filename := range filenames {
		f, err := os.OpenFile(filename, os.O_RDONLY, 0444)
		if err != nil {
			log.Fatalf("cannot open intermediate file: %s", filename)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		sort.Sort(ByKey(kva))
	}

	reduceFilename := reduceFilename(task.Id)
	reduceFile, err := os.OpenFile(reduceFilename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("cannot create reduce file `%s`", reduceFilename)
	}
	defer reduceFile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(reduceFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	isDebug := os.Getenv("DEBUG") == "1"
	if !isDebug {
		log.SetOutput(io.Discard)
		log.SetFlags(0)
	}

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
				case ReduceTask:
					runReduceTask(task, reducef)
				default:
					log.Fatalf("invalid task: `%+v`", task)
				}

				err := reportTaskDone(task)
				if err != nil {
					log.Fatalf("cannot report task done to coordinator: `%+v`", task)
				}
			}

		default:
			log.Fatalf("invalid task status: `%+v`", rpcTask.Status)
		}
	}
}

func reportTaskDone(task Task) error {
	log.Printf("finished task `%+v`, report to coordinator", task)
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
