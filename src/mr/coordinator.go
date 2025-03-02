package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
)

type Coordinator struct {
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	mu          sync.Mutex // lock to protect tasks
}

// Your code here -- RPC handlers for the worker to call.

// Schedule task to worker.
//
// Coordinator will first try to schedule map task.
// If all map tasks are running, coordinator should wait.
// Only if there is no map task, coordinator can look at reduce tasks.
func (c *Coordinator) ScheduleTask(workerArgs *int, reply *RpcTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Task.NReduce = c.nReduce

	if len(c.mapTasks) > 0 {
		// Worker should run map tasks first
		for i, mapTask := range c.mapTasks {
			if mapTask.CanBeScheduled() {
				reply.Task.Id = mapTask.Id
				reply.Task.Kind = MapTask
				reply.Status = WorkerCanRunTask
				reply.Task.MapFilename = mapTask.MapFilename

				// schedule the task, so other cannot touch it
				c.mapTasks[i].Schedule()
				log.Printf("distributed task `%+v` for worker `%+v`", c.mapTasks[i], *workerArgs)
				return nil
			}
		}
		log.Printf("all map tasks are running, worker `%+v` should wait", *workerArgs)
		reply.Status = WorkerShouldWait
	} else if len(c.reduceTasks) > 0 {
		// All map tasks are done, worker should run reduce tasks
		for i, reduceTask := range c.reduceTasks {
			if reduceTask.CanBeScheduled() {
				reply.Task.Id = reduceTask.Id
				reply.Task.Kind = ReduceTask
				reply.Status = WorkerCanRunTask

				// schedule the task, so other cannot touch it
				c.reduceTasks[i].Schedule()
				log.Printf("distributed task: `%+v` for worker `%+v`", c.reduceTasks[i], *workerArgs)
				return nil
			}
		}
		log.Printf("all reduce tasks are running, worker `%+v` should wait", *workerArgs)
		reply.Status = WorkerShouldWait
	} else {
		log.Printf("all tasks are finished, worker `%+v` should abort", *workerArgs)
		reply.Status = WorkerShouldStop
	}

	return nil
}

func (c *Coordinator) FinishTask(task *Task, _ *int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("task `%+v` done", task)

	switch task.Kind {

	case MapTask:
		mapTaskSize := len(c.mapTasks)
		c.mapTasks = slices.DeleteFunc(c.mapTasks, func(t Task) bool { return t.Id == task.Id })
		if mapTaskSize != len(c.mapTasks)+1 {
			log.Fatalf("unable to delete map task: `%+v`", *task)
		}

	case ReduceTask:
		reduceTaskSize := len(c.reduceTasks)
		c.reduceTasks = slices.DeleteFunc(c.reduceTasks, func(t Task) bool { return t.Id == task.Id })
		if reduceTaskSize != len(c.reduceTasks)+1 {
			log.Fatalf("unable to delete reduce task: `%+v`", *task)
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return len(c.mapTasks) == 0 && len(c.reduceTasks) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	isDebug := os.Getenv("DEBUG") == "1"
	if !isDebug {
		log.SetOutput(io.Discard)
		log.SetFlags(0)
	}

	c := Coordinator{}
	c.nReduce = nReduce

	// Create map tasks
	for mapId, file := range files {
		c.mapTasks = append(c.mapTasks, Task{
			Id:          mapId,
			Kind:        MapTask,
			MapFilename: file,
		})
	}

	// Create reduce tasks
	for reduceId := range nReduce {
		c.reduceTasks = append(c.reduceTasks, Task{
			Id:   reduceId,
			Kind: ReduceTask,
		})
	}

	c.server()
	return &c
}
