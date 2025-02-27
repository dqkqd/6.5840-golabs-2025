package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	remainingTasks []Task

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GiveTask(workerArgs *int, task *Task) error {
	c.mu.Lock()
	if len(c.remainingTasks) == 0 {
		c.mu.Unlock()
		return fmt.Errorf("no task to give")
	}
	// TODO: check if there is running map task, then we shouldn't give any reduce task out.
	// TODO: we should separated map task and reduce task right?
	// TODO: even if we handle map task out, we shouldn't drop it, should just keep it in memory to make sure the task can be run successfully.
	remainingTask := &c.remainingTasks[0]
	c.remainingTasks = c.remainingTasks[1:]
	// TODO: might try to use defer later;
	c.mu.Unlock()

	task.Kind = remainingTask.Kind
	task.MapInputFile = remainingTask.MapInputFile

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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _, file := range files {
		task := Task{
			Kind:         MapTask,
			MapInputFile: file,
		}
		c.remainingTasks = append(c.remainingTasks, task)
	}

	c.server()
	return &c
}
