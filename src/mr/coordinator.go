package mr

import (
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

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Distribute task to worker.
//
// Coordinator will first try to distribute map task.
// If all map tasks are running, coordinator should wait.
// Only if there is no map task, coordinator then looks at reduce tasks.
func (c *Coordinator) DistributeTask(workerArgs *int, task *RpcTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task.Task.NReduce = c.nReduce

	if len(c.mapTasks) > 0 {
		// Worker should run map tasks first
		for i, mapTask := range c.mapTasks {
			// Found a non-running task, give it to worker
			if !mapTask.running {
				task.Task.Id = mapTask.Id
				task.Task.Kind = MapTask
				task.Status = Ok
				task.Task.MapFilename = mapTask.MapFilename

				// mark this task as running, so other workers cannot access it
				c.mapTasks[i].running = true
				log.Printf("distributed task `%+v` for worker `%+v`", c.mapTasks[i], *workerArgs)
				return nil
			}
		}
		log.Printf("all map tasks are running, worker `%+v` should wait", *workerArgs)
		task.Status = Wait
	} else if len(c.reduceTasks) > 0 {
		// All map tasks are done, worker should run reduce tasks
		for i, reduceTask := range c.reduceTasks {
			if !reduceTask.running {
				task.Task.Id = reduceTask.Id
				task.Task.Kind = ReduceTask
				task.Status = Ok

				// mark this task as running, so other workers cannot acess it
				c.reduceTasks[i].running = true
				log.Printf("distributed task: `%+v` for worker `%+v`", c.reduceTasks[i], *workerArgs)
				return nil
			}
		}
		log.Printf("all reduce tasks are running, worker `%+v` should wait", *workerArgs)
		task.Status = Wait
	} else {
		log.Printf("all tasks are finished, worker `%+v` should abort", *workerArgs)
		task.Status = Stopped
	}

	return nil
}

func (c *Coordinator) FinishTask(task *Task, _ *int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch task.Kind {

	case MapTask:
		mapTaskSize := len(c.mapTasks)
		c.mapTasks = slices.DeleteFunc(c.mapTasks, func(t Task) bool { return t.Id == task.Id })
		if mapTaskSize != len(c.mapTasks)+1 {
			log.Fatalf("unable to delete map task: `%+v`", *task)
		}
		reduceTask := Task{
			Id:      task.Id,
			Kind:    ReduceTask,
			NReduce: c.nReduce,
			running: false,
		}
		log.Printf("create a new reduce task: `%+v`\n", reduceTask)
		c.reduceTasks = append(c.reduceTasks, reduceTask)

	case ReduceTask:
		mapTaskSize := len(c.reduceTasks)
		c.reduceTasks = slices.DeleteFunc(c.reduceTasks, func(t Task) bool { return t.Id == task.Id })
		if mapTaskSize != len(c.reduceTasks)+1 {
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
	c := Coordinator{}
	c.nReduce = nReduce

	// Your code here.
	for i, file := range files {
		task := Task{
			Id:          i,
			running:     false,
			Kind:        MapTask,
			MapFilename: file,
		}
		c.mapTasks = append(c.mapTasks, task)
	}

	c.server()
	return &c
}
