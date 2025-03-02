package mr

import "time"

type (
	TaskKind      int
	RpcTaskStatus int
)

const (
	MapTask TaskKind = iota
	ReduceTask
)

type Task struct {
	Id          int
	Kind        TaskKind
	NReduce     int    // number of reducer
	MapFilename string // input for map task

	// private fields, to help coordinator manage tasks, these fields are not sent through rpc
	running   bool // whether a task is running
	startedAt time.Time
}

// If we can schedule this task for worker
//
// A task can be scheduled if and only if:
//   - It is not running
//   - It has been running for more than 10 seconds but no response from worker (worker might be dead)
func (t Task) CanBeScheduled() bool {
	if !t.running {
		return true
	}

	elapsed := time.Since(t.startedAt)
	return elapsed.Seconds() > 10
}

func (t *Task) Schedule() {
	t.running = true
	t.startedAt = time.Now()
}

const (
	Wait RpcTaskStatus = iota
	Ok
	Stopped
)

type RpcTask struct {
	Task   Task
	Status RpcTaskStatus
}
