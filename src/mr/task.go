package mr

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
	running bool // whether a task is running
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
