package mr

type TaskKind int

const (
	MapTask TaskKind = iota
	ReduceTask
)

type Task struct {
	kind TaskKind
	// input for map task
	mapInputFile string
}
