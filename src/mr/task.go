package mr

type TaskKind int

const (
	MapTask TaskKind = iota
	ReduceTask
)

type Task struct {
	Kind TaskKind
	// input for map task
	MapInputFile string
}
