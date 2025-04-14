package raft

type logEntryType int

const (
	noOpLogEntry logEntryType = iota
	clientLogEntry
)

type raftLog struct {
	Command      any          // state machine command
	CommandIndex int          // index of the log in state machine (ignoring no-op entry)
	Term         int          // term number
	LogEntryType logEntryType // whether this is no-op entry or normal entry from client
}
