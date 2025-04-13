package raft

type logEntryType int

const (
	noOpLogEntry logEntryType = iota
	clientLogEntry
)

type raftLog struct {
	Command      any          // state machine command
	Term         int          // term number
	LogEntryType logEntryType // whether this is no-op entry or normal entry from client
}
