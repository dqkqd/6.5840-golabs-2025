package raft

import "sort"

type logEntryType int

const (
	noOpLogEntry logEntryType = iota
	clientLogEntry
)

type raftLogEntry struct {
	Command      any          // state machine command
	CommandIndex int          // index of the log in state machine (ignoring no-op entry)
	LogIndex     int          // the real log index without compaction
	Term         int          // term number
	LogEntryType logEntryType // whether this is no-op entry or normal entry from client
}

type raftLog []raftLogEntry

func (r raftLog) findFirstIndexWithTerm(term int) int {
	return sort.Search(len(r), func(i int) bool {
		return r[i].Term >= term
	})
}

func (r raftLog) findLastIndexWithTerm(term int) int {
	// find the first index of term + 1, then subtract by 1
	return r.findFirstIndexWithTerm(term+1) - 1
}
