package raft

import (
	"cmp"
	"slices"
	"sort"
)

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

func (r raftLog) findFirstIndexWithTerm(term int) (int, bool) {
	return slices.BinarySearchFunc(r, term, func(e raftLogEntry, term int) int {
		return cmp.Compare(e.Term, term)
	})
}

func (r raftLog) findLastIndexWithTerm(term int) (int, bool) {
	// find the first index of term + 1, then subtract by 1
	index, _ := r.findFirstIndexWithTerm(term + 1)
	index -= 1

	if index >= 0 && index < len(r) && r[index].Term == term {
		return index, true
	}
	return index, false
}

func (r raftLog) findLastCommandIndex(commandIndex int) (int, bool) {
	index := sort.Search(len(r), func(i int) bool {
		return r[i].CommandIndex >= commandIndex+1
	})
	index -= 1

	if index >= 0 && index < len(r) && r[index].CommandIndex == commandIndex {
		return index, true
	}
	return index, false
}
