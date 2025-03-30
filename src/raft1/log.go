package raft

type raftLog struct {
	Command any // state machine command
	Term    int // term number
}
