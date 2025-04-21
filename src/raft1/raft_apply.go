package raft

import (
	"time"

	"6.5840/raftapi"
)

// looping and applying log to the state machine
func (rf *Raft) applier() {
	for !rf.killed() {
		logs, ok := rf.applyLog()
		if ok {
			for _, log := range logs {
				switch log.LogEntryType {

				case clientLogEntry:
					// wait on blocking channel, to avoid sending command out of order
					rf.applyCh <- raftapi.ApplyMsg{
						CommandValid: true,
						Command:      log.Command,
						CommandIndex: log.CommandIndex,
					}
					DPrintf(tApply, "S%d(%d,-), apply, log: %+v", rf.me, log.Term, log)

				case noOpLogEntry:
					DPrintf(tApply, "S%d(%d,-), skip, log: %+v", rf.me, log.Term, log)
				}
			}
		} else {
			// wait abit and check again later
			time.Sleep(30 * time.Millisecond)
		}

	}
}

// get log command to send to the state machine,
// only run apply if lastApplied < commitIndex
func (rf *Raft) applyLog() (logs []raftLog, ok bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < rf.commitIndex {
		logs = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
		rf.lastApplied = rf.commitIndex
		return logs, true
	}

	return
}
