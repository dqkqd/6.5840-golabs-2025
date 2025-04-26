package raft

import (
	"6.5840/raftapi"
)

// looping and applying log to the state machine
func (rf *Raft) applier() {
	for !rf.killed() {
		<-rf.commitIndexChangedCh

		logs := make([]raftLog, 0)
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			logs = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

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

	}
}
