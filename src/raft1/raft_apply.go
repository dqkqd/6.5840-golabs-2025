package raft

import (
	"6.5840/raftapi"
)

// looping and applying log to the state machine
func (rf *Raft) applier() {
	for !rf.killed() {
		commitIndex := <-rf.commitIndexCh

		if rf.lastApplied < commitIndex {
			rf.mu.Lock()
			logs := rf.log[rf.lastApplied+1 : commitIndex+1]
			rf.mu.Unlock()
			rf.lastApplied = commitIndex
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
}
