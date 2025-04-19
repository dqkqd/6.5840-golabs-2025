package raft

import "time"

// looping and send append entries to a peer until agreement is reached
func (rf *Raft) replicate(server int, nextIndex int) {
	if !rf.canReplicate(server) {
		return
	}

	for !rf.killed() {

		// do not send append tries if we are not leader
		rf.mu.Lock()
		state := rf.state
		if state != Leader {
			rf.mu.Unlock()
			break
		}
		args := rf.appendEntriesArgs(nextIndex)
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), cannot send append entries, retry", rf.me, args.Term, server, args.Term)
			time.Sleep(sleepTimeout())
			continue
		}

		argsNextIndex, finished := rf.handleAppendEntriesReply(server, &args, &reply)
		nextIndex = argsNextIndex
		if finished {
			DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), finished append entries", rf.me, args.Term, server, args.Term)
			break
		}
	}

	// mark `replicating` as false so other process can run
	rf.mu.Lock()
	rf.replicating[server] = false
	rf.mu.Unlock()
}

// handle returned append entries
// return the nextIndex to retry, or finished to indicate we shouldn't send out more any rpc
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (nextIndex int, finished bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), handle append entries", rf.me, args.Term, server, reply.Term)

	// Rules for Servers: lower term, change to follower
	// term has been changed from peer, change to follower and return immediately
	if reply.Term > rf.currentTerm {
		DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append failed, change to follower", rf.me, args.Term, server, reply.Term)
		rf.changeTerm(reply.Term)
		return
	}

	if reply.Success {
		nextIndex, finished = rf.handleSuccessAppendEntries(server, args, reply)
	} else {
		nextIndex = rf.handleFailedAppendEntries(server, args, reply)
	}

	return
}

func (rf *Raft) canReplicate(server int) bool {
	if rf.me == server {
		DPrintf(tSendAppend, "S%d(-), do not send append entries to self", rf.me)
		return false
	}

	// check whether another process is replicating
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.replicating[server] {
		DPrintf(tSendAppend, "S%d(-) -> S%d(-), already replicating", rf.me, server)
		return false
	}
	// make this as true, so other process cannot run
	rf.replicating[server] = true
	DPrintf(tSendAppend, "S%d(-) -> S%d(-), start replicating", rf.me, server)
	return true
}

// handle entries that was successfully sent
// return true if all the log are replicated
// otherwise, return false
func (rf *Raft) handleSuccessAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (nextIndex int, finished bool) {
	DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append entries succeeded", rf.me, args.Term, server, reply.Term)

	// update nextIndex and matchIndex
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	// find majority replicated entries to commit
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		// TODO: might not need this check?
		if rf.log[n].Term == rf.currentTerm {
			replicatedCount := 1 // self should be count
			for i := range rf.peers {
				if rf.me != i && rf.matchIndex[i] >= n {
					replicatedCount++
				}
			}
			// majority of servers have been replicated
			if replicatedCount*2 > len(rf.peers) {
				rf.commitIndex = n
				go rf.applyCommand()
				break
			}
		}
	}

	nextIndex = rf.nextIndex[server]
	finished = nextIndex == len(rf.log)

	if !finished {
		DPrintf(tSendAppend,
			"S%d(%d) -> S%d(%d), len(log)=%d > nextIndex=%d, need another append entries round",
			rf.me, args.Term, server, reply.Term, len(rf.log), rf.nextIndex[server],
		)
	}

	return nextIndex, finished
}

// handle entries that was replied with failure, leader must change `nextIndex` and retry
func (rf *Raft) handleFailedAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (nextIndex int) {
	// rejection, change nextIndex and retry

	DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append entries failed, args=%+v, reply=%+v", rf.me, rf.currentTerm, server, reply.Term, args, reply)

	if reply.XTerm != -1 && reply.XIndex != -1 {
		// conflicting term

		// find the last index of XTerm (the index before the first XTerm + 1)
		lastXTermIndex := rf.findFirstIndexWithTerm(reply.XTerm+1) - 1

		if lastXTermIndex >= 0 && rf.log[lastXTermIndex].Term == reply.XTerm {
			// leader has XTerm
			rf.nextIndex[server] = lastXTermIndex + 1
		} else {
			// leader doesn't have XTerm
			rf.nextIndex[server] = reply.XIndex
		}
	} else {
		// follower's log is too short
		rf.nextIndex[server] = reply.XLen
	}

	DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append entries failed, change nextIndex=%d", rf.me, rf.currentTerm, server, reply.Term, rf.nextIndex[server])

	return rf.nextIndex[server]
}
