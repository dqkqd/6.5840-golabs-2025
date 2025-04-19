package raft

// looping and send append entries to a peer until agreement is reached
func (rf *Raft) replicate(server int, nextIndex int) {
	// do not send to self
	if rf.me == server {
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
		rf.sendAppendEntries(server, &args, &reply)

		argsNextIndex, finished := rf.handleAppendEntriesReply(server, &args, &reply)
		nextIndex = argsNextIndex
		if finished {
			break
		}
	}
}

// handle returned append entries
// return the nextIndex to retry, or finished to indicate we shouldn't send out more any rpc
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (nextIndex int, finished bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rules for Servers: lower term, change to follower
	// term has been changed from peer, change to follower and return immediately
	if reply.Term > rf.currentTerm {
		DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append entries failed, might change to follower", rf.me, rf.currentTerm, server, reply.Term)
		rf.changeTerm(reply.Term)
		return
	}

	if reply.Success {
		nextIndex, finished = rf.handleSuccessAppendEntries(server, args, reply)
	} else {
		nextIndex = rf.handleFailedAppendEntries(server, reply)
	}

	return
}

// handle entries that was successfully sent
// return true if all the log are replicated
// otherwise, return false
func (rf *Raft) handleSuccessAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (nextIndex int, finished bool) {
	DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append entries success", rf.me, rf.currentTerm, server, reply.Term)

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
				rf.applyCommand()
				break
			}
		}
	}

	nextIndex = rf.nextIndex[server]
	return nextIndex, nextIndex == len(rf.log)
}

// handle entries that was replied with failure, leader must change `nextIndex` and retry
func (rf *Raft) handleFailedAppendEntries(server int, reply *AppendEntriesReply) (nextIndex int) {
	// rejection, change nextIndex and retry

	DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append entries failed, reply=%+v", rf.me, rf.currentTerm, server, reply.Term, reply)

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
