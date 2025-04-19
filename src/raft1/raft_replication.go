package raft

// looping and send append entries to a peer until agreement is reached
func (rf *Raft) replicate(server int, args AppendEntriesArgs) {
	// do not send to self
	if rf.me == server {
		return
	}

	for !rf.killed() {

		// do not send append tries if we are not leader
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Leader {
			return
		}

		reply := AppendEntriesReply{}
		rf.sendAppendEntries(server, &args, &reply)

		// Rules for Servers: lower term, change to follower
		// term has been changed from peer,
		// do not send `AppendEntries` for this term and return immediately
		if reply.Term > args.Term {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append failed at term %d, might change to follower", rf.me, rf.currentTerm, server, reply.Term, args.Term)
			rf.changeTerm(reply.Term)
			return
		}

		if reply.Success {
			rf.handleSuccessAppendEntries(server, &args, &reply)
			// appending successfully, no need to send any append entries
			return
		} else {
			rf.handleFailedAppendEntries(server, &args, &reply)
		}
	}
}

// handle entries that was successfully sent
func (rf *Raft) handleSuccessAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
				return
			}
		}
	}
}

// handle entries that was replied with failure, leader must change `nextIndex` and retry
func (rf *Raft) handleFailedAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// rejection, change nextIndex and retry
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	*args = rf.appendEntriesArgs(rf.nextIndex[server])
}
