package raft

// looping and send append entries to a peer until agreement is reached
func (rf *Raft) replicate(peerId int, args AppendEntriesArgs) {
	// do not send to self
	if rf.me == peerId {
		return
	}

	// TODO: stop this loop if it is elected again?
	// TODO: stop if we are not the leader any more? This requires lock.
	peer := rf.peers[peerId]

	for !rf.killed() {

		// do not send append tries if we are not leader
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf(tSendAppend, "S%d(%d) -> S%d(-), send append entries, args=%+v", rf.me, rf.currentTerm, peerId, args)
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		peer.Call("Raft.AppendEntries", &args, &reply)

		// appending successfully, no need to send any append entries
		if reply.Success {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append entries success", rf.me, rf.currentTerm, peerId, reply.Term)

			// update nextIndex and matchIndex
			rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1

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

			return
		}

		// Rules for Servers: lower term, change to follower
		// term has been changed from peer,
		// do not send `AppendEntries` for this term and return immediately
		if reply.Term > args.Term {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append failed at term %d, maybe change to follower", rf.me, rf.currentTerm, peerId, reply.Term, args.Term)
			rf.changeTerm(reply.Term)
			return
		}

		// rejection, change nextIndex and retry
		rf.mu.Lock()

		if reply.XTerm != -1 && reply.XIndex != -1 {
			// conflicting term

			// find the last index of XTerm (the index before the first XTerm + 1)
			lastXTermIndex := rf.findFirstIndexWithTerm(reply.XTerm+1) - 1

			if lastXTermIndex >= 0 && rf.log[lastXTermIndex].Term == reply.XTerm {
				// leader has XTerm
				rf.nextIndex[peerId] = lastXTermIndex + 1
			} else {
				// leader doesn't have XTerm
				rf.nextIndex[peerId] = reply.XIndex
			}
		} else {
			// follower's log is too short
			rf.nextIndex[peerId] = reply.XLen
		}

		DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append rejected, nextIndex=%d, reply=%+v", rf.me, rf.currentTerm, peerId, reply.Term, rf.nextIndex[peerId], reply)

		args = rf.appendEntriesArgs(rf.nextIndex[peerId])

		rf.mu.Unlock()
	}
}
