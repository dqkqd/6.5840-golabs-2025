package raft

import "time"

// looping and send append entries to a peer until agreement is reached
func (rf *Raft) replicate(server int) {
	if !rf.canReplicate(server) {
		return
	}

	defer func() {
		// mark `replicating` as false so other process can run
		rf.mu.Lock()
		rf.replicating[server] = false
		rf.mu.Unlock()
	}()

loop:
	for !rf.killed() {

		// do not send append tries if we are not leader
		rf.mu.Lock()
		state := rf.state
		// make sure nextIndex does not exceed log length
		rf.nextIndex[server] = min(rf.nextIndex[server], len(rf.log))
		args := rf.appendEntriesArgs(rf.nextIndex[server])
		rf.mu.Unlock()

		if state != Leader {
			DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d,-), do not send append entries", rf.me, args.Term, state, server, args.Term)
			break
		}

		// `sendAppendEntries` can wait even if server reconnect, so we need to add timeout ourselves
		replyCh := make(chan AppendEntriesReply)
		go func() {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				replyCh <- reply
			}
		}()

		select {
		case <-time.After(retryTimeout()):
			DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d), retry", rf.me, args.Term, state, server, args.Term)
			continue
		case reply := <-replyCh:
			finished := rf.handleAppendEntriesReply(server, &args, &reply)
			if finished {
				DPrintf(tSendAppend, "S%d(%d,-) -> S%d(%d,-), finished append entries", rf.me, args.Term, server, args.Term)
				break loop
			}
		}
	}
}

// handle returned append entries
// return the nextIndex to retry, or finished to indicate we shouldn't send out more any rpc
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (finished bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d), handle append entries", rf.me, args.Term, rf.state, server, reply.Term)

	if reply.Term != rf.currentTerm {
		DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d), append failed, term changed, currentTerm=%d", rf.me, args.Term, rf.state, server, reply.Term, rf.currentTerm)

		// Rules for Servers: lower term, change to follower
		// term has been changed from peer, change to follower and return immediately
		if reply.Term > rf.currentTerm {
			rf.maybeChangeTerm(reply.Term)
		}
		// otherwise, the reply is from previous append entries round
		// it might be staled and incorrect. Should retry with different nextIndex

		finished = rf.state != Leader

		if finished {
			DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d), append failed, not a leader", rf.me, args.Term, rf.state, server, reply.Term)
		}

		return finished
	}

	if reply.Success {
		return rf.handleSuccessAppendEntries(server, args, reply)
	} else {
		return rf.handleFailedAppendEntries(server, args, reply)
	}
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
		DPrintf(tSendAppend, "S%d(-,%v) -> S%d(-), already replicating", rf.me, rf.state, server)
		return false
	} else {
		// make this as true, so other process cannot run
		rf.replicating[server] = true
		DPrintf(tSendAppend, "S%d(-,%v) -> S%d(-), start replicating", rf.me, rf.state, server)
		return true
	}
}

// handle entries that was successfully sent
// return true if all the log are replicated
// otherwise, return false
func (rf *Raft) handleSuccessAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (finished bool) {
	DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d), append entries succeeded", rf.me, args.Term, rf.state, server, reply.Term)

	// update nextIndex and matchIndex
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	// find majority replicated entries to commit
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
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
				break
			}
		}
	}

	finished = rf.nextIndex[server] == len(rf.log)

	if !finished {
		DPrintf(tSendAppend,
			"S%d(%d,%v) -> S%d(%d), len(log)=%d > nextIndex=%d, need another append entries round",
			rf.me, args.Term, rf.state, server, reply.Term, len(rf.log), rf.nextIndex[server],
		)
	}

	return finished
}

// handle entries that was replied with failure, leader must change `nextIndex` and retry
func (rf *Raft) handleFailedAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (finished bool) {
	// rejection, change nextIndex and retry

	DPrintf(
		tSendAppend,
		"S%d(%d,%v) -> S%d(%d), append entries failed, args=%+v, reply=%+v",
		rf.me, args.Term, rf.state, server, reply.Term, args, reply,
	)

	if reply.XTerm != -1 && reply.XIndex != -1 {
		// conflicting term

		lastXTermIndex := rf.findLastIndexWithTerm(reply.XTerm)

		if lastXTermIndex >= 0 && rf.log[lastXTermIndex].Term == reply.XTerm {
			// leader has XTerm
			rf.nextIndex[server] = lastXTermIndex + 1
		} else {
			// leader doesn't have XTerm
			rf.nextIndex[server] = reply.XIndex
		}
	} else if reply.XLen != -1 {
		// follower's log is too short
		rf.nextIndex[server] = reply.XLen
	}

	DPrintf(tSendAppend,
		"S%d(%d,%v) -> S%d(%d), append entries failed, change nextIndex=%d",
		rf.me, rf.currentTerm, rf.state, server, reply.Term, rf.nextIndex[server],
	)

	// always return false indicating that we have not finished
	return false
}

// create appendEntriesArgs at certain index
func (rf *Raft) appendEntriesArgs(at int) AppendEntriesArgs {
	entries := make([]raftLog, len(rf.log[at:]))
	copy(entries, rf.log[at:])

	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: at - 1,
		PrevLogTerm:  rf.log[at-1].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}
