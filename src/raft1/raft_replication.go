package raft

import (
	"fmt"
	"sort"
	"time"
)

type replicateType int

const (
	sendAppendEntries replicateType = iota
	sendInstallSnapshot
)

// looping and send append entries to a peer until agreement is reached
func (rf *Raft) replicate(server int) {
	if !rf.canReplicate(server) {
		return
	}

	defer func() {
		// mark `replicating` as false so other process can run
		rf.lock("replicating[server]=false")
		rf.replicating[server] = false
		rf.unlock("replicating[server]=false")
	}()

loop:
	for !rf.killed() {
		var appendEntriesArgs AppendEntriesArgs
		var installSnapshotArgs InstallSnapshotArgs
		var repType replicateType

		// do not send append tries if we are not leader
		rf.lock(fmt.Sprintf("replicate %d", server))
		state := rf.state
		term := rf.currentTerm
		// make sure nextIndex does not exceed log length
		rf.nextIndex[server] = min(rf.nextIndex[server], len(rf.log))

		// whether we are sending append entries or snapshot
		if rf.nextIndex[server] >= 1 {
			repType = sendAppendEntries
			appendEntriesArgs = rf.appendEntriesArgs(rf.nextIndex[server])
		} else {
			repType = sendInstallSnapshot
			installSnapshotArgs = rf.installSnapshotArgs()
		}

		rf.unlock(fmt.Sprintf("replicate %d", server))

		if state != Leader {
			DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d,-), do not replicate", rf.me, term, state, server, term)
			return
		}

		switch repType {
		case sendAppendEntries:
			finished := rf.replicateAppendEntries(server, state, &appendEntriesArgs)
			if finished {
				break loop
			}

		case sendInstallSnapshot:
			finished := rf.replicateInstallSnapshot(server, state, &installSnapshotArgs)
			if finished {
				break loop
			}
		}
	}
}

func (rf *Raft) replicateAppendEntries(server int, state serverState, args *AppendEntriesArgs) (finished bool) {
	// `sendAppendEntries` can wait even if server reconnect, so we need to add timeout ourselves
	replyCh := make(chan AppendEntriesReply)

	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, args, &reply)
		if ok {
			replyCh <- reply
		}
	}()

	select {
	case <-time.After(retryTimeout()):
		DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d), retry", rf.me, args.Term, state, server, args.Term)
		return false
	case reply := <-replyCh:
		DPrintf(tSendAppend, "S%d(%d,-) -> S%d(%d,-), receive append entries reply", rf.me, args.Term, server, args.Term)
		finished := rf.handleAppendEntriesReply(server, args, &reply)
		if finished {
			DPrintf(tSendAppend, "S%d(%d,-) -> S%d(%d,-), finished append entries replication", rf.me, args.Term, server, args.Term)
		}
		return finished
	}
}

func (rf *Raft) replicateInstallSnapshot(server int, state serverState, args *InstallSnapshotArgs) (finished bool) {
	// `sendInstallSnapshot` can wait even if server reconnect, so we need to add timeout ourselves
	replyCh := make(chan InstallSnapshotReply)

	go func() {
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, args, &reply)
		if ok {
			replyCh <- reply
		}
	}()

	select {
	case <-time.After(retryTimeout()):
		DPrintf(tSendSnapshot, "S%d(%d,%v) -> S%d(%d), retry", rf.me, args.Term, state, server, args.Term)
		return false
	case reply := <-replyCh:
		DPrintf(tSendSnapshot, "S%d(%d,-) -> S%d(%d,-), receive install snapshot reply", rf.me, args.Term, server, args.Term)
		finished = rf.handleInstallSnapshotReply(server, args, &reply)
		if finished {
			DPrintf(tSendSnapshot, "S%d(%d,-) -> S%d(%d,-), finished install snapshot replication", rf.me, args.Term, server, args.Term)
		}
		return finished
	}
}

// handle returned append entries
// return the nextIndex to retry, or finished to indicate we shouldn't send out more any rpc
func (rf *Raft) handleAppendEntriesReply(server int, rawargs *AppendEntriesArgs, rawreply *AppendEntriesReply) (finished bool) {
	rf.lock("handleAppendEntriesReply")
	defer rf.unlock("handleAppendEntriesReply")
	finished = rf.handleTermMismatchReplicateReply(server, rawargs.Term, rawreply.Term, tSendAppend)
	if finished {
		return
	}

	// Offsetting the index
	args := *rawargs
	reply := *rawreply
	args.PrevLogIndex = rf.toCompactedIndex(args.PrevLogIndex)
	args.LeaderCommit = rf.toCompactedIndex(args.LeaderCommit)
	reply.XIndex.Value = rf.toCompactedIndex(reply.XIndex.Value)
	reply.XLen.Value = rf.toCompactedIndex(reply.XLen.Value)

	DPrintf(tSendAppend,
		"S%d(%d,%v) -> S%d(%d), handle append entries\n\targs=%+v\n\trawargs=%+v\n\treply=%+v\n\trawreply=%+v",
		rf.me, args.Term, rf.state, server, reply.Term, args, rawargs, reply, rawreply,
	)

	// can be sure that current term, reply term, and args term match
	if reply.Success {
		return rf.handleSuccessAppendEntries(server, &args, &reply)
	} else {
		return rf.handleFailedAppendEntries(server, &args, &reply)
	}
}

// handle returned append entries
// return the nextIndex to retry, or finished to indicate we shouldn't send out more any rpc
func (rf *Raft) handleInstallSnapshotReply(server int, rawargs *InstallSnapshotArgs, rawreply *InstallSnapshotReply) (finished bool) {
	rf.lock("handleInstallSnapshotReply")
	defer rf.unlock("handleInstallSnapshotReply")
	finished = rf.handleTermMismatchReplicateReply(server, rawargs.Term, rawreply.Term, tSendSnapshot)
	if finished {
		return
	}
	args := *rawargs
	args.LastIncludedLogIndex = rf.toCompactedIndex(args.LastIncludedLogIndex)

	// update nextIndex and matchIndex
	rf.matchIndex[server] = args.LastIncludedLogIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	return rf.handleMatchIndexChanged(server, args.Term, rawreply.Term, tSendSnapshot)
}

// handle entries that was successfully sent
// return true if all the log are replicated
// otherwise, return false
func (rf *Raft) handleSuccessAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (finished bool) {
	DPrintf(tSendAppend, "S%d(%d,%v) -> S%d(%d), append entries succeeded", rf.me, args.Term, rf.state, server, reply.Term)
	// update nextIndex and matchIndex
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	return rf.handleMatchIndexChanged(server, args.Term, reply.Term, tSendAppend)
}

// handle entries that was replied with failure, leader must change `nextIndex` and retry
func (rf *Raft) handleFailedAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (finished bool) {
	// rejection, change nextIndex and retry

	DPrintf(
		tSendAppend,
		"S%d(%d,%v) -> S%d(%d), append entries failed, args=%+v, reply=%+v",
		rf.me, args.Term, rf.state, server, reply.Term, args, reply,
	)

	if reply.XTerm.Some && reply.XIndex.Some {
		// conflicting term
		lastXTermIndex, found := rf.log.findLastIndexWithTerm(reply.XTerm.Value)
		if found {
			// leader has XTerm
			rf.nextIndex[server] = lastXTermIndex + 1
		} else {
			// leader doesn't have XTerm
			rf.nextIndex[server] = reply.XIndex.Value
		}
	} else if reply.XLen.Some {
		// follower's log is too short
		rf.nextIndex[server] = reply.XLen.Value
	}

	DPrintf(tSendAppend,
		"S%d(%d,%v) -> S%d(%d), append entries failed, change nextIndex=%d",
		rf.me, rf.currentTerm, rf.state, server, reply.Term, rf.nextIndex[server],
	)

	// always return false indicating that we have not finished
	return false
}

func (rf *Raft) handleTermMismatchReplicateReply(server int, argsTerm, replyTerm int, topic logTopic) (finished bool) {
	// continue if the term match
	if rf.currentTerm == argsTerm && rf.currentTerm == replyTerm {
		return false
	}

	// the reply term doesn't match current term or args term
	// we check if we should change the term and then abort the handling
	DPrintf(topic, "S%d(%d,%v) -> S%d(%d), replicating failed, term changed, currentTerm=%d", rf.me, argsTerm, rf.state, server, replyTerm, rf.currentTerm)

	// Rules for Servers: lower term, change to follower
	// term has been changed from peer, change to follower and return immediately
	if rf.currentTerm < replyTerm {
		rf.maybeChangeTerm(replyTerm)
	}
	// otherwise, the reply is from previous round
	// it might be staled and incorrect. Should retry

	// only continue if we are still the leader
	if rf.state != Leader {
		DPrintf(topic, "S%d(%d,%v) -> S%d(%d), replicating failed, not a leader", rf.me, argsTerm, rf.state, server, replyTerm)
		return true
	} else {
		return false
	}
}

func (rf *Raft) handleMatchIndexChanged(server int, argsTerm, replyTerm int, topic logTopic) (finished bool) {
	rf.setCommitIndexAsMajorityReplicatedIndex()

	finished = rf.nextIndex[server] == len(rf.log)
	if !finished {
		DPrintf(topic,
			"S%d(%d,%v) -> S%d(%d), len(log)=%d > nextIndex=%d, need another append entries round",
			rf.me, argsTerm, rf.state, server, replyTerm, len(rf.log), rf.nextIndex[server],
		)
	}

	return finished
}

func (rf *Raft) canReplicate(server int) bool {
	if rf.me == server {
		DPrintf(tSendAppend, "S%d(-), do not send append entries to self", rf.me)
		return false
	}

	// check whether another process is replicating
	rf.lock("canReplicate")
	defer rf.unlock("canReplicate")
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

// create appendEntriesArgs at certain index
func (rf *Raft) appendEntriesArgs(at int) AppendEntriesArgs {
	entries := make(raftLog, len(rf.log[at:]))
	copy(entries, rf.log[at:])
	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.toRawIndex(at - 1),
		PrevLogTerm:  rf.log[at-1].Term,
		Entries:      entries,
		LeaderCommit: rf.toRawIndex(rf.commitIndex),
	}
}

func (rf *Raft) installSnapshotArgs() InstallSnapshotArgs {
	data := make([]byte, len(rf.snapshot))
	copy(data, rf.snapshot)
	return InstallSnapshotArgs{
		Term:                     rf.currentTerm,
		LeaderId:                 rf.me,
		LastIncludedCommandIndex: rf.log[0].CommandIndex,
		LastIncludedLogIndex:     rf.toRawIndex(0),
		LastIncludedTerm:         rf.log[0].Term,
		Data:                     data,
	}
}

func (rf *Raft) setCommitIndexAsMajorityReplicatedIndex() {
	// find majority matchIndex `n` such that 0 <= n < len(log),
	// we instead find the first index that the majority doesn't hold.
	// If the majority doesn't hold for `i`, then it doesn't hold for `i + 1` and so on,
	// then `n = index - 1` (the largest index that the majority holds true)
	//
	// we can always sure that commitIndex >= 0 (because it is replicated in the state machine)
	// but follower's matchIndex can negative (incase that follower need to catch up using snapshot)
	// so `n` might not always exist
	index := sort.Search(len(rf.log), func(i int) bool {
		count := 1 // self should always be counted
		for server := range rf.peers {
			if rf.matchIndex[server] >= i {
				count++
			}
		}
		return count*2 <= len(rf.peers)
	})
	n := index - 1

	// we don't commit already committed indexes
	if n <= rf.commitIndex {
		return
	}

	// only check the largest `n` such that `rf.log[n].Term == currentTerm`,
	nMax, found := rf.log.findLastIndexWithTerm(rf.currentTerm)
	if !found {
		return
	}
	n = min(n, nMax)
	if rf.log[n].Term != rf.currentTerm {
		return
	}

	rf.commitIndex = n
	go func() { rf.commitIndexChangedCh <- true }()
}
