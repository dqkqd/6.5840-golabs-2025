package raft

import "time"

type requestVoteReplyWithServerId struct {
	server int
	reply  RequestVoteReply
}

type electionRecordTimer struct {
	server int
	term   int
	at     time.Time
}

func (t *electionRecordTimer) refresh(server int, term int) {
	t.server = server
	t.term = term
	t.at = time.Now()
}

func (rf *Raft) elect() {
	rf.lock("elect")
	defer rf.unlock("elect")

	if !rf.shouldStartElection() {
		return
	}

	DPrintf(tElection, "S%d(%d,%v), election started", rf.me, rf.currentTerm, rf.state)

	rf.maybeChangeTerm(rf.currentTerm + 1)
	rf.vote(rf.me)
	rf.changeState(Candidate)
	rf.resetElectionTimeout()

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.toRawIndex(lastLogIndex),
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}

	// voters send vote through this channel, it's need to be buffered.
	voteCh := make(chan requestVoteReplyWithServerId, len(rf.peers)-1)

	// send votes in the background
	go rf.sendVotes(voteCh, args)

	// collect votes in the background
	go rf.collectVotes(voteCh, args.Term)
}

func (rf *Raft) sendVotes(voteCh chan<- requestVoteReplyWithServerId, args RequestVoteArgs) {
	for server := range rf.peers {
		if server != rf.me {
			go func() {
				for !rf.killed() {
					rf.lock("sendVotes")
					state := rf.state
					term := rf.currentTerm
					rf.unlock("sendVotes")

					if state != Candidate {
						DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), do not send request vote, not a candidate", rf.me, args.Term, state, server, args.Term)
						return
					}

					if term != args.Term {
						DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), do not send request vote, term changed=%d", rf.me, args.Term, state, server, args.Term, term)
						return
					}

					// `sendRequestVote` can wait even if server reconnect, so we need to add timeout ourselves
					replyCh := make(chan RequestVoteReply)
					go func() {
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(server, &args, &reply)
						if ok {
							replyCh <- reply
						}
					}()

					select {
					case <-time.After(retryTimeout()):
						DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), retry", rf.me, args.Term, state, server, args.Term)
						continue
					case reply := <-replyCh:
						DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), successfully sent", rf.me, args.Term, state, server, args.Term)
						voteCh <- requestVoteReplyWithServerId{server: server, reply: reply}
						return
					}
				}
			}()
		}
	}
}

func (rf *Raft) collectVotes(voteCh <-chan requestVoteReplyWithServerId, electionTerm int) {
	// we have voted for ourselves already, so this should be 1
	totalVotes := 1

	// waiting from vote channels and timeout
	for !rf.killed() {
		r := <-voteCh
		nextTotalVotes, finished := rf.handleRequestVoteReply(electionTerm, r.server, &r.reply, totalVotes)
		if finished {
			return
		}
		totalVotes = nextTotalVotes
	}
}

// handle request vote reply from server
// return the total votes and whether this election should be stopped
func (rf *Raft) handleRequestVoteReply(electionTerm int, server int, reply *RequestVoteReply, totalVotes int) (nextTotalVotes int, finished bool) {
	rf.lock("handleRequestVoteReply")
	defer rf.unlock("handleRequestVoteReply")

	DPrintf(tVote,
		"S%d(%d,%v) -> S%d(%d), received request vote reply %+v for election %d",
		rf.me, rf.currentTerm, rf.state, server, reply.Term, reply, electionTerm,
	)

	// the reply term doesn't match current term or election term
	// there is only 2 cases:
	// - server has higher term than the leader
	// - server has lower term than (or equal with) the leader but the election term is staled
	if reply.Term != rf.currentTerm || reply.Term != electionTerm {
		if rf.currentTerm < reply.Term {
			DPrintf(tVote,
				"S%d(%d,%v) -> S%d(%d), received higher term for election %d, abort",
				rf.me, rf.currentTerm, rf.state, server, reply.Term, electionTerm,
			)
			rf.maybeChangeTerm(reply.Term)
		} else {
			DPrintf(tVote,
				"S%d(%d,%v) -> S%d(%d), term changed during election %d, abort",
				rf.me, rf.currentTerm, rf.state, server, reply.Term, electionTerm,
			)
		}

		return totalVotes, true
	}

	if rf.state != Candidate {
		DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), only candidate can collect votes", rf.me, rf.currentTerm, rf.state, server, reply.Term)
		return totalVotes, true
	}

	if reply.VoteGranted {
		DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), granted, totalVotes=%d", rf.me, rf.currentTerm, rf.state, server, reply.Term, totalVotes)
		totalVotes += 1
	} else {
		DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), no granted, totalVotes=%d", rf.me, rf.currentTerm, rf.state, server, reply.Term, totalVotes)
	}

	// if votes received from majority of servers: become leader
	if totalVotes*2 > len(rf.peers) {
		rf.becomeLeader()
		return totalVotes, true
	}

	return totalVotes, false
}

func (rf *Raft) shouldStartElection() bool {
	DPrintf(tElection, "S%d(%d,%v), attempt to start an election", rf.me, rf.currentTerm, rf.state)

	// if we are follower, then this should be the first time we send votes.
	// if we are candidate, then we have been sending votes for a while but could not become the leader.
	// otherwise, we are leader and should not start election
	if rf.state == Leader {
		DPrintf(tElection, "S%d(%d,%v), leader cannot start election", rf.me, rf.currentTerm, rf.state)
		return false
	}

	now := time.Now()

	lastAppendEntriesTimeDiff := now.Sub(rf.lastAppendEntriesTime.at)
	if lastAppendEntriesTimeDiff <= rf.electionTimeout {
		DPrintf(
			tElection,
			"S%d(%d,%v), abort election, have just received append entries from S%d(%d), %05d <= %05d ",
			rf.me, rf.currentTerm, rf.state, rf.lastAppendEntriesTime.server, rf.lastAppendEntriesTime.term,
			lastAppendEntriesTimeDiff.Milliseconds(), rf.electionTimeout.Milliseconds(),
		)
		return false
	}

	lastVotedForTimeDiff := now.Sub(rf.lastVotedForTime.at)
	if lastVotedForTimeDiff <= rf.electionTimeout {
		DPrintf(
			tElection,
			"S%d(%d,%v), abort election, have just voted for S%d(%d), %05d <= %05d",
			rf.me, rf.currentTerm, rf.state, rf.lastVotedForTime.server, rf.lastVotedForTime.term,
			lastVotedForTimeDiff.Milliseconds(), rf.electionTimeout.Milliseconds(),
		)
		return false
	}

	return true
}

func (rf *Raft) resetElectionTimeout() {
	// reset election timeout
	rf.electionTimeout = electionTimeout()
	rf.electionModifier <- rf.electionTimeout
}
