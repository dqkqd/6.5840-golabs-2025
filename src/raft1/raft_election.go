package raft

import "time"

type requestVoteReplyWithServerId struct {
	server int
	reply  RequestVoteReply
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if we are follower, then this should be the first time we send votes.
	// if we are candidate, then we have been sending votes for a while but could not become the leader.
	// otherwise, we are leader and should not start election
	if rf.state == Leader {
		return
	}

	DPrintf(tElection, "S%d(%d,%v), start election", rf.me, rf.currentTerm, rf.state)

	rf.maybeChangeTerm(rf.currentTerm + 1)
	rf.vote(rf.me)
	rf.changeState(Candidate)

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}

	// voters send vote through this channel, it's need to be buffered.
	voteCh := make(chan requestVoteReplyWithServerId, len(rf.peers)-1)

	// send votes in parallel in the background
	go rf.sendVotes(voteCh, args)

	// collect votes in the background
	go rf.collectVotes(voteCh, args.Term)
}

func (rf *Raft) sendVotes(voteCh chan<- requestVoteReplyWithServerId, args RequestVoteArgs) {
	for server := range rf.peers {
		if server != rf.me {
			go func() {
				// make sure we are not waiting all days
				electionTimeoutCh := time.After(rf.electionTimeout)

				for !rf.killed() {
					rf.mu.Lock()
					state := rf.state
					term := rf.currentTerm
					rf.mu.Unlock()

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
					case <-electionTimeoutCh:
						return
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
	// make sure we are not waiting all days
	electionTimeoutCh := time.After(rf.electionTimeout)

	// we have voted for ourselves already, so this should be 1
	totalVotes := 1

	// waiting from vote channels and timeout
	for !rf.killed() {
		select {

		// timeout
		case <-electionTimeoutCh:
			return

		case r := <-voteCh:
			nextTotalVotes, finished := rf.handleRequestVoteReply(electionTerm, r.server, &r.reply, totalVotes)
			if finished {
				return
			}
			totalVotes = nextTotalVotes
		}
	}
}

// handle request vote reply from server,
func (rf *Raft) handleRequestVoteReply(electionTerm int, server int, reply *RequestVoteReply, totalVotes int) (nextTotalVotes int, finished bool) {
	if reply.VoteGranted {
		totalVotes += 1
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), received request vote reply %+v", rf.me, electionTerm, rf.state, server, reply.Term, reply)

	if rf.state != Candidate {
		DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), only candidate can collect votes", rf.me, electionTerm, rf.state, server, reply.Term)
		return totalVotes, true
	}

	// Rules for Servers: lower term, change to follower
	if rf.currentTerm < reply.Term || electionTerm < reply.Term {
		DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), received higher term, currentTerm=%d", rf.me, electionTerm, rf.state, server, reply.Term, rf.currentTerm)
		rf.maybeChangeTerm(reply.Term)
		return totalVotes, true
	}

	// staled term
	if rf.currentTerm != electionTerm {
		DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), term changed during election, currentTerm=%d", rf.me, electionTerm, rf.state, server, reply.Term, rf.currentTerm)
		return totalVotes, true
	}

	if reply.VoteGranted {
		DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), granted, totalVotes=%d", rf.me, electionTerm, rf.state, server, reply.Term, totalVotes)
	} else {
		DPrintf(tVote, "S%d(%d,%v) -> S%d(%d), no granted, totalVotes=%d", rf.me, electionTerm, rf.state, server, reply.Term, totalVotes)
	}

	// if votes received from majority of servers: become leader
	if totalVotes*2 > len(rf.peers) {
		rf.becomeLeader()
		return totalVotes, true
	}

	return totalVotes, false
}
