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
	if rf.state != Follower && rf.state != Candidate {
		return
	}

	DPrintf(tElection, "S%d(%d), start election, state=%+v", rf.me, rf.currentTerm, rf.state)

	rf.changeTerm(rf.currentTerm + 1)
	rf.electionTimeout = electionTimeout()
	rf.vote(rf.me)
	rf.state = Candidate

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
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(server, &args, &reply)
					if ok {
						voteCh <- requestVoteReplyWithServerId{server: server, reply: reply}
						return
					}
					select {
					case <-time.After(sleepTimeout()):
						continue
					case <-electionTimeoutCh:
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
			reply := r.reply
			DPrintf(tVote, "S%d(%d) -> S%d(%d), received vote", rf.me, electionTerm, r.server, reply.Term)

			// Rules for Servers: lower term, change to follower
			// election for this term should be aborted
			if electionTerm < reply.Term {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				DPrintf(tVote, "S%d(%d) -> S%d(%d), higher term", rf.me, electionTerm, r.server, reply.Term)
				rf.changeTerm(reply.Term)
				return
			}

			if reply.VoteGranted {
				totalVotes += 1
				DPrintf(tVote, "S%d(%d) -> S%d(%d), granted, totalVotes=%d", rf.me, electionTerm, r.server, reply.Term, totalVotes)
			} else {
				DPrintf(tVote, "S%d(%d) -> S%d(%d), no granted, totalVotes=%d", rf.me, electionTerm, r.server, reply.Term, totalVotes)
			}

			// if votes received from majority of servers: become leader
			if totalVotes*2 > len(rf.peers) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// rf.currentTerm might be changed at somepoint, when we became leader, or when we became follower.
				// Hence, we need to check the current term again to make sure we are in the right term.
				if electionTerm == rf.currentTerm {
					rf.becomeLeader()
				}
				return
			}
		}
	}
}
