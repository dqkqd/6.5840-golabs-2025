package raft

import "time"

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

	// make sure we are not waiting all days
	electionTimeoutCh := time.After(rf.electionTimeout)

	// voters send vote through this channel, it's need to be buffered.
	votingCh := make(chan RequestVoteReply, len(rf.peers)-1)

	// we have voted for ourselves already, so this should become 1
	totalVotes := 1

	// voting start in the background
	go func() {
		for server := range rf.peers {
			if server != rf.me {
				args := args

				go func() {
					reply := RequestVoteReply{}
					rf.sendRequestVote(server, &args, &reply)
					votingCh <- reply
				}()
			}
		}
	}()

	// collect votes in the background
	go func() {
		// waiting from vote channels and timeout
		for !rf.killed() {
			select {

			// timeout
			case <-electionTimeoutCh:
				return

			case reply := <-votingCh:

				// Rules for Servers: lower term, change to follower
				// election for this term should be aborted
				if args.Term < reply.Term {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.changeTerm(reply.Term)
					return
				}

				if reply.VoteGranted {
					totalVotes += 1
				}

				// if votes received from majority of servers: become leader
				if totalVotes*2 > len(rf.peers) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// rf.currentTerm might be changed at somepoint, when we became leader, or when we became follower.
					// Hence, we need to check the current term again to make sure we are in the right term.
					if args.Term == rf.currentTerm {
						rf.becomeLeader()
					}
					return
				}
			}
		}
	}()
}
