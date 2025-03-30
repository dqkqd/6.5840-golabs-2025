package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type serverState int

const (
	Leader serverState = iota
	Follower
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int       // latest term server has seen (initialized to 0 on first boot, increase monotonically)
	votedFor    int       // candidateId that received vote in current term (or `-1` if none)
	log         []raftLog // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increase monotinically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increase monotonically)

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// optional fields
	state                 serverState  // state of the server: leader, follower, or candidate
	latestAppendEntriesAt time.Time    // the latest time we received the entries, since we only save this to compute the monotonical timediff, it is fine
	electionNotifier      waitNotifier // notify and reset election timeout)
	heartbeatNotifier     waitNotifier // notify heartbeat
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

type AppendEntriesArgs struct {
	Term         int       // leader's term
	LeaderId     int       // so follower can redirect clients
	PrevLogIndex int       // index of log entry immediately preceding new ones
	PrevLogTerm  int       // term of `PrevLogIndex` entry
	Entries      []raftLog // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int       // leader's `commitIndex`
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// record the time when we received the AppendEntries
	rf.latestAppendEntriesAt = time.Now()

	// AppendEntries rule 1: reply false for smaller term from leader
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Rules for Servers: lower term, change to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.LeaderId
	}

	reply.Term = args.Term

	// AppendEntries rule 2: reply false
	// if log doesn't contain entry at prevLogIndex whose term match prevLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true

	index := 0
	for index < len(args.Entries) {
		logIndex := args.PrevLogIndex + 1 + index
		if len(rf.log) <= logIndex {
			break
		}

		// AppendEntries rule 3: find conflicted entry with the same index but different term
		// and delete conflicted entry and all that follow it
		if rf.log[logIndex].Term != args.Entries[index].Term {
			rf.log = rf.log[:logIndex]
			break
		}

		index++
	}

	// AppendEntries rule 4: append any new entries not already in the log.
	// If we exited the loop earlier, then there were 2 cases:
	// - exceeding `rf.log` len.
	// - conflicting, and `rf.log` was truncated
	// in either case, we need to append the remaining entries.
	// otherwise, we traversed all entries thus the append does nothing.
	rf.log = append(rf.log, args.Entries[index:]...)

	// AppendEntries rule 5: change commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rules for Servers: lower term, change to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = Follower
		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}

	// RequestVote rule 1: reply false if request's term < current term
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// RequestVote rule 2: voted for is null or candidate id
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// and candidate's log is as least as up-to-date
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			// then we should vote
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVotes(args RequestVoteArgs) <-chan RequestVoteReply {
	// send RequestVoteRPCs to all other servers in parallel
	// TODO: need a way to stop looping, do we need a done ch?

	// result is collected in `votingCh`
	// buffered channel is used to avoid blocking
	votingCh := make(chan RequestVoteReply, len(rf.peers)-1)

	for serverId, peer := range rf.peers {
		if serverId != rf.me {
			args := args

			go func() {
				for !rf.killed() {
					reply := RequestVoteReply{}
					ok := peer.Call("Raft.RequestVote", &args, &reply)
					if ok {
						votingCh <- reply
						return
					}
					sleep()
				}
			}()
		}
	}

	return votingCh
}

// looping and send append entries to a peer until agreement is reached
func (rf *Raft) sendAppendEntries(peerId int, term int) {
	// do not send to self
	if rf.me == peerId {
		return
	}

	// TODO: stop this loop if it is elected again?
	// TODO: stop if we are not the leader any more? This requires lock.
	peer := rf.peers[peerId]

	for !rf.killed() {

		// construct entries for peer's args
		rf.mu.Lock()
		prevLogIndex := rf.nextIndex[peerId] - 1
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex].Term,
			Entries:      rf.log[prevLogIndex+1:],
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := peer.Call("Raft.AppendEntries", &args, &reply)
		if !ok {
			sleep()
			continue
		}

		// appending successfully, no need to send any append entries
		if reply.Success {
			return
		}

		// term has been changed from peer
		if reply.Term > args.Term {
			rf.changeTerm(reply.Term)
			return
		}

		// rejection, decrement nextIndex and retry
		rf.mu.Lock()
		rf.nextIndex[peerId]--
		rf.mu.Unlock()
	}
}

// change term based on Rule for All Servers,
// lock must not be hold
func (rf *Raft) changeTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
	}
}

func (rf *Raft) elect(currentElectionTimeout time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if we are follower, then this should be the first time we send votes.
	// if we are candidate, then we have been sending votes for a while but could not become the leader.
	// otherwise, we are leader and should not start election
	if rf.state != Follower && rf.state != Candidate {
		return
	}

	// If we are follower, we do not want to start an election if we receive heartbeat periodically
	if rf.state == Follower && time.Since(rf.latestAppendEntriesAt) < currentElectionTimeout {
		return
	}

	DPrintf("%d: start election", rf.me)

	rf.state = Candidate
	rf.currentTerm += 1 // increment current term
	rf.votedFor = rf.me // vote for self

	timeout := electionTimeout()
	rf.electionNotifier.changeTimeout(timeout) // reset election timeout

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}

	// voting starts in the background thread
	go func() {
		votingCh := rf.sendRequestVotes(args)

		// we have voted for ourselves already, so this should become 1
		totalVotes := 1

		// make sure we are not waiting all days
		electionTimeoutCh := time.After(timeout)

		// waiting from vote channels and timeout
		for !rf.killed() {
			select {

			// timeout
			case <-electionTimeoutCh:
				return

			case reply := <-votingCh:

				// Rules for Servers: lower term, change to follower
				if args.Term < reply.Term {
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
						DPrintf("%d: become leader", rf.me)
						rf.state = Leader

						rf.nextIndex = make([]int, len(rf.peers))
						for peerId := range rf.peers {
							rf.nextIndex[peerId] = len(rf.log)

							// send initial heartbeat to each servers
							go func() {
								rf.sendAppendEntries(peerId, args.Term)
							}()
						}

						// reset heartbeat
						rf.heartbeatNotifier.changeTimeout(heartbeatTimeout())

					}
					return
				}
			}
		}
	}()
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// only leader can send heart beat
	if rf.state != Leader {
		return
	}

	DPrintf("%d: send heartbeat", rf.me)

	term := rf.currentTerm

	// send heartbeat, because leader initialized all `nextIndex` to be the last entry
	// in the log, so all `entries` should be empty.
	for peerId := range rf.peers {
		go func() {
			rf.sendAppendEntries(peerId, term)
		}()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	// TODO: persist the log to disk

	// append the log
	rf.log = append(rf.log, raftLog{command, term})

	for peerId := range rf.peers {
		go func() {
			rf.sendAppendEntries(peerId, term)
		}()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func electionTimeout() time.Duration {
	// election timeout: between 200ms and 300ms
	ms := 200 + (rand.Int63() % 100)
	return time.Duration(ms) * time.Millisecond
}

func heartbeatTimeout() time.Duration {
	// heartbeat timeout: 150ms
	return 150 * time.Millisecond
}

// loop shouldn't execute continuouslly without waiting
func sleep() {
	time.Sleep(10 * time.Millisecond)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {

		case currentElectionTimeout := <-rf.electionNotifier.waitedFor:
			rf.elect(currentElectionTimeout)

		case <-rf.heartbeatNotifier.waitedFor:
			rf.heartbeat()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// start as follower
	rf.state = Follower

	// hasn't voted for any one
	rf.votedFor = -1

	// rafg log is 1-indexed, but we start with an entry at term 0
	rf.log = []raftLog{}
	rf.log = append(rf.log, raftLog{Term: 0})

	// initialize election and heartbeat notifier
	rf.electionNotifier = waitNotify(electionTimeout())
	rf.heartbeatNotifier = waitNotify(heartbeatTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// main loop
	go rf.ticker()

	return rf
}
