package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"io"
	"log"
	"math/rand"
	"os"
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
	currentTerm int64  // latest term server has seen (initialized to 0 on first boot, increase monotonically)
	votedFor    int    // candidateId that received vote in current term (or `-1` if none)
	log         []byte // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increase monotinically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increase monotonically)

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// optional fields
	state serverState // state of the server: leader, follower, or candidate
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	isleader = rf.state == Leader

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
	Term         int64  // leader's term
	LeaderId     int    // so follower can redirect clients
	PrevLogIndex int    // index of log entry immediately preceding new ones
	PrevLogTerm  int64  // term of `PrevLogIndex` entry
	Entries      []byte // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int    // leader's `commitIndex`
}

type AppendEntriesReply struct {
	Term    int64 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64 // candidate's term
	CandidateId  int   // candidate requesting vote
	LastLogIndex int   // index of candidate's last log entry
	LastLogTerm  int64 // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64 // CurrentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term {
		// become this candidate's follower
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.CandidateId

		reply.Term = args.Term
		reply.VoteGranted = true

	} else if rf.currentTerm > args.Term {
		// higher term, do not vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// equal term, we haven't vote or we have voted for this candidate already
		// TODO: compare log
		rf.votedFor = args.CandidateId

		reply.Term = args.Term
		reply.VoteGranted = true

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// only follower or candidate can start an election
	if rf.state != Follower && rf.state != Candidate {
		return
	}

	log.Printf("%d: start election", rf.me)

	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me

	voteCh := make(chan RequestVoteReply, len(rf.peers)-1)
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	// send request votes in parallel
	for serverId := range rf.peers {
		if serverId != rf.me {
			requestVoteArgs := requestVoteArgs

			go func() {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(serverId, &requestVoteArgs, &reply)
				if ok {
					voteCh <- reply
				} else {
					voteCh <- RequestVoteReply{Term: -1, VoteGranted: false}
				}
			}()
		}
	}

	votedTerm := rf.currentTerm

	// receive votes in the background
	go func() {
		// we have voted for ourselves already
		totalVotes := 1
		for reply := range voteCh {

			if reply.VoteGranted {
				totalVotes += 1
			}

			// someone has higher term, we should become follower right away
			if votedTerm < reply.Term {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// need to check again to make sure `rf.currentTerm` hasn't changed since
				if rf.currentTerm <= reply.Term {
					rf.state = Follower
				}
				return
			}

			// we can become leader
			if totalVotes*2 >= len(rf.peers) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// need to check again to make sure `rf.currentTerm` hasn't changed since
				if rf.currentTerm == votedTerm {
					log.Printf("%d: become leader", rf.me)
					rf.state = Leader
				}
				return
			}
		}
	}()
}

func (rf *Raft) lastLogIndex() int {
	// TODO: implement
	return 0
}

func (rf *Raft) lastLogTerm() int64 {
	// TODO: implement
	return 0
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

func (rf *Raft) mainBackgroundLoop(startElectionCh chan bool, startHeartbeatCh chan bool) {
	for !rf.killed() {
		select {

		case <-startElectionCh:
			rf.startElection()

		case <-startHeartbeatCh:
			// TODO: start heart beat
		}
	}
}

func (rf *Raft) electionTicker(startElectionCh chan bool) {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		// election timeout between 200ms and 300ms
		electionTimeout := 200 + (rand.Int63() % 100)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		// start election
		startElectionCh <- true
	}
}

func (rf *Raft) heartbeatTicker(startHeartbeatCh chan bool) {
	for !rf.killed() {
		// sleep 150 ms for heartbeat
		time.Sleep(150 * time.Millisecond)
		// start heartbeat
		startHeartbeatCh <- true
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
	// disable log for real test
	enableLog := os.Getenv("ENABLE_LOG") == "1"
	if !enableLog {
		log.SetOutput(io.Discard)
	}

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// start as follower
	rf.state = Follower

	startElectionCh := make(chan bool)
	startHeartbeatCh := make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker(startElectionCh)

	// start ticker goroutine to start heartbeat
	go rf.heartbeatTicker(startHeartbeatCh)

	// main loop
	go rf.mainBackgroundLoop(startElectionCh, startHeartbeatCh)

	return rf
}
