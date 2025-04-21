package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type serverState string

const (
	Leader    serverState = "Leader"
	Follower  serverState = "Follower"
	Candidate serverState = "Candidate"
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
	state             serverState           // state of the server: leader, follower, or candidate
	electionNotifier  waitNotifier          // notify and reset election timeout)
	heartbeatNotifier waitNotifier          // notify heartbeat
	applyCh           chan raftapi.ApplyMsg // apply channel to state machine
	electionTimeout   time.Duration         // current election timeout
	replicating       []bool                // boolean array indicate whether an server is replicating

	// record the last time we received append entries, or voted for someone, to determine whether we should start an election
	lastAppendEntriesTime electionRecordTimer
	lastVotedForTime      electionRecordTimer
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil {
		log.Fatalf("Cannot persist log")
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var rlog []raftLog
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rlog) != nil {
		log.Fatal("Cannot read persist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = rlog
	}
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
	// optimization
	XTerm  int // Term in the conflicting entry, -1 means empty
	XIndex int // index of the first entry with that term, -1 means empty
	XLen   int // follower's log length, -1 means empty
}

func (args AppendEntriesArgs) Format(f fmt.State, c rune) {
	switch c {
	case 'v':
		if f.Flag('+') {
			lastEntries := []raftLog{}
			if len(args.Entries) > 0 {
				lastEntries = append(lastEntries, args.Entries[len(args.Entries)-1])
			}

			fmt.Fprintf(f,
				"{Term:%+v LeaderId:%+v PrevLogIndex:%+v PrevLogTerm:%+v entriesSize:%+v lastEntries:%+v LeaderCommit:%+v}",
				args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), lastEntries, args.LeaderCommit,
			)
		}
	default:
		fmt.Fprintf(f, "%v", args)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply's default value
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1
	reply.Success = false

	DPrintf(tReceiveAppend, "S%d(%d,%v) <- S%d(%d), receive append %+v", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, args)

	// AppendEntries rule 1: reply false for smaller term from leader
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		DPrintf(tReceiveAppend, "S%d(%d,%v) <- S%d(%d), append reject, lower term, currentTerm=%d", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// Rule for Server: candidate, convert to follower if receive append entries from leader
	if rf.state == Candidate {
		rf.changeState(Follower)
	}

	// Rules for Servers: lower term, change to follower
	rf.maybeChangeTerm(args.Term)

	// record append entries timer
	rf.lastAppendEntriesTime.refresh(args.LeaderId, args.Term)

	reply.Term = args.Term

	// AppendEntries rule 2: reply false
	// if log doesn't contain entry at prevLogIndex whose term match prevLogTerm
	if len(rf.log) <= args.PrevLogIndex {
		// follower's log is too short
		reply.XLen = len(rf.log)
		DPrintf(tReceiveAppend,
			"S%d(%d,%v) <- S%d(%d), append reject, follower's log is too short, len(log)=%d",
			rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, len(rf.log),
		)
		return
	}
	// conflict term
	xTerm := rf.log[args.PrevLogIndex].Term
	if xTerm != args.PrevLogTerm {
		reply.XTerm = xTerm
		reply.XIndex = rf.findFirstIndexWithTerm(reply.XTerm)
		DPrintf(tReceiveAppend,
			"S%d(%d,%v) <- S%d(%d), append reject, conflict term, reply=%+v",
			rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, reply,
		)
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
	if len(args.Entries[index:]) > 0 {
		rf.log = append(rf.log, args.Entries[index:]...)
	}

	DPrintf(tReceiveAppend,
		"S%d(%d,%v) <- S%d(%d), append success, reply: %+v",
		rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, reply,
	)

	// persist the log
	rf.persist()

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

	DPrintf(tVote, "S%d(%d,%v) <- S%d(%d), receive request vote", rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term)

	// RequestVote rule 1: reply false if request's term < current term
	if rf.currentTerm > args.Term {
		DPrintf(tVote, "S%d(%d,%v) <- S%d(%d), higher term, do not vote", rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term)

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Rules for Servers: lower term, change to follower (but do not reply immediately)
	rf.maybeChangeTerm(args.Term)

	reply.Term = rf.currentTerm

	// RequestVote rule 2: voted for is null or candidate id
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// and candidate's log is as least as up-to-date
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			DPrintf(tVote,
				"S%d(%d,%v) <- S%d(%d), accept (term, index): S%d(%d, %d) <= S%d(%d, %d)",
				rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term,
				rf.me, lastLogTerm, lastLogIndex,
				args.CandidateId, args.LastLogTerm, args.LastLogIndex,
			)

			// then we should vote
			rf.vote(args.CandidateId)

			// record votedfor timer
			rf.lastVotedForTime.refresh(args.CandidateId, args.Term)

			reply.Term = args.Term
			reply.VoteGranted = true
		} else {
			DPrintf(tVote,
				"S%d(%d,%v) <- S%d(%d), reject (term, index): S%d(%d, %d) > S%d(%d, %d)",
				rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term,
				rf.me, lastLogTerm, lastLogIndex, args.CandidateId, args.LastLogTerm, args.LastLogIndex,
			)
		}
	} else {
		DPrintf(tVote,
			"S%d(%d,%v) <- S%d(%d), reject, already voted for S%d",
			rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term, rf.votedFor,
		)
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
	DPrintf(tVote, "S%d(%d,-) -> S%d(-), send request vote", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf(tSendAppend, "S%d(%d,-) -> S%d(-), send append entries", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) findFirstIndexWithTerm(term int) int {
	return sort.Search(len(rf.log), func(i int) bool {
		return rf.log[i].Term >= term
	})
}

// change term based on Rule for All Servers,
// lock must not be hold by caller
func (rf *Raft) maybeChangeTerm(term int) {
	if term > rf.currentTerm {
		DPrintf(tStatus, "S%d(%d,%v), change term to %d", rf.me, rf.currentTerm, rf.state, term)
		rf.currentTerm = term
		rf.vote(-1)
		rf.changeState(Follower)
		rf.persist()
	}
}

// leader initialization,
// lock must be hold by caller
func (rf *Raft) becomeLeader() {
	// only candidate can become leader
	if rf.state != Candidate {
		DPrintf(tBecomeLeader, "S%d(%d,%v) cannot become leader", rf.me, rf.currentTerm, rf.state)
		return
	}

	DPrintf(tBecomeLeader, "S%d(%d,%v) become leader", rf.me, rf.currentTerm, rf.state)
	rf.changeState(Leader)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peerId := range rf.peers {

		// volatile on leader on election: nextIndex = last log index + 1
		rf.nextIndex[peerId] = len(rf.log)

		// volatile on leader on election: matchIndex = 0
		rf.matchIndex[peerId] = 0
	}

	// put an no-op entry into the log
	rf.log = append(rf.log, raftLog{CommandIndex: rf.log[len(rf.log)-1].CommandIndex, Term: rf.currentTerm, LogEntryType: noOpLogEntry})
	rf.persist()
	// reset heartbeat and send heartbeat immediately
	rf.heartbeatNotifier.changeTimeout(heartbeatTimeout(), wakeupNow)
}

// get log command to send to the state machine,
// only run apply if lastApplied < commitIndex
func (rf *Raft) applyLog() (log raftLog, ok bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		return rf.log[rf.lastApplied], true
	}

	return
}

// looping and applying log to the state machine
func (rf *Raft) applier() {
	for !rf.killed() {
		log, ok := rf.applyLog()
		if ok {
			switch log.LogEntryType {

			case clientLogEntry:
				// wait on blocking channel, to avoid sending command out of order
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      log.Command,
					CommandIndex: log.CommandIndex,
				}
				DPrintf(tApply, "S%d(%d,-), apply, log: %+v", rf.me, log.Term, log)

			case noOpLogEntry:
				DPrintf(tApply, "S%d(%d,-), skip, log: %+v", rf.me, log.Term, log)
			}
		} else {
			// wait abit and check again later
			time.Sleep(30 * time.Millisecond)
		}

	}
}

// grant vote for someone, caller must hold lock
func (rf *Raft) vote(peer int) {
	rf.votedFor = peer

	if rf.votedFor == -1 {
		DPrintf(tVote, "S%d(%d,%v), reset vote", rf.me, rf.currentTerm, rf.state)
	} else {
		if rf.votedFor == rf.me {
			DPrintf(tVote, "S%d(%d,%v), vote for self", rf.me, rf.currentTerm, rf.state)
		} else {
			DPrintf(tVote, "S%d(%d,%v), vote for %d", rf.me, rf.currentTerm, rf.state, peer)
			rf.changeState(Follower)
		}
	}

	rf.persist()
}

func (rf *Raft) changeState(state serverState) {
	if rf.state != state {
		DPrintf(tStatus, "S%d(%d,%v), change state to %v", rf.me, rf.currentTerm, rf.state, state)
		rf.state = state
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// only leader can send heart beat
	if rf.state != Leader {
		return
	}

	DPrintf(tHeartbeat, "S%d(%d) send heartbeat", rf.me, rf.currentTerm)

	// send heartbeat
	for peerId := range rf.peers {
		go rf.replicate(peerId)
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

	index := rf.log[len(rf.log)-1].CommandIndex + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	// append the log
	entry := raftLog{Command: command, CommandIndex: index, Term: term, LogEntryType: clientLogEntry}
	rf.log = append(rf.log, entry)
	rf.persist()

	DPrintf(tStart, "S%d(%d), start agreement, entry: %+v", rf.me, rf.currentTerm, entry)
	for peerId := range rf.peers {
		go rf.replicate(peerId)
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

// do not burn cpu cycle
func retryTimeout() time.Duration {
	return 200 * time.Millisecond
}

func electionTimeout() time.Duration {
	// election timeout: between 800ms and 1300ms
	ms := 800 + (rand.Int63() % 500)
	return time.Duration(ms) * time.Millisecond
}

func heartbeatTimeout() time.Duration {
	// heartbeat timeout: 100ms
	return 100 * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {

		case <-rf.electionNotifier.wakeup:
			rf.elect()

		case <-rf.heartbeatNotifier.wakeup:
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
	logInit()

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh

	// start as follower
	rf.state = Follower

	// hasn't voted for any one
	rf.votedFor = -1

	// commit index and last applied
	rf.commitIndex = 0
	rf.lastApplied = 0

	// rafg log is 1-indexed, but we start with an entry at term 0
	rf.log = []raftLog{}
	rf.log = append(rf.log, raftLog{CommandIndex: 0, Term: 0, LogEntryType: noOpLogEntry})

	// initialize election and heartbeat notifier
	rf.electionTimeout = electionTimeout()
	rf.electionNotifier = waitNotify(rf.electionTimeout)
	rf.heartbeatNotifier = waitNotify(heartbeatTimeout())

	// allow to replicate to other servers
	rf.replicating = make([]bool, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// main loop
	go rf.ticker()

	// apply command
	go rf.applier()

	return rf
}
