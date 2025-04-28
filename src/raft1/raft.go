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
	currentTerm int     // latest term server has seen (initialized to 0 on first boot, increase monotonically)
	votedFor    int     // candidateId that received vote in current term (or `-1` if none)
	log         raftLog // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	snapshot    []byte  // snapshot for log compaction

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increase monotinically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increase monotonically)

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// optional fields
	state                serverState           // state of the server: leader, follower, or candidate
	electionModifier     chan time.Duration    // whether to reset election timeout
	heartbeatTrigger     chan bool             // whether to trigger heartbeat now
	applyCh              chan raftapi.ApplyMsg // apply channel to state machine
	electionTimeout      time.Duration         // current election timeout
	replicating          []bool                // boolean array indicate whether an server is replicating
	commitIndexChangedCh chan bool
	snapshotChangedCh    chan bool

	// record the last time we received append entries, or voted for someone, to determine whether we should start an election
	lastAppendEntriesTime electionRecordTimer
	lastVotedForTime      electionRecordTimer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.lock("GetState")
	defer rf.unlock("GetState")
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
	rf.persister.Save(raftstate, rf.snapshot)
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
	var rlog raftLog
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rlog) != nil {
		log.Fatal("Cannot read persist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = rlog
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.lock("PersistBytes")
	defer rf.unlock("PersistBytes")
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(commandIndex int, snapshot []byte) {
	// Your code here (3D).
	rf.lock("Snapshot")
	defer rf.unlock("Snapshot")
	if rf.killed() {
		return
	}

	// find the last entry with commandIndex.
	entryIndex, found := rf.log.findLastCommandIndex(commandIndex)
	if !found {
		DPrintf(tSnapshot, "S%d(%d,%v), invalid commandIndex=%d", rf.me, rf.currentTerm, rf.state, commandIndex)
		log.Fatal("invalid snapshot")
	}

	rf.removeSnapshotLog(entryIndex, raftLogEntry{
		Command:      nil,
		CommandIndex: rf.log[entryIndex].CommandIndex,
		LogIndex:     rf.log[entryIndex].LogIndex,
		Term:         rf.currentTerm,
		LogEntryType: noOpLogEntry,
	})

	rf.snapshot = snapshot
	rf.persist()

	DPrintf(tSnapshot,
		"S%d(%d,%v) snapshot for commandIndex=%d, offset=%d",
		rf.me, rf.currentTerm, rf.state, commandIndex, rf.snapshotOffset(),
	)
}

type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of `PrevLogIndex` entry
	Entries      raftLog // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader's `commitIndex`
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	// optimization
	XTerm  OptionInt // Term in the conflicting entry
	XIndex OptionInt // index of the first entry with that term
	XLen   OptionInt // follower's log length
}

func (args AppendEntriesArgs) Format(f fmt.State, c rune) {
	switch c {
	case 'v':
		if f.Flag('+') {
			lastEntries := make(raftLog, 0)
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

func (rf *Raft) AppendEntries(rawargs *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.lock("AppendEntries")
	defer rf.unlock("AppendEntries")

	// Offsetting the args index
	args := *rawargs
	args.PrevLogIndex = rf.toCompactedIndex(args.PrevLogIndex)
	args.LeaderCommit = rf.toCompactedIndex(args.LeaderCommit)
	// Offsetting the reply index before return
	defer func() {
		reply.XIndex.Value = rf.toRawIndex(reply.XIndex.Value)
		reply.XLen.Value = rf.toRawIndex(reply.XLen.Value)
	}()

	// reply's default value
	reply.XTerm = OptionInt{Some: false}
	reply.XIndex = OptionInt{Some: false}
	reply.XLen = OptionInt{Some: false}
	reply.Success = false

	DPrintf(tReceiveAppend,
		"S%d(%d,%v) <- S%d(%d), receive append\n\targs=%+v\n\trawargs=%+v",
		rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, args, rawargs,
	)

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
		reply.XLen = OptionInt{Value: len(rf.log), Some: true}
		DPrintf(tReceiveAppend,
			"S%d(%d,%v) <- S%d(%d), append reject, follower's log is too short, len(log)=%d",
			rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, len(rf.log),
		)
		return
	}
	// conflict term
	xTerm := rf.log[args.PrevLogIndex].Term
	if xTerm != args.PrevLogTerm {
		xIndex, _ := rf.log.findFirstIndexWithTerm(xTerm)
		reply.XTerm = OptionInt{Value: xTerm, Some: true}
		reply.XIndex = OptionInt{Value: xIndex, Some: true}
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
		go func() { rf.commitIndexChangedCh <- true }()
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
func (rf *Raft) RequestVote(rawargs *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.lock("RequestVote")
	defer rf.unlock("RequestVote")

	// Offsetting the index
	args := *rawargs
	args.LastLogIndex = rf.toCompactedIndex(args.LastLogIndex)

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
				"S%d(%d,%v) <- S%d(%d), accept (term, index, rawIndex): S%d(%d, %d, %d) <= S%d(%d, %d, %d)",
				rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term,
				rf.me, lastLogTerm, lastLogIndex, rf.toRawIndex(lastLogIndex),
				args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.toRawIndex(args.LastLogIndex),
			)

			// then we should vote
			rf.vote(args.CandidateId)

			// record votedfor timer
			rf.lastVotedForTime.refresh(args.CandidateId, args.Term)

			reply.Term = args.Term
			reply.VoteGranted = true
		} else {
			DPrintf(tVote,
				"S%d(%d,%v) <- S%d(%d), reject (term, index, rawIndex): S%d(%d, %d, %d) > S%d(%d, %d, %d)",
				rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term,
				rf.me, lastLogTerm, lastLogIndex, rf.toRawIndex(lastLogIndex),
				args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.toRawIndex(args.LastLogIndex),
			)
		}
	} else {
		DPrintf(tVote,
			"S%d(%d,%v) <- S%d(%d), reject, already voted for S%d",
			rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term, rf.votedFor,
		)
	}
}

type InstallSnapshotArgs struct {
	Term                     int    // leader's term
	LeaderId                 int    // so follower can redirect clients
	LastIncludedCommandIndex int    // the snapshot command index
	LastIncludedLogIndex     int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm         int    // term of LastIncludedIndex
	Data                     []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (args InstallSnapshotArgs) Format(f fmt.State, c rune) {
	switch c {
	case 'v':
		if f.Flag('+') {
			fmt.Fprintf(f,
				"{Term:%+v LeaderId:%+v LastIncludedCommandIndex:%+v LastIncludedLogIndex:%+v LastIncludedTerm:%+v SnapshotSize:%+v}",
				args.Term, args.LeaderId, args.LastIncludedCommandIndex, args.LastIncludedLogIndex, args.LastIncludedTerm, len(args.Data),
			)
		}
	default:
		fmt.Fprintf(f, "%v", args)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")
	defer rf.unlock("InstallSnapshot")

	DPrintf(tReceiveSnapshot,
		"S%d(%d,%v) <- S%d(%d), receive snapshot %+v",
		rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, args,
	)

	// InstallSnapshot rule 1: reply false for smaller term from leader
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		DPrintf(tReceiveSnapshot,
			"S%d(%d,%v) <- S%d(%d), snapshot reject, lower term, currentTerm=%d",
			rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, rf.currentTerm,
		)
		return
	}

	// Rule for Server: candidate, convert to follower if receive append entries from leader
	if rf.state == Candidate {
		rf.changeState(Follower)
	}

	// Rules for Servers: lower term, change to follower
	rf.maybeChangeTerm(args.Term)

	// record append entries timer
	// TODO: timer
	rf.lastAppendEntriesTime.refresh(args.LeaderId, args.Term)

	reply.Term = args.Term

	// checking whether we receive an outdated snapshot
	if args.LastIncludedCommandIndex <= rf.log[0].CommandIndex {
		DPrintf(tReceiveSnapshot,
			"S%d(%d,%v) <- S%d(%d), snapshot reject, receive and outdated snapshot, commandIndex=%d <= currentIndex=%d",
			rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, args.LastIncludedCommandIndex, rf.log[0].CommandIndex,
		)
		return
	}

	rf.snapshot = make([]byte, len(args.Data))
	copy(rf.snapshot, args.Data)
	DPrintf(tReceiveSnapshot,
		"S%d(%d,%v) <- S%d(%d), wrote snapshot len=%d",
		rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, len(rf.snapshot),
	)

	zombieEntry := raftLogEntry{
		Command:      nil,
		CommandIndex: args.LastIncludedCommandIndex,
		LogIndex:     args.LastIncludedLogIndex,
		Term:         args.LastIncludedTerm,
		LogEntryType: noOpLogEntry,
	}
	// retain log entries if matching index with term is found
	index, found := rf.log.findLastCommandIndex(args.LastIncludedCommandIndex)
	if found {
		// retain the follow entries
		rf.removeSnapshotLog(index, zombieEntry)
	} else {
		// discard the logs
		rf.removeSnapshotLog(len(rf.log)-1, zombieEntry)
	}

	// persist the log and snapshot
	rf.persist()

	go func() {
		rf.snapshotChangedCh <- true
	}()
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf(tSendSnapshot, "S%d(%d,-) -> S%d(-), send install snapshot", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	rf.log = append(rf.log, raftLogEntry{
		CommandIndex: rf.log[len(rf.log)-1].CommandIndex,
		LogIndex:     rf.log[len(rf.log)-1].LogIndex + 1,
		Term:         rf.currentTerm,
		LogEntryType: noOpLogEntry,
	})
	rf.persist()
	// trigger heartbeat now
	rf.heartbeatTrigger <- true
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

// Remove entries in the log upto index.
// Put a zombie entry at the index 0.
func (rf *Raft) removeSnapshotLog(upto int, zombieEntry raftLogEntry) {
	// convert all the indexes to the real index before removing log entries
	rf.commitIndex = rf.toRawIndex(rf.commitIndex)
	rf.lastApplied = rf.toRawIndex(rf.lastApplied)
	if len(rf.matchIndex) > 0 && len(rf.nextIndex) > 0 {
		for i := range rf.peers {
			rf.matchIndex[i] = rf.toRawIndex(rf.matchIndex[i])
			rf.nextIndex[i] = rf.toRawIndex(rf.nextIndex[i])
		}
	}

	// convert all the real index back to the compacted index after removing log entries
	defer func() {
		// commit index and last applied should never less than 0
		// because we committed in the snapshot ourselves
		rf.commitIndex = max(0, rf.toCompactedIndex(rf.commitIndex))
		rf.lastApplied = max(0, rf.toCompactedIndex(rf.lastApplied))
		if len(rf.matchIndex) > 0 && len(rf.nextIndex) > 0 {
			for i := range rf.peers {
				rf.matchIndex[i] = rf.toCompactedIndex(rf.matchIndex[i])
				rf.nextIndex[i] = rf.toCompactedIndex(rf.nextIndex[i])
			}
		}
	}()

	newlog := make(raftLog, 0)
	newlog = append(newlog, zombieEntry)
	if upto+1 < len(rf.log) {
		newlog = append(newlog, rf.log[upto+1:]...)
	}
	rf.log = newlog
}

func (rf *Raft) snapshotOffset() int {
	return rf.log[0].LogIndex
}

func (rf *Raft) toRawIndex(index int) int {
	return index + rf.snapshotOffset()
}

func (rf *Raft) toCompactedIndex(index int) int {
	return index - rf.snapshotOffset()
}

func (rf *Raft) heartbeat() {
	rf.lock("heartbeat")
	defer rf.unlock("heartbeat")

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

	rf.lock("Start")
	defer rf.unlock("Start")

	index := rf.log[len(rf.log)-1].CommandIndex + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	// append the log
	entry := raftLogEntry{
		Command:      command,
		CommandIndex: index,
		LogIndex:     rf.log[len(rf.log)-1].LogIndex + 1,
		Term:         term, LogEntryType: clientLogEntry,
	}
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

func (rf *Raft) lock(name string) {
	rf.mu.Lock()
	DPrintf(tStatus, "S%d(%d) lock, %s", rf.me, rf.currentTerm, name)
}

func (rf *Raft) unlock(name string) {
	DPrintf(tStatus, "S%d(%d) unlock, %s", rf.me, rf.currentTerm, name)
	rf.mu.Unlock()
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
	heartbeatTimer := time.After(heartbeatTimeout())

	var electionTimeout time.Duration
	var electionTimer <-chan time.Time

	for !rf.killed() {
		select {

		// manually trigger heartbeat
		case <-rf.heartbeatTrigger:
			go rf.heartbeat()
			heartbeatTimer = time.After(heartbeatTimeout())

		// automatically trigger heartbeat
		case <-heartbeatTimer:
			go rf.heartbeat()
			heartbeatTimer = time.After(heartbeatTimeout())

		// manually modify electionTimeout
		case electionTimeout = <-rf.electionModifier:
			electionTimer = time.After(electionTimeout)

		// automatically trigger election
		case <-electionTimer:
			go rf.elect()
			electionTimer = time.After(electionTimeout)

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
	rf.log = make(raftLog, 0)
	rf.log = append(rf.log, raftLogEntry{
		CommandIndex: 0,
		LogIndex:     0,
		Term:         0,
		LogEntryType: noOpLogEntry,
	})

	// initialize election and heartbeat timeout channel
	rf.heartbeatTrigger = make(chan bool)

	initElectionTimeout := electionTimeout()
	rf.electionTimeout = initElectionTimeout
	rf.electionModifier = make(chan time.Duration)
	go func() {
		rf.electionModifier <- initElectionTimeout
	}()

	// allow to replicate to other servers
	rf.replicating = make([]bool, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// main loop
	go rf.ticker()

	// apply command
	rf.commitIndexChangedCh = make(chan bool)
	rf.snapshotChangedCh = make(chan bool)
	go rf.applier()

	return rf
}
