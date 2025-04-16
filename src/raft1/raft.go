package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
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
	state                 serverState           // state of the server: leader, follower, or candidate
	latestAppendEntriesAt time.Time             // the latest time we received the entries, since we only save this to compute the monotonical timediff, it is fine
	electionNotifier      waitNotifier          // notify and reset election timeout)
	heartbeatNotifier     waitNotifier          // notify heartbeat
	applyCh               chan raftapi.ApplyMsg // apply channel to state machine
	electionTimeout       time.Duration         // current election timeout
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply's default value
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1
	reply.Success = false

	DPrintf(tReceiveAppend, "S%d(%d) <- S%d(%d), receive append %+v ", rf.me, rf.currentTerm, args.LeaderId, args.Term, args)

	// record the time when we received the AppendEntries
	rf.latestAppendEntriesAt = time.Now()

	// AppendEntries rule 1: reply false for smaller term from leader
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		DPrintf(tReceiveAppend, "S%d(%d) <- S%d(%d), append reject, lower term", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	// Rules for Servers: lower term, change to follower
	if rf.currentTerm < args.Term {
		rf.changeTerm(args.Term)
		rf.vote(args.LeaderId)
	}

	reply.Term = args.Term

	// AppendEntries rule 2: reply false
	// if log doesn't contain entry at prevLogIndex whose term match prevLogTerm
	if len(rf.log) <= args.PrevLogIndex {
		// follower's log is too short
		reply.XLen = len(rf.log)
		DPrintf(tReceiveAppend,
			"S%d(%d) <- S%d(%d), append reject, follower's log too is short, len(log)=%d",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, len(rf.log),
		)
		return
	}
	// conflict term
	xTerm := rf.log[args.PrevLogIndex].Term
	if xTerm != args.PrevLogTerm {
		reply.XTerm = xTerm
		reply.XIndex = rf.findFirstIndexWithTerm(reply.XTerm)
		DPrintf(tReceiveAppend, "S%d(%d) <- S%d(%d), append reject, conflict term, reply=`%+v`", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply)
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
		// TODO: remove rf.log
		DPrintf(tReceiveAppend, "S%d(%d) <- S%d(%d), entry: %v, log: %v", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.Entries[index:], rf.log)
	}

	// persist the log
	rf.persist()

	// AppendEntries rule 5: change commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCommand()
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

	DPrintf(tVote, "S%d(%d) <- S%d(%d), receive request vote", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	// RequestVote rule 1: reply false if request's term < current term
	if rf.currentTerm > args.Term {
		DPrintf(tVote, "S%d(%d) <- S%d(%d), higher term, do not vote", rf.me, rf.currentTerm, args.CandidateId, args.Term)

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Rules for Servers: lower term, change to follower (but do not reply immediately)
	if rf.currentTerm < args.Term {
		DPrintf(tVote, "S%d(%d) <- S%d(%d), lower term", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.changeTerm(args.Term)
		rf.vote(args.CandidateId)
		reply.Term = args.Term
	}

	// RequestVote rule 2: voted for is null or candidate id
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// and candidate's log is as least as up-to-date
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			DPrintf(tVote,
				"S%d(%d) <- S%d(%d), accept (term, index): S%d(%d, %d) <= S%d(%d, %d)",
				rf.me, rf.currentTerm, args.CandidateId, args.Term,
				rf.me, lastLogTerm, lastLogIndex,
				args.CandidateId, args.LastLogTerm, args.LastLogIndex,
			)

			// then we should vote
			rf.vote(args.CandidateId)
			reply.Term = args.Term
			reply.VoteGranted = true
		} else {
			DPrintf(tVote,
				"S%d(%d) <- S%d(%d), reject (term, index): S%d(%d, %d) > S%d(%d, %d)",
				rf.me, rf.currentTerm, args.CandidateId, args.Term,
				rf.me, lastLogTerm, lastLogIndex,
				args.CandidateId, args.LastLogTerm, args.LastLogIndex,
			)
		}
	} else {
		DPrintf(tVote, "S%d(%d) <- S%d(%d), reject, already voted for S%d", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.votedFor)
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
					DPrintf(tVote, "S%d(%d) -> S%d(-), send request vote", rf.me, args.Term, serverId)
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

// create appendEntriesArgs at certain index
func (rf *Raft) appendEntriesArgs(at int) AppendEntriesArgs {
	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: at - 1,
		PrevLogTerm:  rf.log[at-1].Term,
		Entries:      rf.log[at:],
		LeaderCommit: rf.commitIndex,
	}
}

// looping and send append entries to a peer until agreement is reached
func (rf *Raft) sendAppendEntries(peerId int, args AppendEntriesArgs) {
	// do not send to self
	if rf.me == peerId {
		return
	}

	// TODO: stop this loop if it is elected again?
	// TODO: stop if we are not the leader any more? This requires lock.
	peer := rf.peers[peerId]

	for !rf.killed() {

		// do not send append tries if we are not leader
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf(tSendAppend, "S%d(%d) -> S%d(-), send append entries, args=%+v", rf.me, rf.currentTerm, peerId, args)
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := peer.Call("Raft.AppendEntries", &args, &reply)
		if !ok {
			sleep()
			continue
		}

		// appending successfully, no need to send any append entries
		if reply.Success {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append entries success", rf.me, rf.currentTerm, peerId, reply.Term)

			// update nextIndex and matchIndex
			rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1

			// find majority replicated entries to commit
			for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
				// TODO: might not need this check?
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
						rf.applyCommand()
						break
					}
				}
			}

			return
		}

		// Rules for Servers: lower term, change to follower
		// term has been changed from peer,
		// do not send `AppendEntries` for this term and return immediately
		if reply.Term > args.Term {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append failed at term %d, maybe change to follower", rf.me, rf.currentTerm, peerId, reply.Term, args.Term)
			rf.changeTerm(reply.Term)
			return
		}

		// rejection, change nextIndex and retry
		rf.mu.Lock()

		if reply.XTerm != -1 && reply.XIndex != -1 {
			// conflicting term

			// find the last index of XTerm (the index before the first XTerm + 1)
			lastXTermIndex := rf.findFirstIndexWithTerm(reply.XTerm+1) - 1

			if lastXTermIndex >= 0 && rf.log[lastXTermIndex].Term == reply.XTerm {
				// leader has XTerm
				rf.nextIndex[peerId] = lastXTermIndex + 1
			} else {
				// leader doesn't have XTerm
				rf.nextIndex[peerId] = reply.XIndex
			}
		} else {
			// follower's log is too short
			rf.nextIndex[peerId] = reply.XLen
		}

		DPrintf(tSendAppend, "S%d(%d) -> S%d(%d), append rejected, nextIndex=%d, reply=%+v", rf.me, rf.currentTerm, peerId, reply.Term, rf.nextIndex[peerId], reply)

		args = rf.appendEntriesArgs(rf.nextIndex[peerId])

		rf.mu.Unlock()
	}
}

func (rf *Raft) findFirstIndexWithTerm(term int) int {
	return sort.Search(len(rf.log), func(i int) bool {
		return rf.log[i].Term >= term
	})
}

// change term based on Rule for All Servers,
// lock must not be hold by caller
func (rf *Raft) changeTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.persist()
	}
}

// leader initialization,
// lock must be hold by caller
func (rf *Raft) becomeLeader() {
	rf.state = Leader

	DPrintf(tBecomeLeader, "S%d(%d) become leader", rf.me, rf.currentTerm)

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

// apply log to state machine
func (rf *Raft) applyCommand() {
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for !rf.killed() && rf.lastApplied < rf.commitIndex {
			// TODO: persist lastApplied

			// do not send index-0
			rf.lastApplied++

			log := rf.log[rf.lastApplied]

			switch log.LogEntryType {

			case clientLogEntry:
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      log.Command,
					CommandIndex: log.CommandIndex,
				}
				DPrintf(tApply, "S%d(%d), apply, log: %+v", rf.me, rf.currentTerm, log)

			case noOpLogEntry:
				DPrintf(tApply, "S%d(%d), skip: log: %+v", rf.me, rf.currentTerm, log)
			}

		}
	}()
}

// grant vote for someone, caller must hold lock
func (rf *Raft) vote(peer int) {
	// TODO: is this logic correct?
	// do not vote twice
	if rf.votedFor == peer {
		return
	}

	rf.votedFor = peer
	rf.persist()

	if rf.me == peer {
		DPrintf(tVote, "S%d(%d) vote for self", rf.me, rf.currentTerm)
	} else {
		DPrintf(tVote, "S%d(%d) vote for %d", rf.me, rf.currentTerm, peer)
	}

	rf.electionTimeout = electionTimeout()

	// reset election timeout
	rf.electionNotifier.changeTimeout(rf.electionTimeout, wakeupLater)
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

	// If we are follower, we do not want to start an election if we receive heartbeat periodically
	if rf.state == Follower && time.Since(rf.latestAppendEntriesAt) < rf.electionTimeout {
		return
	}

	DPrintf(tElection, "S%d(%d), start election", rf.me, rf.currentTerm)

	rf.state = Candidate
	// TODO: should we always increment this or only if we are follower?
	rf.changeTerm(rf.currentTerm + 1)
	rf.vote(rf.me)

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}

	// make sure we are not waiting all days
	electionTimeoutCh := time.After(rf.electionTimeout)

	// voting starts in the background thread
	go func() {
		votingCh := rf.sendRequestVotes(args)

		// we have voted for ourselves already, so this should become 1
		totalVotes := 1

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
		// send empty append entries
		args := rf.appendEntriesArgs(len(rf.log))
		go func() {
			rf.sendAppendEntries(peerId, args)
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
		args := rf.appendEntriesArgs(rf.nextIndex[peerId])
		go func() {
			rf.sendAppendEntries(peerId, args)
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
	// election timeout: between 500ms and 800ms
	ms := 500 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}

func heartbeatTimeout() time.Duration {
	// heartbeat timeout: 100ms
	return 100 * time.Millisecond
}

// loop shouldn't execute continuouslly without waiting
func sleep() {
	time.Sleep(10 * time.Millisecond)
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
	rf.electionNotifier = waitNotify(electionTimeout())
	rf.heartbeatNotifier = waitNotify(heartbeatTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// main loop
	go rf.ticker()

	return rf
}
