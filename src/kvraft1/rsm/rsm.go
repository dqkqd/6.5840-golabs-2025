package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"

	"github.com/google/uuid"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int       // state machine id
	Id  uuid.UUID // op unique id
	Req any       // actual request from client
}

type ReturnOp struct {
	opId         uuid.UUID
	result       any
	commandIndex int
}

type waitSubmitCh struct {
	id uuid.UUID
	ch chan<- ReturnOp
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	addWaitSubmitCh   chan waitSubmitCh
	closeWaitSubmitCh chan uuid.UUID
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	logInit()

	rsm := &RSM{
		me:                me,
		maxraftstate:      maxraftstate,
		applyCh:           make(chan raftapi.ApplyMsg),
		sm:                sm,
		addWaitSubmitCh:   make(chan waitSubmitCh),
		closeWaitSubmitCh: make(chan uuid.UUID, 1000),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	DPrintf(tStart, "S%d: start rsm", rsm.me)

	// reader goroutines
	go rsm.runReader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	op := Op{Me: rsm.me, Id: uuid.New(), Req: req}

	w := make(chan ReturnOp)
	select {
	case rsm.addWaitSubmitCh <- waitSubmitCh{id: op.Id, ch: w}:
	case <-time.After(1 * time.Second):
		return rpc.ErrWrongLeader, nil
	}

	defer func() { rsm.closeWaitSubmitCh <- op.Id }()

	expectedIndex, term, isLeader := rsm.rf.Start(op)
	DPrintf(tSubmit, "S%d: start op=%+v in term=%d, expected index=%d", rsm.me, op, term, expectedIndex)
	if !isLeader {
		DPrintf(tSubmitErr, "S%d: reject op=%+v, not a leader in term %d", rsm.me, op, term)
		return rpc.ErrWrongLeader, nil
	}

	// need a for loop to check whether term or leader changed
	for {
		select {
		case res, ok := <-w:
			if !ok {
				DPrintf(tStop, "S%d: stop operation", rsm.me)
				return rpc.ErrWrongLeader, nil
			}
			DPrintf(tReturn, "S%d, receive returned op=%+v, res=%+v", rsm.me, op, res)
			if res.commandIndex != expectedIndex {
				DPrintf(tSubmitErr, "S%d: reject op=%+v, expected index=%v, got=%v", rsm.me, op, expectedIndex, res.commandIndex)
				return rpc.ErrWrongLeader, nil
			}
			DPrintf(tSubmitOk, "S%d: accept op=%+v, res=%+v", rsm.me, op, res)
			return rpc.OK, res.result

		case <-time.After(10 * time.Millisecond):
			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader {
				DPrintf(tSubmitErr, "S%d: reject op=%+v, not a leader in term %d", rsm.me, op, term)
				return rpc.ErrWrongLeader, nil
			}
			if currentTerm != term {
				DPrintf(tSubmitErr, "S%d: reject op=%+v, term changed while waiting for result (%d -> %d)", rsm.me, op, term, currentTerm)
				return rpc.ErrWrongLeader, nil
			}

		}
	}
}

func (rsm *RSM) runReader() {
	waitCh := make(map[uuid.UUID]chan<- ReturnOp)
	defer func() {
		for _, ch := range waitCh {
			close(ch)
		}
	}()

	for {
		select {
		case msg, ok := <-rsm.applyCh:
			if !ok {
				DPrintf(tStop, "S%d: applyCh closed, stop operation", rsm.me)
				return
			}

			DPrintf(tApply, "S%d: msg=%+v", rsm.me, msg)

			if msg.CommandValid {
				op := msg.Command.(Op)
				DPrintf(tApply, "S%d <- S%d: received from applyCh, msg=%+v, op=%+v", rsm.me, op.Me, msg, op)
				res := rsm.sm.DoOp(op.Req)
				DPrintf(tDoOp, "S%d, DoOp res=%+v", rsm.me, res)
				if op.Me == rsm.me {
					w, ok := waitCh[op.Id]
					if ok {
						DPrintf(tReturn, "S%d, send returned msg=%+v, op=%+v, res=%+v", rsm.me, msg, op, res)
						w <- ReturnOp{opId: op.Id, result: res, commandIndex: msg.CommandIndex}
					}
				}

				if rsm.maxraftstate != -1 {
					// truncate the log if the size >= 80%
					if rsm.Raft().PersistBytes()*10 >= rsm.maxraftstate*8 {
						DPrintf(tSnapshot, "S%d, taking snapshot msg=%+v, op=%+v, res=%+v", rsm.me, msg, op, res)
						snapshot := rsm.sm.Snapshot()
						rsm.Raft().Snapshot(msg.CommandIndex, snapshot)
					}
				}
			} else if msg.SnapshotValid {
				DPrintf(tRestore, "S%d, restore from snapshot msg=%+v", rsm.me, msg)
				rsm.sm.Restore(msg.Snapshot)
			}

		case w := <-rsm.addWaitSubmitCh:
			// add new channel for waiting opId
			waitCh[w.id] = w.ch

		case id := <-rsm.closeWaitSubmitCh:
			// do not wait on channel id
			delete(waitCh, id)
		}
	}
}
