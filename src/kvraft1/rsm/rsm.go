package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int   // state machine id
	Id  int64 // op unique id
	Req any   // actual request from client
}

type ReturnOp struct {
	opId         int64
	result       any
	commandIndex int
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
	opId     atomic.Int64
	returnCh map[int64]chan ReturnOp
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
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		returnCh:     make(map[int64]chan ReturnOp),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
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
	op := Op{Me: rsm.me, Id: rsm.opId.Add(1), Req: req}

	retCh := make(chan ReturnOp)
	rsm.mu.Lock()
	rsm.returnCh[op.Id] = retCh
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		retCh, ok := rsm.returnCh[op.Id]
		if ok {
			delete(rsm.returnCh, op.Id)
			close(retCh)
		}
		rsm.mu.Unlock()
	}()

	expectedIndex, term, _ := rsm.rf.Start(op)
	DPrintf(tSubmit, "S%d: start op=%+v in term=%d, expected index=%d", rsm.me, op, term, expectedIndex)

	// need a for loop to check whether term or leader changed
	for {
		select {
		case res, ok := <-retCh:
			if !ok {
				DPrintf(tStop, "S%d: stop operation", rsm.me)
				return rpc.ErrWrongLeader, nil
			}
			DPrintf(tReceiveReturn, "S%d, receive returned op=%+v, res=%+v", rsm.me, op, res)
			if res.commandIndex != expectedIndex {
				DPrintf(tSubmitErr, "S%d: reject op=%+v, expected index=%v, got=%v", rsm.me, op, expectedIndex, res.commandIndex)
				return rpc.ErrWrongLeader, nil
			}
			DPrintf(tSubmitOk, "S%d: accept op=%+v, res=%+v", rsm.me, op, res)
			return rpc.OK, res.result

		case <-time.After(50 * time.Millisecond):
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
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			// channel is closed
			DPrintf(tStop, "S%d: applyCh closed, stop operation", rsm.me)
			rsm.mu.Lock()
			for opId, ch := range rsm.returnCh {
				delete(rsm.returnCh, opId)
				close(ch)
			}
			rsm.mu.Unlock()
			return
		}
		op := msg.Command.(Op)
		DPrintf(tApply, "S%d <- S%d: received from applyCh, op=%+v", rsm.me, op.Me, op)
		res := rsm.sm.DoOp(op.Req)
		if op.Me == rsm.me {
			rsm.mu.Lock()
			retCh, ok := rsm.returnCh[op.Id]
			rsm.mu.Unlock()
			if ok {
				DPrintf(tSendReturn, "S%d: send DoOp result=%+v", rsm.me, res)
				retCh <- ReturnOp{opId: op.Id, result: res, commandIndex: msg.CommandIndex}
			}
		}
	}
}
