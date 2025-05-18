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
	returnCh chan ReturnOp
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
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		returnCh:     make(chan ReturnOp),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	logInit()

	// reader goroutines
	go func() {
		for {
			for {
				// applyCh can be null, need to check to avoid deadlock
				select {
				case msg := <-rsm.applyCh:
					op := msg.Command.(Op)
					DPrintf(tApply, "S%d <- S%d: received from applyCh, op=%+v", rsm.me, op.Me, op)
					res := rsm.sm.DoOp(op.Req)

					if op.Me == rsm.me {
						DPrintf(tSendReturn, "S%d: send DoOp result=%+v", rsm.me, res)
						rsm.returnCh <- ReturnOp{opId: op.Id, result: res, commandIndex: msg.CommandIndex}
					}

				case <-time.After(50 * time.Millisecond):
					if rsm.applyCh == nil {
						// channel is closed
						DPrintf(tStop, "S%d: stop operation", rsm.me)
						close(rsm.returnCh)
						return
					}
				}
			}
		}
	}()
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
	DPrintf(tSubmit, "S%d: start op=%+v", rsm.me, op)

	expectedIndex, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		DPrintf(tSubmit, "S%d: reject op=%+v, not a leader in term %d", rsm.me, op, term)
		return rpc.ErrWrongLeader, nil
	}

	for {
		select {
		case res, ok := <-rsm.returnCh:
			if !ok {
				DPrintf(tStop, "S%d: stop operation", rsm.me)
				// TODO: maybe?
				return rpc.ErrMaybe, nil
			}

			DPrintf(tReceiveReturn, "S%d, receive returned op=%+v, res=%+v", rsm.me, op, res)
			if res.commandIndex != expectedIndex {
				DPrintf(tSubmit, "S%d: reject op=%+v, expected index=%v, got=%v", rsm.me, op, expectedIndex, res.commandIndex)
				return rpc.ErrWrongLeader, nil
			}
			if res.opId < op.Id {
				DPrintf(tSubmit, "S%d: skip op=%+v, staled id, expected=%v, got=%v", rsm.me, op, op.Id, res.opId)
				continue
			}
			return rpc.OK, res.result

		case <-time.After(time.Millisecond * 10):
			currentTerm, _ := rsm.rf.GetState()
			if currentTerm != term {
				DPrintf(tSubmit, "S%d: reject op=%+v, term changed while waiting for result (%d -> %d)", rsm.me, op, term, currentTerm)
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
