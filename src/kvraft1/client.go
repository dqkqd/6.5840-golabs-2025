package kvraft

import (
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader atomic.Int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	logInit()
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leader = atomic.Int64{}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	for {
		leader := ck.leader.Load()
		reply := rpc.GetReply{}
		DPrintf(tClerkGet, "C%p get args=%v from leader=%d", ck.clnt, args, leader)
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		DPrintf(tClerkGet, "C%p, ok=%v, get args=%v from leader=%d, return %v", ck.clnt, ok, args, leader, reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.waitAndChangeLeader(leader)
		} else {
			return reply.Value, reply.Version, reply.Err
		}
		DPrintf(tClerkGet, "C%p, Change leader from %d to %d", ck.clnt, leader, ck.leader.Load())
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	maybe := false
	for {
		leader := ck.leader.Load()
		reply := rpc.PutReply{}
		DPrintf(tClerkPut, "C%p put args=%v to leader=%d", ck.clnt, args, leader)
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)
		DPrintf(tClerkPut, "C%p, ok=%v put args=%v to leader=%d, return %v", ck.clnt, ok, args, leader, reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			// we might have successfully put this key into the server but the response was lost or,
			// we send request to a non leader, but it might be elected as the new leader right after that.
			maybe = true
			ck.waitAndChangeLeader(leader)
		} else {
			// if we got `ErrVersion` with maybe, then this is not the first time we send it.
			// we need to return `ErrMaybe` since we don't know the previous rpc was success or not
			if maybe && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		DPrintf(tClerkPut, "C%p Change leader from %d to %d", ck.clnt, leader, ck.leader.Load())
	}
}

func (ck *Clerk) waitAndChangeLeader(currentLeader int64) {
	time.Sleep(10 * time.Millisecond)
	ck.leader.CompareAndSwap(currentLeader, (currentLeader+1)%int64(len(ck.servers)))
}
