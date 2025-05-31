package shardgrp

import (
	"sync/atomic"
	"time"

	tester "6.5840/tester1"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader atomic.Int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	logInit()
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader = atomic.Int64{}
	return ck
}

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

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for {
		leader := ck.leader.Load()
		reply := shardrpc.FreezeShardReply{}
		DPrintf(tClerkFreezeShard, "C%p freeze args=%+v, leader=%d", ck.clnt, args, leader)
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)
		DPrintf(tClerkFreezeShard, "C%p, ok=%v, freeze args=%+v from leader=%d, return %+v", ck.clnt, ok, args, leader, reply)
		DPrintf(tClerkFreezeShard, "C%p, Change leader from %d to %d", ck.clnt, leader, ck.leader.Load())
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	for {
		leader := ck.leader.Load()
		reply := shardrpc.InstallShardReply{}
		DPrintf(tClerkInstallShard, "C%p install args=%+v from leader=%d", ck.clnt, args, leader)
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)
		DPrintf(tClerkInstallShard, "C%p, ok=%v, install args=%+v from leader=%d, return %+v", ck.clnt, ok, args, leader, reply)
		DPrintf(tClerkInstallShard, "C%p, Change leader from %d to %d", ck.clnt, leader, ck.leader.Load())
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) waitAndChangeLeader(currentLeader int64) {
	time.Sleep(50 * time.Millisecond)
	ck.leader.CompareAndSwap(currentLeader, (currentLeader+1)%int64(len(ck.servers)))
}
