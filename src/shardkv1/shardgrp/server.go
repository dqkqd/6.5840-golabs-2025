package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type kvErr = int

const (
	kvErrOk kvErr = iota
	kvErrNoKey
	kvErrVersion
)

type keyValue struct {
	Value   string
	Version uint64
}

type keyValueStore struct {
	mu      sync.Mutex
	data    map[string]keyValue
	freezed bool
}

func (s *keyValueStore) get(key string) (keyValue, bool) {
	v, ok := s.data[key]
	return v, ok
}

func (s *keyValueStore) put(key, value string, version uint64) kvErr {
	if version == 0 {
		_, ok := s.data[key]
		if ok {
			return kvErrVersion
		}
		s.data[key] = keyValue{Version: 1, Value: value}
		return kvErrOk
	} else {
		v, ok := s.data[key]
		if !ok {
			return kvErrNoKey
		}
		if v.Version != version {
			return kvErrVersion
		}
		s.data[key] = keyValue{Version: version + 1, Value: value}
		return kvErrOk
	}
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu    sync.Mutex
	store map[shardcfg.Tshid]*keyValueStore
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch r := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}

		shid := shardcfg.Key2Shard(r.Key)
		kv.mu.Lock()
		store, ok := kv.store[shid]
		kv.mu.Unlock()
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}

		store.mu.Lock()
		defer store.mu.Unlock()
		v, ok := store.get(r.Key)
		if ok {
			reply.Err = rpc.OK
			reply.Value = v.Value
			reply.Version = rpc.Tversion(v.Version)
		} else {
			reply.Err = rpc.ErrNoKey
		}
		DPrintf(tDoOp, "S%d, get args, req=%+v, reply=%+v", kv.me, r, reply)
		return reply

	case rpc.PutArgs:
		reply := rpc.PutReply{}

		shid := shardcfg.Key2Shard(r.Key)
		kv.mu.Lock()
		store, ok := kv.store[shid]
		if !ok {
			store = &keyValueStore{data: make(map[string]keyValue)}
			kv.store[shid] = store
		}
		kv.mu.Unlock()

		store.mu.Lock()
		defer store.mu.Unlock()
		err := store.put(r.Key, r.Value, uint64(r.Version))
		switch err {
		case kvErrOk:
			reply.Err = rpc.OK
		case kvErrNoKey:
			reply.Err = rpc.ErrNoKey
		case kvErrVersion:
			reply.Err = rpc.ErrVersion
		}
		DPrintf(tDoOp, "S%d, put args, req=%+v, reply=%+v", kv.me, r, reply)
		return reply

	case shardrpc.FreezeShardArgs:
		reply := shardrpc.FreezeShardReply{}
		kv.mu.Lock()
		store, ok := kv.store[r.Shard]
		kv.mu.Unlock()
		reply.Num = r.Num
		if !ok {
			// empty data, create one
			store = &keyValueStore{data: make(map[string]keyValue)}
			kv.store[r.Shard] = store
		}

		store.mu.Lock()
		defer store.mu.Unlock()
		store.freezed = true
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(store.data)
		reply.State = w.Bytes()
		reply.Err = rpc.OK
		DPrintf(tDoOp, "S%d, freeze args, req=%+v, store=%+v, replyNum=%+v, replyErr=%+v", kv.me, r, store, reply.Num, reply.Err)
		return reply

	case shardrpc.InstallShardArgs:
		reply := shardrpc.InstallShardReply{}

		reader := bytes.NewBuffer(r.State)
		d := labgob.NewDecoder(reader)
		var data map[string]keyValue
		if d.Decode(&data) != nil {
			log.Fatalf("%v couldn't decode stored data to install", kv.me)
		}

		kv.mu.Lock()
		defer kv.mu.Unlock()
		_, ok := kv.store[r.Shard]
		if ok {
			panic("Not implemented")
		}
		kv.store[r.Shard] = &keyValueStore{data: data}

		reply.Err = rpc.OK
		DPrintf(tDoOp, "S%d, install args, req=%+v, store=%+v, reply=%+v", kv.me, r, data, reply.Err)
		return reply

	default:
		log.Fatalf("invalid request, %T, %+v", req, req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(len(kv.store))
	for shid, store := range kv.store {
		store.mu.Lock()
		e.Encode(shid)
		e.Encode(store.data)
		store.mu.Unlock()
	}
	DPrintf(tSnapshot, "S%d store: %+v", kv.me, kv.store)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var size int
	if d.Decode(&size) != nil {
		log.Fatalf("%v couldn't decode size of the stores", kv.me)
	}

	for range size {
		var shid shardcfg.Tshid
		var data map[string]keyValue

		if d.Decode(&shid) != nil {
			log.Fatalf("%v couldn't decode shard id", kv.me)
		}
		if d.Decode(&data) != nil {
			log.Fatalf("%v couldn't decode stored data", kv.me)
		}

		store, ok := kv.store[shid]
		if !ok {
			kv.store[shid] = &keyValueStore{data: data}
		} else {
			store.mu.Lock()
			kv.store[shid].data = data
			store.mu.Unlock()
		}
	}
	DPrintf(tRestore, "S%d store: %+v", kv.me, kv.store)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	DPrintf(tServerGet, "S%d, get, req=%v", kv.me, *args)
	err, rep := kv.rsm.Submit(*args)
	DPrintf(tServerGet, "S%d, get return, req=%v, ret=%v", kv.me, *args, *reply)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
	} else {
		r := rep.(rpc.GetReply)
		reply.Err = r.Err
		reply.Value = r.Value
		reply.Version = r.Version
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	DPrintf(tServerPut, "S%d, put, req=%v", kv.me, *args)
	err, rep := kv.rsm.Submit(*args)
	DPrintf(tServerPut, "S%d, put return, req=%v, ret=%v", kv.me, *args, *reply)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
	} else {
		r := rep.(rpc.PutReply)
		reply.Err = r.Err
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	DPrintf(tServerFreezeShard, "S%d, freeze, req=%+v", kv.me, *args)
	err, rep := kv.rsm.Submit(*args)
	DPrintf(tServerFreezeShard, "S%d, freeze return, req=%+v, ret=%+v", kv.me, *args, *reply)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
	} else {
		r := rep.(shardrpc.FreezeShardReply)
		reply.Err = r.Err
		reply.Num = r.Num
		reply.State = r.State
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	DPrintf(tServerInstallShard, "S%d, install, req=%+v", kv.me, *args)
	err, rep := kv.rsm.Submit(*args)
	DPrintf(tServerInstallShard, "S%d, install return, req=%+v, ret=%+v", kv.me, *args, *reply)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
	} else {
		r := rep.(shardrpc.InstallShardReply)
		reply.Err = r.Err
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	logInit()
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.store = make(map[shardcfg.Tshid]*keyValueStore)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// Your code here
	return []tester.IService{kv, kv.rsm.Raft()}
}
