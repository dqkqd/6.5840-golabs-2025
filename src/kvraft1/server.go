package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
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
	data map[string]keyValue
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

	// Your definitions here.
	mu    sync.Mutex
	store keyValueStore
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		v, ok := kv.store.get(r.Key)
		if ok {
			reply.Err = rpc.OK
			reply.Value = v.Value
			reply.Version = rpc.Tversion(v.Version)
		} else {
			reply.Err = rpc.ErrNoKey
		}
		DPrintf(tDoOp, "S%d, get args, req=%v, reply=%v", kv.me, r, reply)
		return reply

	case rpc.PutArgs:
		reply := rpc.PutReply{}
		err := kv.store.put(r.Key, r.Value, uint64(r.Version))
		switch err {
		case kvErrOk:
			reply.Err = rpc.OK
		case kvErrNoKey:
			reply.Err = rpc.ErrNoKey
		case kvErrVersion:
			reply.Err = rpc.ErrVersion
		}
		DPrintf(tDoOp, "S%d, put args, req=%v, reply=%v", kv.me, r, reply)
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
	e.Encode(kv.store.data)
	DPrintf(tSnapshot, "S%d store: %+v", kv.me, kv.store)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.store.data) != nil {
		log.Fatalf("%v couldn't decode snapshot", kv.me)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	logInit()
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.store = keyValueStore{data: make(map[string]keyValue)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
