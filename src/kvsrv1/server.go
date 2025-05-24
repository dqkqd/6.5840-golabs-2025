package kvsrv

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)



type keyValue struct {
	value   string
	version rpc.Tversion
}

type keyValueStore struct {
	data map[string]keyValue
}

func (s keyValueStore) get(key string) (keyValue, bool) {
	v, ok := s.data[key]
	return v, ok
}

func (s keyValueStore) put(key, value string) {
	v, ok := s.get(key)
	if ok {
		s.data[key] = keyValue{
			version: v.version + 1,
			value:   value,
		}
	} else {
		s.data[key] = keyValue{
			version: 1,
			value:   value,
		}
	}
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store keyValueStore
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.store = keyValueStore{
		data: make(map[string]keyValue),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv.store.get(args.Key)
	if ok {
		reply.Version = v.version
		reply.Value = v.value
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv.store.get(args.Key)
	if ok {
		if v.version == args.Version {
			// match version
			kv.store.put(args.Key, args.Value)
			reply.Err = rpc.OK
		} else {
			// unmatch
			reply.Err = rpc.ErrVersion
		}
	} else {
		// no keys
		if args.Version == 0 {
			// create a new one
			kv.store.put(args.Key, args.Value)
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
