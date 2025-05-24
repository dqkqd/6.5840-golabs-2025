package kvsrv

import (
	"sync"

	"6.5840/kvsrv1/rpc"
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
	value   string
	version uint64
}

type keyValueStore struct {
	mu   sync.Mutex
	data map[string]keyValue
}

func (s *keyValueStore) get(key string) (keyValue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *keyValueStore) put(key, value string, version uint64) kvErr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version == 0 {
		_, ok := s.data[key]
		if ok {
			return kvErrVersion
		}
		s.data[key] = keyValue{version: 1, value: value}
		return kvErrOk
	} else {
		v, ok := s.data[key]
		if !ok {
			return kvErrNoKey
		}
		if v.version != version {
			return kvErrVersion
		}
		s.data[key] = keyValue{version: version + 1, value: value}
		return kvErrOk
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
	v, ok := kv.store.get(args.Key)
	if ok {
		reply.Version = rpc.Tversion(v.version)
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
	err := kv.store.put(args.Key, args.Value, uint64(args.Version))
	switch err {
	case kvErrOk:
		reply.Err = rpc.OK
	case kvErrVersion:
		reply.Err = rpc.ErrVersion
	case kvErrNoKey:
		reply.Err = rpc.ErrNoKey
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
