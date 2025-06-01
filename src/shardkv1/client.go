package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	logInit()

	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	sh := shardcfg.Key2Shard(key)
	DPrintf(tClerkGet, "C%p, Shardkv get key=%v from shard=%d", ck, key, sh)
	for {
		cfg := ck.sck.Query()
		_, servers, ok := cfg.GidServers(sh)

		DPrintf(tClerkGet, "C%p, Shardkv get key=%v from shard=%d, servers=%v", ck, key, sh, servers)
		if !ok {
			DPrintf(tClerkGet, "C%p, Shardkv get key=%v from shard=%d, servers=%v no response", ck, key, sh, servers)
			ck.wait()
			continue
		}

		c := shardgrp.MakeClerk(ck.clnt, servers)
		value, version, err := c.Get(key)
		if err == rpc.ErrWrongGroup {
			DPrintf(tClerkGet, "C%p, Shardkv get key=%v from shard=%d, servers=%v wrong group", ck, key, sh, servers)
			ck.wait()
			continue
		}

		return value, version, err

	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	sh := shardcfg.Key2Shard(key)
	DPrintf(tClerkPut, "C%p, Shardkv put key=%v from shard=%d", ck, key, sh)
	for {
		cfg := ck.sck.Query()
		_, servers, ok := cfg.GidServers(sh)

		DPrintf(tClerkPut, "C%p, Shardkv put key=%v from shard=%d, servers=%v", ck, key, sh, servers)
		if !ok {
			DPrintf(tClerkPut, "C%p, Shardkv put key=%v from shard=%d, servers=%v no response", ck, key, sh, servers)
			ck.wait()
			continue
		}

		c := shardgrp.MakeClerk(ck.clnt, servers)
		err := c.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			DPrintf(tClerkPut, "C%p, Shardkv get key=%v from shard=%d, servers=%v wrong group", ck, key, sh, servers)
			ck.wait()
			continue
		}

		return err
	}
}

func (ck *Clerk) wait() {
	time.Sleep(10 * time.Millisecond)
}
