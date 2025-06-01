package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	kvsrv "6.5840/kvsrv1"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

const (
	CFG_KEY     = "cfg"     // current configuration
	NEW_CFG_KEY = "new_cfg" // the next configuration
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	logInit()

	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	new, _, err := sck.get(NEW_CFG_KEY)
	// there is no new config
	if err != rpc.OK {
		return
	}

	cur, _, err := sck.get(CFG_KEY)
	// there is no current config
	if err != rpc.OK {
		return
	}

	// need to rerun migration
	if new.Num > cur.Num {
		sck.ChangeConfigTo(new)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.put(CFG_KEY, cfg, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	DPrintf(tChangeConfig, "try to change configuration to: %+v", new)

	cur, curVer, err := sck.get(CFG_KEY)
	if err != rpc.OK {
		log.Fatalf("cannot query current config %+v", err)
	}

	if cur.Num >= new.Num {
		DPrintf(tChangeConfig, "current configuration version is the latest: current=%v >= new=%v", cur.Num, new.Num)
		return
	}

	// before starting, trying to save persist the new configuration first,
	// allowing to recover in case of failure
	existingNew, ver, err := sck.get(NEW_CFG_KEY)
	if err != rpc.OK {
		// no new configuration exist, we can add one to it
		sck.put(NEW_CFG_KEY, new, 0)
	} else {
		if existingNew.Num > new.Num {
			DPrintf(tChangeConfig, "the existing new configuration version is higher, go on with existing new: existingNew=%v >= new=%v", existingNew.Num, new.Num)
			new = existingNew
		} else {
			DPrintf(tChangeConfig, "change new configuration in the store %+v", new)
			sck.put(NEW_CFG_KEY, new, ver)
		}
	}

	DPrintf(tChangeConfig, "try to change config from %+v, to %+v", cur, new)

	// record for deleting later
	oldShardSevers := make(map[shardcfg.Tshid][]string)

	for shid := range shardcfg.NShards {
		sh := shardcfg.Tshid(shid)

		oGid, oSrv, ok := cur.GidServers(sh)
		if !ok {
			log.Fatalf("cannot get old servers for shard %v", shid)
		}
		nGid, nSrv, ok := new.GidServers(sh)
		if !ok {
			log.Fatalf("cannot get new servers for shard %v", shid)
		}

		DPrintf(tChangeConfig, "shard %v, change from gid %d to %d", sh, oGid, nGid)
		if oGid != nGid {
			oc := shardgrp.MakeClerk(sck.clnt, oSrv)
			nc := shardgrp.MakeClerk(sck.clnt, nSrv)
			oldShardSevers[sh] = oSrv

			DPrintf(tChangeConfig, "shard %v, freeze old gid %d, servers: %v", sh, oGid, oSrv)
			state, err := oc.FreezeShard(sh, new.Num)
			if err != rpc.OK {
				panic("not implemented")
			}

			DPrintf(tChangeConfig, "shard %v, install new gid %d, servers: %v", sh, nGid, nSrv)
			err = nc.InstallShard(sh, state, new.Num)
			if err != rpc.OK {
				panic("not implemented")
			}
		}
	}

	DPrintf(tChangeConfig, "submit the current config as the new one: %+v", new)
	sck.put(CFG_KEY, new, curVer)
	DPrintf(tChangeConfig, "submitted, use the new config: %+v", new)

	for sh, srv := range oldShardSevers {
		DPrintf(tChangeConfig, "shard %v, delete from old servers: %v", sh, srv)
		oc := shardgrp.MakeClerk(sck.clnt, srv)
		err = oc.DeleteShard(sh, new.Num)
		if err != rpc.OK {
			panic("not implemented")
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfg, _, err := sck.get(CFG_KEY)
	if err != rpc.OK {
		log.Fatalf("cannot query config %+v", err)
	}
	return cfg
}

func (sck *ShardCtrler) get(key string) (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	s, version, err := sck.Get(key)
	if err != rpc.OK {
		return nil, version, err
	}
	cfg := shardcfg.FromString(s)
	return cfg, version, err
}

func (sck *ShardCtrler) put(key string, cfg *shardcfg.ShardConfig, version rpc.Tversion) {
	err := sck.Put(key, cfg.String(), version)
	if err != rpc.OK && err != rpc.ErrMaybe {
		log.Fatalf("cannot put config to key=%v, version=%v, cfg=%+v, err=%v", key, version, cfg, err)
	}
}
