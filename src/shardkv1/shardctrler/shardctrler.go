package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"github.com/google/uuid"

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
	new, newver, err := sck.get(NEW_CFG_KEY)
	// there is no new config
	if err != rpc.OK {
		return
	}

	cur, curver, err := sck.get(CFG_KEY)
	// there is no current config
	if err != rpc.OK {
		return
	}

	// need to rerun migration
	if new.Num > cur.Num {
		if newver != curver+1 {
			panic("not implemented")
		}
		sck.changeConfig(new, false)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.putChecked(CFG_KEY, cfg, 0)
	sck.putChecked(NEW_CFG_KEY, cfg, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	sck.changeConfig(new, true)
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

func (sck *ShardCtrler) putChecked(key string, cfg *shardcfg.ShardConfig, version rpc.Tversion) {
	if !sck.put(key, cfg, version) {
		log.Fatalf("cannot put key=%v, version=%v, cfg=%+v", key, version, cfg)
	}
}

func (sck *ShardCtrler) put(key string, cfg *shardcfg.ShardConfig, version rpc.Tversion) bool {
	err := sck.Put(key, cfg.String(), version)
	return err == rpc.OK || err == rpc.ErrMaybe
}

func (sck *ShardCtrler) changeConfig(new *shardcfg.ShardConfig, addNew bool) {
	uuid := uuid.New().String()
	DPrintf(tChangeConfig, "id=%v, try to change configuration to: %+v", uuid, new)

	cur, curver, err := sck.get(CFG_KEY)
	if err != rpc.OK {
		DPrintf(tChangeConfig, "id=%v, cannot get current config, abort", uuid)
		return
	}
	if cur.Num >= new.Num {
		DPrintf(tChangeConfig, "id=%v, the current config is the latest, current=%+v >= num=%+v", uuid, cur.Num, new.Num)
		return
	}

	if new.Num != cur.Num+1 {
		panic("new.Num > cur.Num + 1")
	}

	if addNew {
		DPrintf(tChangeConfig, "id=%v, change new configuration in the store %+v", uuid, new)
		ok := sck.put(NEW_CFG_KEY, new, curver)
		if !ok {
			DPrintf(tChangeConfig, "id=%v, can't change new configuration in the store, abort, new=%+v", uuid, new)
			return
		}
	}
	DPrintf(tChangeConfig, "id=%v, try to change config from %+v, to %+v", uuid, cur, new)

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

		DPrintf(tChangeConfig, "id=%v, shard %v, change from gid %d to %d", uuid, sh, oGid, nGid)
		if oGid != nGid {
			oc := shardgrp.MakeClerk(sck.clnt, oSrv)
			nc := shardgrp.MakeClerk(sck.clnt, nSrv)
			oldShardSevers[sh] = oSrv

			DPrintf(tChangeConfig, "id=%v, shard %v, freeze old gid %d, servers: %v", uuid, sh, oGid, oSrv)
			state, err := oc.FreezeShard(sh, new.Num)
			if err == rpc.ErrWrongGroup {
				DPrintf(tChangeConfig, "id=%v, shard %v, cannot freeze, wrong group, abort, old gid %d, servers: %v", uuid, sh, oGid, oSrv)
				return
			}
			if err != rpc.OK {
				panic("not implemented")
			}

			DPrintf(tChangeConfig, "id=%v, shard %v, install new gid %d, servers: %v", uuid, sh, nGid, nSrv)
			err = nc.InstallShard(sh, state, new.Num)
			if err == rpc.ErrWrongGroup {
				DPrintf(tChangeConfig, "id=%v, shard %v, cannot install, wrong group, abort, old gid %d, servers: %v", uuid, sh, oGid, oSrv)
				return
			}
			if err != rpc.OK {
				panic("not implemented")
			}
		}
	}

	DPrintf(tChangeConfig, "id=%v, submit the current config as the new one: %+v", uuid, new)
	ok := sck.put(CFG_KEY, new, curver)
	if !ok {
		DPrintf(tChangeConfig, "id=%v, submit new config failed, someone has changed it: new=%+v", uuid, new)
		return
	}
	DPrintf(tChangeConfig, "id=%v, submitted, use the new config: %+v", uuid, new)

	for sh, srv := range oldShardSevers {
		DPrintf(tChangeConfig, "id=%v, shard %v, delete from old servers: %v", uuid, sh, srv)
		oc := shardgrp.MakeClerk(sck.clnt, srv)
		err := oc.DeleteShard(sh, new.Num)
		if err == rpc.ErrWrongGroup {
			DPrintf(tChangeConfig, "id=%v, shard %v, cannot delete, wrong group, abort, shid %d, servers: %v", uuid, sh, sh, srv)
			return
		}
		if err != rpc.OK {
			panic("not implemented")
		}
	}
}
