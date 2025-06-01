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
	new, _, err := sck.Get(NEW_CFG_KEY)
	// there is no new config
	if err != rpc.OK {
		return
	}

	cur, _, err := sck.Get(CFG_KEY)
	// there is no current config
	if err != rpc.OK {
		return
	}

	// need to rerun
	newCfg := shardcfg.FromString(new)
	curCfg := shardcfg.FromString(cur)

	if newCfg.Num > curCfg.Num {
		sck.ChangeConfigTo(newCfg)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	cfgString := cfg.String()
	err := sck.Put(CFG_KEY, cfgString, 0)
	if err != rpc.OK && err != rpc.ErrMaybe {
		log.Fatalf("cannot put config %+v: %v", err, cfgString)
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	DPrintf(tChangeConfig, "try to change configuration to: %+v", new)

	curString, curVer, err := sck.Get(CFG_KEY)
	if err != rpc.OK {
		log.Fatalf("cannot query config %+v", err)
	}
	cur := shardcfg.FromString(curString)

	if cur.Num >= new.Num {
		DPrintf(tChangeConfig, "current configuration version is the latest: current=%v >= new=%v", cur.Num, new.Num)
		return
	}

	// before starting, trying to save persist the new configuration first,
	// allowing to recover in case of failure
	existingNewString, ver, err := sck.Get(NEW_CFG_KEY)
	if err != rpc.OK {
		// no new configuration exist, we can add one to it
		err = sck.Put(NEW_CFG_KEY, new.String(), 0)
		if err != rpc.OK && err != rpc.ErrMaybe {
			log.Fatalf("cannot add new configuration before changing config err=%v, cfg=%+v", err, new)
		}
	} else {
		existingNew := shardcfg.FromString(existingNewString)
		if existingNew.Num > new.Num {
			DPrintf(tChangeConfig, "the existing new configuration version is higher, go on with existing new: existingNew=%v >= new=%v", existingNew.Num, new.Num)
			new = existingNew
		} else {
			DPrintf(tChangeConfig, "change new configuration in the store %+v", new)
			err = sck.Put(NEW_CFG_KEY, new.String(), ver)
			if err != rpc.OK && err != rpc.ErrMaybe {
				log.Fatalf("cannot add new configuration before changing config err=%v, cfg=%+v", err, new)
			}
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
	err = sck.Put(CFG_KEY, new.String(), curVer)
	if err != rpc.OK && err != rpc.ErrMaybe {
		log.Fatalf("cannot change the current config, %+v: %v", err, cur)
	}
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
	cfgString, _, err := sck.Get(CFG_KEY)
	if err != rpc.OK {
		log.Fatalf("cannot query config %+v", err)
	}
	cfg := shardcfg.FromString(cfgString)
	return cfg
}
