package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type lockState = string

const (
	Locked   lockState = "locked"
	Unlocked           = "unlocked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lkey   string
	lstate lockState
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lkey: l, lstate: Unlocked}
	// You may add code here
	lk.init()
	return lk
}

// try to init the lock if it doesn't exist
func (lk *Lock) init() {
	err := lk.ck.Put(lk.lkey, Unlocked, 0)
	if err == rpc.ErrVersion {
		log.Printf("someone has initialized the lock already, lkey=`%s`", lk.lkey)
	} else if !(err == rpc.OK || err == rpc.ErrMaybe) {
		log.Fatalf("cannot initialize lock lkey=`%s`, err=`%s`", lk.lkey, err)
	}
}

func (lk *Lock) Acquire() {
	// Your code here

	// for then test-and-set
	for {
		value, version, err := lk.ck.Get(lk.lkey)

		if err != rpc.OK {
			log.Fatalf("lock lkey=`%s` wasn't initialized", lk.lkey)
		}

		if value == Unlocked {
			err = lk.ck.Put(lk.lkey, Locked, version)
			if err == rpc.OK || err == rpc.ErrMaybe {
				return
			}
		}

		time.Sleep(time.Second)
	}
}

func (lk *Lock) Release() {
	// Your code here

	value, version, err := lk.ck.Get(lk.lkey)
	if err != rpc.OK {
		log.Fatalf("lock lkey=`%s` wasn't initialized", lk.lkey)
	}

	if value != Locked {
		log.Fatalf("lock lkey=`%s` wasn't locked", lk.lkey)
	}

	err = lk.ck.Put(lk.lkey, Unlocked, version)
	if !(err == rpc.OK || err == rpc.ErrMaybe) {
		log.Fatalf("cannot unlock lkey=`%s`", lk.lkey)
	}
}
