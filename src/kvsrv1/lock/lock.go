package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const unlockedState = "__unlocked"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	clientId string
	lockKey  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		clientId: kvtest.RandValue(8),
		lockKey:  l,
	}
	// You may add code here
	lk.init()
	return lk
}

func wait() {
	time.Sleep(100 * time.Millisecond)
}

// ensure the lockKey is initialized on the server
func (lk *Lock) init() {
	for {
		err := lk.ck.Put(lk.lockKey, unlockedState, 0)
		if err == rpc.ErrMaybe {
			wait()
			continue
		} else {
			break
		}
	}
}

// get should always return OK
func (lk *Lock) lockState() (string, rpc.Tversion) {
	value, version, err := lk.ck.Get(lk.lockKey)
	if err != rpc.OK {
		log.Fatalf("cannot get lock key, err=`%s`", err)
	}
	return value, version
}

func (lk *Lock) Acquire() {
	// test-and-set
	for {
		lockState, version := lk.lockState()

		// already locked
		if lockState == lk.clientId {
			break
		}

		// try to lock it
		if lockState == unlockedState {
			lk.ck.Put(lk.lockKey, lk.clientId, version)
		}
		wait()
	}
}

func (lk *Lock) Release() {
	var lockState string
	var version rpc.Tversion
	lockState, version = lk.lockState()

	if lockState != lk.clientId {
		log.Fatalf("client=`%s` is not holding the lock", lk.clientId)
	}

	for {
		lk.ck.Put(lk.lockKey, unlockedState, version)
		lockState, version = lk.lockState()
		if lockState != lk.clientId {
			break
		} else {
			wait()
		}
	}
}
