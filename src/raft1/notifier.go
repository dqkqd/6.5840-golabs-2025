package raft

import "time"

type wakeupKind int

const (
	wakeupNow wakeupKind = iota
	wakeupLater
)

type waitNotifier struct {
	wakeup          chan bool            // notify outer that we have finished waiting a cycle
	timeoutModifier chan<- time.Duration // allow outer to modify waiting timer
}

// change current timeout cycle.
// wake indicate that we should wakeup immediately or we should wait until the next cycle
func (w *waitNotifier) changeTimeout(timeout time.Duration, wake wakeupKind) {
	if wake == wakeupNow {
		w.wakeup <- true
	}
	w.timeoutModifier <- timeout
}

func waitNotify(defaultTimeout time.Duration) waitNotifier {
	wakeup := make(chan bool)
	timerModifier := make(chan time.Duration)

	go func() {
		timeout := defaultTimeout
		waitingCh := time.After(timeout)
		// TODO: should we have `rf.killed` here?
		for {
			select {

			// wait and loop
			case <-waitingCh:
				wakeup <- true
				waitingCh = time.After(timeout)

			// reset if receiving a new timer
			case t := <-timerModifier:
				timeout = t
				waitingCh = time.After(timeout)
			}
		}
	}()

	return waitNotifier{wakeup, timerModifier}
}
