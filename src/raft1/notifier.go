package raft

import "time"

type waitNotifier struct {
	waitedFor       <-chan time.Duration // notify outer that we have finished waiting a cycle
	timeoutModifier chan<- time.Duration // allow outer to modify waiting timer
}

func (w *waitNotifier) changeTimeout(timeout time.Duration) {
	w.timeoutModifier <- timeout
}

func waitNotify(defaultTimeout time.Duration) waitNotifier {
	waitedFor := make(chan time.Duration)
	timerModifier := make(chan time.Duration)

	go func() {
		timeout := defaultTimeout
		waitingCh := time.After(timeout)
		// TODO: should we have `rf.killed` here?
		for {
			select {

			// wait and loop
			case <-waitingCh:
				waitedFor <- timeout
				waitingCh = time.After(timeout)

			// reset if receiving a new timer
			case t := <-timerModifier:
				timeout = t
				waitingCh = time.After(timeout)
			}
		}
	}()

	return waitNotifier{waitedFor, timerModifier}
}
