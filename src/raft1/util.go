package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const (
	Debug          = true
	enableLogColor = true
)

var debugStart time.Time

type (
	logTopic string
	logColor string
)

const (
	tVote         logTopic = "VOTE"
	tAppend       logTopic = "APND"
	tHeartbeat    logTopic = "BEAT"
	tStart        logTopic = "STRT"
	tApply        logTopic = "APLY"
	tBecomeLeader logTopic = "LEAD"
	tElection     logTopic = "ELCT"
)

const (
	tColorReset   logColor = "\033[0m"
	tColorBlack   logColor = "\033[30m"
	tColorRed     logColor = "\033[31m"
	tColorGreen   logColor = "\033[32m"
	tColorYellow  logColor = "\033[33m"
	tColorBlue    logColor = "\033[34m"
	tColorMagenta logColor = "\033[35m"
	tColorCyan    logColor = "\033[36m"
	tColorWhite   logColor = "\033[37m"
)

func logInit() {
	if debugStart.IsZero() {
		debugStart = time.Now()
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}
}

func DPrintf(topic logTopic, format string, a ...any) {
	if Debug {
		at := time.Since(debugStart).Microseconds()
		at /= 100
		prefix := fmt.Sprintf("%08d %v ", at, topic)

		format = prefix + format

		color := topic.color()
		if len(color) > 0 {
			format = string(color) + format + string(tColorReset)
		}

		log.Printf(format, a...)
	}
}

func (t logTopic) color() logColor {
	if enableLogColor {
		switch t {
		case tVote:
			return tColorRed
		case tAppend:
			return tColorBlue
		case tHeartbeat:
			return tColorMagenta
		case tStart:
			return tColorYellow
		case tApply:
			return tColorGreen
		case tElection:
			return tColorCyan
		}
	}
	return ""
}
