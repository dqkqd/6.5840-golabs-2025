package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

type OptionInt struct {
	Value int
	Some  bool
}

// Debugging
var (
	Debug      bool
	debugStart time.Time
)

const (
	enableLogColor = true
)

type (
	logTopic string
	logColor string
)

const (
	tVote            logTopic = "VOTE"
	tSendAppend      logTopic = "SLOG"
	tReceiveAppend   logTopic = "RLOG"
	tHeartbeat       logTopic = "BEAT"
	tStart           logTopic = "STRT"
	tApply           logTopic = "APLY"
	tBecomeLeader    logTopic = "LEAD"
	tElection        logTopic = "ELCT"
	tStatus          logTopic = "STAT"
	tSnapshot        logTopic = "SNAP"
	tSendSnapshot    logTopic = "SSNP"
	tReceiveSnapshot logTopic = "RSNP"
)

const (
	tColorReset      logColor = "\033[0m"
	tColorBlack      logColor = "\033[30m"
	tColorRed        logColor = "\033[31m"
	tColorGreen      logColor = "\033[32m"
	tColorYellow     logColor = "\033[33m"
	tColorBlue       logColor = "\033[34m"
	tColorMagenta    logColor = "\033[35m"
	tColorCyan       logColor = "\033[36m"
	tColorWhite      logColor = "\033[37m"
	tColorTeal       logColor = "\033[38;5;38m"
	tColorPurple     logColor = "\033[38;5;93m"
	tColorDarkOrange logColor = "\033[38;5;208m"
	tColorOrange     logColor = "\033[38;5;214m"
	tColorGold       logColor = "\033[38;5;220m"
)

func logInit() {
	if debugStart.IsZero() {
		debugStart = time.Now()
		Debug = os.Getenv("DEBUG") == "1"
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}
}

func DPrintf(topic logTopic, format string, a ...any) {
	if Debug {
		at := time.Since(debugStart).Microseconds()
		at /= 100
		prefix := fmt.Sprintf("%08d RAFT %v ", at, topic)

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
		case tSendAppend:
			return tColorBlue
		case tReceiveAppend:
			return tColorTeal
		case tHeartbeat:
			return tColorMagenta
		case tStart:
			return tColorYellow
		case tApply:
			return tColorGreen
		case tElection:
			return tColorCyan
		case tStatus:
			return tColorPurple
		case tSnapshot:
			return tColorDarkOrange
		case tSendSnapshot:
			return tColorGold
		case tReceiveSnapshot:
			return tColorOrange
		}
	}
	return ""
}
