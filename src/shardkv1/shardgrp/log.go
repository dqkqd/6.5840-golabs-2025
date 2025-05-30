package shardgrp

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
	tClerkGet           logTopic = "CGET"
	tClerkPut           logTopic = "CPUT"
	tClerkFreezeShard   logTopic = "CFRZ"
	tClerkInstallShard  logTopic = "CINS"
	tClerkDeleteShard   logTopic = "CDEL"
	tServerGet          logTopic = "SGET"
	tServerPut          logTopic = "SPUT"
	tServerFreezeShard  logTopic = "SFRZ"
	tServerInstallShard logTopic = "SINS"
	tServerDeleteShard  logTopic = "SDEL"
	tDoOp               logTopic = "DOOP"

	tSnapshot logTopic = "SNAP"
	tRestore  logTopic = "REST"
)

const (
	tColorReset logColor = "\033[0m"

	tColorRed logColor = "\033[31m"

	tColorBlue   logColor = "\033[34m"
	tColorTeal   logColor = "\033[38;5;38m"
	tColorGreen  logColor = "\033[32m"
	tColorYellow logColor = "\033[33m"
	tColorOrange logColor = "\033[38;5;214m"

	tColorMagenta logColor = "\033[35m"
	tColorPurple  logColor = "\033[38;5;93m"
	tColorCyan    logColor = "\033[36m"
	tColorBlack   logColor = "\033[30m"
	tColorPink    logColor = "\033[38;5;200m"

	tColorWhite logColor = "\033[37m"
	tColorLime  logColor = "\033[38;5;154m"

	tColorDarkOrange logColor = "\033[38;5;208m"
	tColorGold       logColor = "\033[38;5;220m"
)

func logInit() {
	if debugStart.IsZero() {
		debugStart = time.Now()
		Debug = os.Getenv("SHARDKV_DEBUG") == "1"
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}
}

func DPrintf(topic logTopic, format string, a ...any) {
	if Debug {
		at := time.Since(debugStart).Microseconds()
		at /= 100
		prefix := fmt.Sprintf("%08d SHARDGRP %v ", at, topic)

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
		case tDoOp:
			return tColorRed

		case tServerPut:
			return tColorBlue
		case tServerGet:
			return tColorTeal
		case tServerInstallShard:
			return tColorGreen
		case tServerFreezeShard:
			return tColorYellow
		case tServerDeleteShard:
			return tColorOrange

		case tClerkGet:
			return tColorMagenta
		case tClerkPut:
			return tColorPurple
		case tClerkFreezeShard:
			return tColorCyan
		case tClerkInstallShard:
			return tColorBlack
		case tClerkDeleteShard:
			return tColorPink

		case tSnapshot:
			return tColorDarkOrange
		case tRestore:
			return tColorGold
		}
	}
	return ""
}
