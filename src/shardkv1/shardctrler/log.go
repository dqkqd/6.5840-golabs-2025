package shardctrler

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
	tChangeConfig    logTopic = "CCFG"
	tServerGet       logTopic = "SGET"
	tServerPut       logTopic = "SPUT"
	tClerkGet        logTopic = "CGET"
	tClerkPut        logTopic = "CPUT"
	tSubmitErr       logTopic = "SUBE"
	tStop            logTopic = "STOP"
	tStart           logTopic = "STRT"
	tSnapshot        logTopic = "SNAP"
	tRestore         logTopic = "REST"
	tReceiveSnapshot logTopic = "RSNP"
	tElection        logTopic = "ELCT"
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
		Debug = os.Getenv("SHARDKV_DEBUG") == "1"
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}
}

func DPrintf(topic logTopic, format string, a ...any) {
	if Debug {
		at := time.Since(debugStart).Microseconds()
		at /= 100
		prefix := fmt.Sprintf("%08d SHARDCTRLER %v ", at, topic)

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
		case tSubmitErr:
			return tColorRed
		case tServerGet:
			return tColorBlue
		case tServerPut:
			return tColorTeal
		case tStart:
			return tColorMagenta
		case tClerkGet:
			return tColorYellow
		case tChangeConfig:
			return tColorGreen
		case tElection:
			return tColorCyan
		case tStop:
			return tColorPurple
		case tSnapshot:
			return tColorDarkOrange
		case tRestore:
			return tColorGold
		case tReceiveSnapshot:
			return tColorOrange
		}
	}
	return ""
}
