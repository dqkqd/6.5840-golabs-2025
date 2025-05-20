package rsm

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
	tApply         logTopic = "APLY"
	tSendReturn    logTopic = "SRTN"
	tReceiveReturn logTopic = "RRTN"
	tSubmit        logTopic = "SUBM"
	tSubmitOk      logTopic = "SUBO"
	tSubmitErr     logTopic = "SUBE"
	tStop          logTopic = "STOP"
	tStart         logTopic = "STRT"

	tElection        logTopic = "ELCT"
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
		Debug = os.Getenv("RSM_DEBUG") == "1"
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}
}

func DPrintf(topic logTopic, format string, a ...any) {
	if Debug {
		at := time.Since(debugStart).Microseconds()
		at /= 100
		prefix := fmt.Sprintf("%08d RSM %v ", at, topic)

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
		case tSendReturn:
			return tColorBlue
		case tReceiveReturn:
			return tColorTeal
		case tStart:
			return tColorMagenta
		case tSubmit:
			return tColorYellow
		case tApply:
			return tColorGreen
		case tElection:
			return tColorCyan
		case tStop:
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
