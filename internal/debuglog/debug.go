package debuglog

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

const (
	logPath = "/media/dibin/HDD1/Works/tdrive-plugin/.cursor/debug-f51fed.log"
	session = "f51fed"
)

var writeMu sync.Mutex

// Write appends one structured diagnostic event. Callers must only pass
// operational state; credentials, tokens, and command output are forbidden.
func Write(hypothesisID, location, message string, data map[string]any) {
	payload := map[string]any{
		"sessionId":    session,
		"hypothesisId": hypothesisID,
		"location":     location,
		"message":      message,
		"data":         data,
		"timestamp":    time.Now().UnixMilli(),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}

	writeMu.Lock()
	defer writeMu.Unlock()
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = file.Write(append(encoded, '\n'))
	_ = file.Close()
}
