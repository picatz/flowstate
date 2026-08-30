package plugin

import (
	"strings"
	"sync"
	"testing"
)

// TestCapturedLogsSynchronizesReadAndWrite covers the logger-capture race from
// #1228 directly: plugin stderr is relayed asynchronously while a test may
// inspect what has been captured before its Host cleanup stops the pump.
func TestCapturedLogsSynchronizesReadAndWrite(t *testing.T) {
	var logs capturedLogs
	logger := newCapturingLogger(t, &logs)

	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for range 1_000 {
			logger.Info("concurrent capture")
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for range 1_000 {
			_ = logs.String()
		}
	}()

	close(start)
	workers.Wait()

	if got := strings.Count(logs.String(), "concurrent capture"); got != 1_000 {
		t.Fatalf("captured %d records, want 1000", got)
	}
}
