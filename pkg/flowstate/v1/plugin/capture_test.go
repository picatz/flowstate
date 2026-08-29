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
	var writes sync.WaitGroup
	writes.Add(1)
	go func() {
		defer writes.Done()
		<-start
		for range 1_000 {
			logger.Info("concurrent capture")
		}
	}()

	close(start)
	for range 1_000 {
		_ = logs.String()
	}
	writes.Wait()

	if got := strings.Count(logs.String(), "concurrent capture"); got != 1_000 {
		t.Fatalf("captured %d records, want 1000", got)
	}
}
