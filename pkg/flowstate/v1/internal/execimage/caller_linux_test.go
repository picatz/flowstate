//go:build linux

package execimage

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
)

// testImage models what a caller does with this package's gate: open the file,
// ask whether the kernel executes it directly, and either execute the
// descriptor's /proc path or fall back to the path and say so. The plugin host
// and the exec task are the real callers; this keeps the gate's end-to-end
// behavior (pinned or not, and what is logged) testable beside the gate.
type testImage struct {
	file     *os.File
	execPath string
	pinned   bool
}

func (im *testImage) close() { im.file.Close() }

func openExecImage(path string, log *slog.Logger) (*testImage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := RefuseUnlessExecutedDirectly(f, info); err != nil {
		log.Warn("executed by path rather than by the descriptor, so the digest says what the path held "+
			"rather than proving what ran", "path", path, "reason", err)
		return &testImage{file: f, execPath: path}, nil
	}
	held, err := RaiseAbove(f, FDFloor)
	if err != nil {
		return nil, err
	}
	execPath, err := Path(held, info)
	if err != nil {
		log.Warn("executed by path rather than by the descriptor, so the digest says what the path held "+
			"rather than proving what ran", "path", path, "reason", err)
		return &testImage{file: held, execPath: path}, nil
	}

	return &testImage{file: held, execPath: execPath, pinned: true}, nil
}

type capturedLogs struct {
	mu      sync.Mutex
	builder strings.Builder
}

func (c *capturedLogs) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.builder.Write(p)
}

func (c *capturedLogs) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.builder.String()
}

func newCapturingLogger(t *testing.T, into *capturedLogs) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.MultiWriter(into, testWriter{t}),
		&slog.HandlerOptions{Level: slog.LevelDebug}))
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, nil))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
