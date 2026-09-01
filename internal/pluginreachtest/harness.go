// Package pluginreachtest owns the process harness shared by first-party
// plugin reachability tests.
package pluginreachtest

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/internal/covbuild"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// BuildPlugin compiles a plugin's real main package without importing its
// generated descriptors into the reachability test process.
func BuildPlugin(t *testing.T, module, output string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	args := append([]string{"build"}, covbuild.BuildArgs()...)
	args = append(args, "-o", output, module)
	cmd := exec.CommandContext(ctx, "go", args...)
	if wd, err := os.Getwd(); err == nil {
		cmd.Dir = wd
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building plugin %s: %v: %s", module, err, out)
	}
}

// ReadFile reads a reachability fixture and fails the calling test on error.
func ReadFile(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading reachability fixture %s: %v", path, err)
	}
	return data
}

// CopyFile installs the same plugin binary under another discovery name.
func CopyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, info.Mode())
}

// OpenHost opens a host, forwards the subprocess coverage destination, and
// bounds cleanup so a failed plugin cannot strand the test.
func OpenHost(t *testing.T, cfg plugin.Config) *plugin.Host {
	t.Helper()

	// Plugin environments are built from scratch, so coverage reaches the
	// subprocess only when it is forwarded explicitly.
	cfg.Env = append(cfg.Env, covbuild.Env()...)
	host, err := plugin.NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		host.Close(ctx)
	})
	if err := host.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return host
}

// DiagnosticText joins diagnostics for assertions without changing their
// order or contents.
func DiagnosticText(diags flowfile.Diagnostics) string {
	var b strings.Builder
	for _, diag := range diags {
		b.WriteString(diag.Message)
		b.WriteByte('\n')
	}
	return b.String()
}

// Logger sends host and plugin logs to the test that launched them.
func Logger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testWriter drops writes after the test has finished because a plugin's
// stderr pump can outlive its launching test by a moment.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	defer func() { _ = recover() }()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
