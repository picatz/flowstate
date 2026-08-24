// Package reachable proves that examples/plugins/codex/workflow.yaml can
// actually reach the "codex" plugin - the property CLAUDE.md requires of
// every capability ("a capability is not done until it is reachable from a
// Flowfile") and which, until this package existed, this plugin's README
// only asserted in prose.
//
// This is deliberately its own package, in its own directory, rather than a
// _test.go file beside main.go - the same trap plugins/vcs/reachable's own
// package doc documents at length, and the same fix: plugins/codex's main
// package imports its own generated types (codexv1.ExecInputs and so on) to
// run the plugin, and a test file in that package would register
// "codex/v1/codex.proto" in this test binary's own global proto registry
// before the test ever ran - which is exactly the condition that let an
// earlier version of plugins/vcs's own equivalent test pass for the wrong
// reason (see that package's doc comment for the specific failure mode).
// This package imports the plugin SDK's host side and nothing under
// plugins/codex/gen, so its own registry starts exactly as bare as a real
// worker's does.
package reachable

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/internal/covbuild"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// codexModule is this plugin's own module path, built as a real, separately
// compiled binary the same way `go -C plugins/codex build` in its README
// does - not a fake standing in for the protocol.
const codexModule = "github.com/picatz/flowstate/plugins/codex"

// exampleDir is where the example workflow file lives, relative to this
// package.
const exampleDir = "../../../examples/plugins/codex"

// TestAFlowfileCanNameTheCodexPluginsTasks is deliberately one test:
// registering into [flowstatev1.DefaultRegistry] is a one-way door with no
// Unregister - see pkg/flowstate/v1/plugin's TestAFlowfileCanNameAPluginTask,
// which this mirrors - so at most one test in this binary may do it, and
// this is that one.
//
// It deliberately never runs codex.exec: that reaches the real OpenAI API
// through a real codex binary this test environment has no business
// holding a credential for. What it proves is the seam a Flowfile actually
// depends on - that a step naming this plugin's task is refused before the
// plugin is registered and accepted once it is, using the descriptors the
// plugin really shipped, not ones this build knows in advance.
func TestAFlowfileCanNameTheCodexPluginsTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"codex")
	buildPlugin(t, binaryPath)

	source := readExample(t, "workflow.yaml")

	// The premise. Before any host registers this plugin's tasks, a Flowfile
	// naming them is a Flowfile naming nothing - what every author who has
	// not installed the plugin actually has - and it is what makes the
	// assertions after registration mean something rather than being true
	// of any file at all.
	if _, ok := flowstatev1.LookupTask("codex.exec"); ok {
		t.Fatal("\"codex.exec\" is already in the default registry before this test registered it, " +
			"so nothing below distinguishes a working seam from a task that was always there")
	}

	before, err := flowfile.ValidateSource(source)
	if err != nil {
		t.Fatalf("ValidateSource: unexpected error: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("the validator accepted a step naming a task no registry holds")
	}
	if !strings.Contains(diagnosticText(before), "codex.exec") {
		t.Errorf("the diagnostics do not name \"codex.exec\", so an author who has not installed "+
			"this plugin gets nothing to search for; diagnostics:\n%s", diagnosticText(before))
	}

	host := openHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         10 * time.Second,
		HealthTimeout:       5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              testLogger(t),
	})

	// The seam: one call, against the registry the engine actually reads.
	if err := host.Register(flowstatev1.DefaultRegistry(), nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Run("the validator accepts the real example file", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(source)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's task is registered and `flow validate` still refuses "+
				"examples/plugins/codex/workflow.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("its schema is checked like a built-in's", func(t *testing.T) {
		// A misspelled input is the one shape `flow validate` reliably
		// refuses for a message schema it learned from this plugin's own
		// descriptors rather than from anything compiled in - the same
		// check plugins/vcs/reachable and plugins/github/reachable make of
		// their own example files.
		unknown, err := flowfile.ValidateSource([]byte(strings.Replace(
			string(source), "prompt: ${vars.prompt}", "promptt: ${vars.prompt}", 1)))
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(unknown) == 0 {
			t.Error("a misspelled input was accepted, so a typo runs and does nothing")
		}
	})

	t.Run("the task's qualifier comes from discovery, not from what the plugin calls itself", func(t *testing.T) {
		// TaskManifest.name may never contain a dot, precisely so a plugin
		// cannot smuggle its own qualifier: the segment before the dot in
		// "codex.exec" comes from the binary's name on the search path,
		// which this plugin does not choose - not from sdk.Plugin{Name:
		// "codex"} in main.go, which is what it says about itself over the
		// wire. Proving that means launching the very same binary under a
		// name it did not pick and checking what it is registered as.
		spoofDir := t.TempDir()
		spoofed := filepath.Join(spoofDir, plugin.BinaryPrefix+"notcodex")
		if err := copyFile(binaryPath, spoofed); err != nil {
			t.Fatalf("copying the plugin binary under another name: %v", err)
		}

		spoofHost := openHost(t, plugin.Config{
			SearchPath:          []string{spoofDir},
			HandshakeTimeout:    10 * time.Second,
			DescribeTimeout:     10 * time.Second,
			CallTimeout:         10 * time.Second,
			HealthTimeout:       5 * time.Second,
			ShutdownGrace:       5 * time.Second,
			DisableHealthChecks: true,
			Logger:              testLogger(t),
		})

		p, ok := spoofHost.Lookup("notcodex")
		if !ok {
			t.Fatal("the renamed binary was not launched")
		}
		if got := p.Manifest().GetName(); got != "codex" {
			t.Fatalf("the renamed binary's own manifest name = %q, want %q - "+
				"this subtest proves nothing if the fixture stops disagreeing with the binary name", got, "codex")
		}

		registry := flowstatev1.NewRegistry()
		if err := spoofHost.Register(registry, nil); err != nil {
			t.Fatalf("Register: %v", err)
		}

		if _, ok := registry.Lookup("codex.exec"); ok {
			t.Error("a plugin launched as \"notcodex\" registered a task under \"codex.exec\" - " +
				"its self-declared name overrode discovery, which is exactly what the dot in " +
				"TaskManifest.name exists to make impossible")
		}
		if _, ok := registry.Lookup("notcodex.exec"); !ok {
			t.Error("a plugin discovered as \"notcodex\" did not register \"notcodex.exec\" - " +
				"the qualifier did not come from discovery at all")
		}
	})
}

// buildPlugin compiles the codex plugin's own main package - the real
// plugin, not a stand-in for it - to the given path. Building it by import
// path rather than importing its packages directly is what keeps this test
// binary's own proto registry from ever seeing codex.v1 before the plugin
// process sends it descriptors of its own accord.
func buildPlugin(t *testing.T, output string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// -cover only when internal/covbuild says coverage was asked for, which
	// is `make coverage` and nothing else. This binary is a real subprocess:
	// without instrumentation every line it runs is invisible to the harness
	// that launched it, and this plugin's end-to-end path runs nowhere else.
	args := append([]string{"build"}, covbuild.BuildArgs()...)
	args = append(args, "-o", output, codexModule)

	cmd := exec.CommandContext(ctx, "go", args...)
	if wd, err := os.Getwd(); err == nil {
		cmd.Dir = wd
	}

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the codex plugin: %v: %s", err, out)
	}
}

// readExample reads a file from this plugin's own worked example directory,
// so that what is validated is the file an author would actually copy
// rather than a string living only inside this test.
func readExample(t *testing.T, name string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(exampleDir, name))
	if err != nil {
		t.Fatalf("reading the example workflow: %v", err)
	}
	return data
}

// copyFile duplicates a file's bytes and mode to a new path, standing in for
// a deployment that installed the same binary under a different name -
// which is all discovery ever looks at.
func copyFile(src, dst string) error {
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

// openHost opens a [plugin.Host] and closes it when the test ends.
func openHost(t *testing.T, cfg plugin.Config) *plugin.Host {
	t.Helper()

	// A plugin's environment is built from scratch rather than inherited
	// (see plugin.pluginEnv), so the coverage destination reaches the
	// process only if it is named here. Empty unless FLOWSTATE_COVERDIR is
	// set, which makes this a no-op outside `make coverage`.
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

// diagnosticText joins diagnostics into one string to assert against.
func diagnosticText(diags flowfile.Diagnostics) string {
	var b strings.Builder
	for _, d := range diags {
		b.WriteString(d.Message)
		b.WriteString("\n")
	}
	return b.String()
}

// testLogger sends host and plugin logs to the test's own output, so a
// failure comes with the plugin's stderr rather than without it.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testWriter adapts *testing.T to io.Writer. Writes are dropped once the
// test has finished, because a plugin's stderr pump can outlive the test
// that started it by a moment, and logging from a finished test panics.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	defer func() { _ = recover() }()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
