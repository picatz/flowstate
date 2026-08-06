// Package reachable proves that examples/plugins/github/ can actually reach
// the "github" plugin - the property CLAUDE.md requires of every capability
// ("a capability is not done until it is reachable from a Flowfile") and
// which, until this package existed, this plugin's README only asserted in
// prose.
//
// This is deliberately its own package, in its own directory, rather than a
// _test.go file beside main.go - see plugins/vcs/reachable's package comment
// for the reason in full: main.go imports this plugin's own generated types
// (githubv1.PullRequestGetInputs and so on), and a test file in that package
// would drag the same import in, registering "github/v1/github.proto" in
// this test binary's own global proto registry before the test ever ran.
// That would make the one thing this test exists to prove - that the engine
// reconstructs this plugin's schema from bytes sent over the wire rather than
// from having compiled it in - true by accident of the test's own layout
// rather than by the mechanism it is supposed to demonstrate. This package
// imports the plugin SDK's host side and nothing under plugins/github/gen, so
// its own registry starts exactly as bare as a real worker's does.
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

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// githubModule is this plugin's own module path, built as a real, separately
// compiled binary the same way `go -C plugins/github build` in its README
// does - not a fake standing in for the protocol.
const githubModule = "github.com/picatz/flowstate/plugins/github"

// exampleDir is where the example workflow files live, relative to this
// package.
const exampleDir = "../../../examples/plugins/github"

// TestAFlowfileCanNameTheGitHubPluginsTasks is deliberately one test:
// registering into [flowstatev1.DefaultRegistry] is a one-way door with no
// Unregister - see pkg/flowstate/v1/plugin's TestAFlowfileCanNameAPluginTask,
// which this mirrors - so at most one test in this binary may do it, and this
// is that one.
//
// It deliberately never runs github.pull_request_get, github.issue_comment,
// or any of the read/audit-tier tasks in triage.yaml: all reach the real
// GitHub API, issue_comment posts a real comment and needs a credential, and
// network access is not this test's business. What it proves is the seam a
// Flowfile actually depends on - that a step naming this plugin's tasks is
// refused before the plugin is registered and accepted once it is, using the
// descriptors the plugin really shipped, not ones this build knows in
// advance.
func TestAFlowfileCanNameTheGitHubPluginsTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"github")
	buildPlugin(t, binaryPath)

	readOnly := readExample(t, "workflow.yaml")
	mutation := readExample(t, "issue-comment.yaml")
	triage := readExample(t, "triage.yaml")

	// The premise. Before any host registers this plugin's tasks, a Flowfile
	// naming them is a Flowfile naming nothing - what every author who has not
	// installed the plugin actually has - and it is what makes the assertions
	// after registration mean something rather than being true of any file at
	// all.
	for _, name := range []string{
		"github.pull_request_get", "github.issue_comment",
		"github.pull_request_list", "github.pull_request_files",
		"github.issue_get", "github.issue_list",
	} {
		if _, ok := flowstatev1.LookupTask(name); ok {
			t.Fatalf("%q is already in the default registry before this test registered it, "+
				"so nothing below distinguishes a working seam from a task that was always there", name)
		}
	}

	before, err := flowfile.ValidateSource(readOnly)
	if err != nil {
		t.Fatalf("ValidateSource(workflow.yaml): unexpected error: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("the validator accepted a step naming a task no registry holds")
	}
	if text := diagnosticText(before); !strings.Contains(text, "github.pull_request_get") {
		t.Errorf("the diagnostics do not name %q, so an author who has not installed this "+
			"plugin gets nothing to search for; diagnostics:\n%s", "github.pull_request_get", text)
	}

	beforeMutation, err := flowfile.ValidateSource(mutation)
	if err != nil {
		t.Fatalf("ValidateSource(issue-comment.yaml): unexpected error: %v", err)
	}
	if len(beforeMutation) == 0 {
		t.Fatal("the validator accepted issue-comment.yaml's step naming a task no registry holds")
	}
	if text := diagnosticText(beforeMutation); !strings.Contains(text, "github.issue_comment") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "github.issue_comment", text)
	}

	beforeTriage, err := flowfile.ValidateSource(triage)
	if err != nil {
		t.Fatalf("ValidateSource(triage.yaml): unexpected error: %v", err)
	}
	if len(beforeTriage) == 0 {
		t.Fatal("the validator accepted triage.yaml's steps naming tasks no registry holds")
	}
	for _, name := range []string{
		"github.pull_request_list", "github.pull_request_files",
		"github.issue_get", "github.issue_list",
	} {
		if text := diagnosticText(beforeTriage); !strings.Contains(text, name) {
			t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", name, text)
		}
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

	t.Run("the validator accepts the read-only example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(readOnly)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/github/workflow.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the validator accepts the comment-posting example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(mutation)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/github/issue-comment.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the validator accepts the read/audit-tier triage example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(triage)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/github/triage.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("its schema is checked like a built-in's", func(t *testing.T) {
		// `number` is int64 in a schema this build never compiled; the only way
		// the validator can know that is the descriptor this plugin shipped
		// over its socket at launch. The step ordinarily writes `number:
		// ${vars.number}`; substituted here for a bare literal of the wrong
		// type, because a var reference's own type is not what is under test.
		wrongType, err := flowfile.ValidateSource([]byte(strings.Replace(
			string(readOnly), "number: ${vars.number}", `number: "one"`, 1)))
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(wrongType) == 0 {
			t.Error("a string was accepted for a var flowing into `number`, which the plugin declares as int64")
		}

		unknown, err := flowfile.ValidateSource([]byte(strings.Replace(
			string(readOnly), "owner: ${vars.owner}", "ownerr: ${vars.owner}", 1)))
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
		// "github.pull_request_get" comes from the binary's name on the
		// search path, which this plugin does not choose - not from
		// sdk.Plugin{Name: "github"} in main.go, which is what it says about
		// itself over the wire. Proving that means launching the very same
		// binary under a name it did not pick and checking what it is
		// registered as.
		spoofDir := t.TempDir()
		spoofed := filepath.Join(spoofDir, plugin.BinaryPrefix+"notgithub")
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

		p, ok := spoofHost.Lookup("notgithub")
		if !ok {
			t.Fatal("the renamed binary was not launched")
		}
		if got := p.Manifest().GetName(); got != "github" {
			t.Fatalf("the renamed binary's own manifest name = %q, want %q - "+
				"this subtest proves nothing if the fixture stops disagreeing with the binary name", got, "github")
		}

		registry := flowstatev1.NewRegistry()
		if err := spoofHost.Register(registry, nil); err != nil {
			t.Fatalf("Register: %v", err)
		}

		if _, ok := registry.Lookup("github.pull_request_get"); ok {
			t.Error("a plugin launched as \"notgithub\" registered a task under " +
				"\"github.pull_request_get\" - its self-declared name overrode discovery, " +
				"which is exactly what the dot in TaskManifest.name exists to make impossible")
		}
		if _, ok := registry.Lookup("notgithub.pull_request_get"); !ok {
			t.Error("a plugin discovered as \"notgithub\" did not register " +
				"\"notgithub.pull_request_get\" - the qualifier did not come from discovery at all")
		}
	})
}

// buildPlugin compiles the github plugin's own main package - the real
// plugin, not a stand-in for it - to the given path. Building it by import
// path rather than importing its packages directly is what keeps this test
// binary's own proto registry from ever seeing github.v1 before the plugin
// process sends it descriptors of its own accord.
func buildPlugin(t *testing.T, output string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "build", "-o", output, githubModule)
	// Run from within the module so the build resolves against this checkout
	// rather than a published version of it.
	if wd, err := os.Getwd(); err == nil {
		cmd.Dir = wd
	}

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the github plugin: %v: %s", err, out)
	}
}

// readExample reads a file from this plugin's own worked example directory,
// so that what is validated is the file an author would actually copy rather
// than a string living only inside this test.
func readExample(t *testing.T, name string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(exampleDir, name))
	if err != nil {
		t.Fatalf("reading the example workflow: %v", err)
	}
	return data
}

// copyFile duplicates a file's bytes and mode to a new path, standing in for
// a deployment that installed the same binary under a different name - which
// is all discovery ever looks at.
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

// testWriter adapts *testing.T to io.Writer. Writes are dropped once the test
// has finished, because a plugin's stderr pump can outlive the test that
// started it by a moment, and logging from a finished test panics.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	defer func() { _ = recover() }()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
