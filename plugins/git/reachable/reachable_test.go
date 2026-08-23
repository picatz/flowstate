// Package reachable proves that examples/plugins/git's workflow files can
// actually reach the "git" plugin - the property CLAUDE.md requires of every
// capability ("a capability is not done until it is reachable from a
// Flowfile").
//
// This is deliberately its own package, in its own directory, rather than a
// _test.go file beside main.go - exactly the reason plugins/vcs/reachable
// gives, which applies unchanged here: main.go imports this plugin's own
// generated types (gitv1.LsRemoteInputs and so on), and a test file in that
// package would register "git/v1/git.proto" in this test binary's own
// global proto registry before the test ever ran, which is exactly the trap
// that made an earlier version of plugins/vcs's own test pass for the wrong
// reason (see that package's doc comment for the full story). This package
// imports the plugin SDK's host side and nothing under plugins/git/gen, so
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

	"github.com/picatz/flowstate/internal/covbuild"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// gitModule is this plugin's own module path, built as a real, separately
// compiled binary.
const gitModule = "github.com/picatz/flowstate/plugins/git"

// exampleDir is where the example workflow files live, relative to this
// package.
const exampleDir = "../../../examples/plugins/git"

// TestAFlowfileCanNameTheGitPluginsTasks is deliberately one test -
// registering into [flowstatev1.DefaultRegistry] is a one-way door with no
// Unregister, so at most one test in this binary may do it. See
// plugins/vcs/reachable's identical test for the full argument; this one
// covers all five of this plugin's example files - the runnable public-read
// example, the parameterized private-read example, the read/audit tier
// example (git.log and git.read_file), the cursor-resume example
// (git.log, twice, chained through next_cursor/cursor), and the
// parameterized write example - rather than one.
func TestAFlowfileCanNameTheGitPluginsTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"git")
	buildPlugin(t, binaryPath)

	readSource := readExample(t, "workflow.yaml")
	privateReadSource := readExample(t, "ls-remote-private.yaml")
	logAndReadFileSource := readExample(t, "log-and-read-file.yaml")
	logResumeSource := readExample(t, "log-resume.yaml")
	logPaginateSource := readExample(t, "log-paginate.yaml")
	writeSource := readExample(t, "commit-push.yaml")

	for _, name := range []string{"git.ls_remote", "git.log", "git.read_file", "git.commit_push"} {
		if _, ok := flowstatev1.LookupTask(name); ok {
			t.Fatalf("%q is already in the default registry before this test registered it, "+
				"so nothing below distinguishes a working seam from a task that was always there", name)
		}
	}

	beforeRead, err := flowfile.ValidateSource(readSource)
	if err != nil {
		t.Fatalf("ValidateSource(workflow.yaml): unexpected error: %v", err)
	}
	if len(beforeRead) == 0 {
		t.Fatal("the validator accepted workflow.yaml naming a task no registry holds")
	}
	if !strings.Contains(diagnosticText(beforeRead), "git.ls_remote") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "git.ls_remote", diagnosticText(beforeRead))
	}

	// ls-remote-private.yaml names the same task as workflow.yaml
	// (git.ls_remote) - proof, before this plugin is even registered, that
	// this is the auth-shapes pair the two files are meant to be: the same
	// unregistered task name refused in both, not two different tasks.
	beforePrivateRead, err := flowfile.ValidateSource(privateReadSource)
	if err != nil {
		t.Fatalf("ValidateSource(ls-remote-private.yaml): unexpected error: %v", err)
	}
	if len(beforePrivateRead) == 0 {
		t.Fatal("the validator accepted ls-remote-private.yaml naming a task no registry holds")
	}
	if !strings.Contains(diagnosticText(beforePrivateRead), "git.ls_remote") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "git.ls_remote", diagnosticText(beforePrivateRead))
	}

	beforeLogAndReadFile, err := flowfile.ValidateSource(logAndReadFileSource)
	if err != nil {
		t.Fatalf("ValidateSource(log-and-read-file.yaml): unexpected error: %v", err)
	}
	if len(beforeLogAndReadFile) == 0 {
		t.Fatal("the validator accepted log-and-read-file.yaml naming tasks no registry holds")
	}
	if !strings.Contains(diagnosticText(beforeLogAndReadFile), "git.log") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "git.log", diagnosticText(beforeLogAndReadFile))
	}
	if !strings.Contains(diagnosticText(beforeLogAndReadFile), "git.read_file") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "git.read_file", diagnosticText(beforeLogAndReadFile))
	}

	beforeLogResume, err := flowfile.ValidateSource(logResumeSource)
	if err != nil {
		t.Fatalf("ValidateSource(log-resume.yaml): unexpected error: %v", err)
	}
	if len(beforeLogResume) == 0 {
		t.Fatal("the validator accepted log-resume.yaml naming a task no registry holds")
	}
	if !strings.Contains(diagnosticText(beforeLogResume), "git.log") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "git.log", diagnosticText(beforeLogResume))
	}

	beforeLogPaginate, err := flowfile.ValidateSource(logPaginateSource)
	if err != nil {
		t.Fatalf("ValidateSource(log-paginate.yaml): unexpected error: %v", err)
	}
	if len(beforeLogPaginate) == 0 {
		t.Fatal("the validator accepted log-paginate.yaml naming a task no registry holds")
	}
	if !strings.Contains(diagnosticText(beforeLogPaginate), "git.log") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "git.log", diagnosticText(beforeLogPaginate))
	}

	beforeWrite, err := flowfile.ValidateSource(writeSource)
	if err != nil {
		t.Fatalf("ValidateSource(commit-push.yaml): unexpected error: %v", err)
	}
	if len(beforeWrite) == 0 {
		t.Fatal("the validator accepted commit-push.yaml naming a task no registry holds")
	}
	if !strings.Contains(diagnosticText(beforeWrite), "git.commit_push") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "git.commit_push", diagnosticText(beforeWrite))
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

	if err := host.Register(flowstatev1.DefaultRegistry(), nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Run("the validator accepts the real read example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(readSource)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/git/workflow.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the validator accepts the real private-read example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(privateReadSource)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/git/ls-remote-private.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the validator accepts the real read/audit-tier example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(logAndReadFileSource)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/git/log-and-read-file.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the validator accepts the real cursor-resume example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(logResumeSource)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/git/log-resume.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the validator accepts the loop-paginated walk", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(logPaginateSource)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/git/log-paginate.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the validator accepts the real write example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(writeSource)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/git/commit-push.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the read task's schema is checked like a built-in's", func(t *testing.T) {
		// prefix is a string in git.v1.LsRemoteInputs; the only way the
		// validator can know that is the descriptor this plugin shipped
		// over its socket at launch, reconstructed by the host.
		//
		// The substitution is checked to have happened. It is a textual edit of
		// an example this test does not own, so the day that example's spelling
		// moves — `prefix: "refs/heads/"` became `prefix: refs/heads/` when
		// #850 settled the formatter's quoting — a replace that matches nothing
		// hands the validator the unmodified, *valid* example and the assertion
		// below reads as the plugin's schema not being checked at all.
		broken := strings.Replace(string(readSource), "prefix: refs/heads/", "prefix: 5", 1)
		if broken == string(readSource) {
			t.Fatal("the example no longer spells `prefix:` the way this substitution expects, so nothing " +
				"was made wrong and this case would pass without checking anything")
		}

		wrongType, err := flowfile.ValidateSource([]byte(broken))
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(wrongType) == 0 {
			t.Error("an int was accepted for prefix, which the plugin declares as string")
		}
	})

	t.Run("the task's qualifier comes from discovery, not from what the plugin calls itself", func(t *testing.T) {
		spoofDir := t.TempDir()
		spoofed := filepath.Join(spoofDir, plugin.BinaryPrefix+"notgit")
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

		p, ok := spoofHost.Lookup("notgit")
		if !ok {
			t.Fatal("the renamed binary was not launched")
		}
		if got := p.Manifest().GetName(); got != "git" {
			t.Fatalf("the renamed binary's own manifest name = %q, want %q", got, "git")
		}

		registry := flowstatev1.NewRegistry()
		if err := spoofHost.Register(registry, nil); err != nil {
			t.Fatalf("Register: %v", err)
		}

		if _, ok := registry.Lookup("git.ls_remote"); ok {
			t.Error("a plugin launched as \"notgit\" registered a task under \"git.ls_remote\" - " +
				"its self-declared name overrode discovery")
		}
		if _, ok := registry.Lookup("notgit.ls_remote"); !ok {
			t.Error("a plugin discovered as \"notgit\" did not register \"notgit.ls_remote\"")
		}
	})
}

func buildPlugin(t *testing.T, output string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// -cover only when internal/covbuild says coverage was asked for, which
	// is `make coverage` and nothing else. This binary is a real subprocess:
	// without instrumentation every line it runs is invisible to the harness
	// that launched it, and this plugin's end-to-end path runs nowhere else.
	args := append([]string{"build"}, covbuild.BuildArgs()...)
	args = append(args, "-o", output, gitModule)

	cmd := exec.CommandContext(ctx, "go", args...)
	if wd, err := os.Getwd(); err == nil {
		cmd.Dir = wd
	}

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the git plugin: %v: %s", err, out)
	}
}

func readExample(t *testing.T, name string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(exampleDir, name))
	if err != nil {
		t.Fatalf("reading the example workflow: %v", err)
	}
	return data
}

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

func diagnosticText(diags flowfile.Diagnostics) string {
	var b strings.Builder
	for _, d := range diags {
		b.WriteString(d.Message)
		b.WriteString("\n")
	}
	return b.String()
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	defer func() { _ = recover() }()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
