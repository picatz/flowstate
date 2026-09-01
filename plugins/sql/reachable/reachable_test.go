// Package reachable proves that examples/plugins/sql's workflow files can
// actually reach the "sql" plugin - the property CLAUDE.md requires of
// every capability ("a capability is not done until it is reachable from a
// Flowfile").
//
// This is deliberately its own package, in its own directory, rather than a
// _test.go file beside main.go - exactly the reason plugins/git/reachable
// and plugins/vcs/reachable give, which applies unchanged here: main.go
// imports this plugin's own generated types (sqlv1.QueryInputs and so on),
// and a test file in that package would register "sql/v1/sql.proto" in this
// test binary's own global proto registry before the test ever ran, which
// is exactly the trap that made an earlier version of plugins/vcs's own
// test pass for the wrong reason (see that package's doc comment for the
// full story). This package imports the plugin SDK's host side and nothing
// under plugins/sql/gen, so its own registry starts exactly as bare as a
// real worker's does.
package reachable

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/internal/pluginreachtest"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// sqlModule is this plugin's own module path, built as a real, separately
// compiled binary.
const sqlModule = "github.com/picatz/flowstate/plugins/sql"

// exampleDir is where the example workflow files live, relative to this
// package.
const exampleDir = "../../../examples/plugins/sql"

// TestAFlowfileCanNameTheSQLPluginsTasks is deliberately one test -
// registering into [flowstatev1.DefaultRegistry] is a one-way door with no
// Unregister, so at most one test in this binary may do it. See
// plugins/git/reachable's identical test for the full argument; this one
// covers both of this plugin's example files - the read query and the
// write transaction.
func TestAFlowfileCanNameTheSQLPluginsTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"sql")
	buildPlugin(t, binaryPath)

	querySource := readExample(t, "workflow.yaml")
	execSource := readExample(t, "transfer.yaml")

	for _, name := range []string{"sql.query", "sql.exec"} {
		if _, ok := flowstatev1.LookupTask(name); ok {
			t.Fatalf("%q is already in the default registry before this test registered it, "+
				"so nothing below distinguishes a working seam from a task that was always there", name)
		}
	}

	beforeQuery, err := flowfile.ValidateSource(querySource)
	if err != nil {
		t.Fatalf("ValidateSource(workflow.yaml): unexpected error: %v", err)
	}
	if len(beforeQuery) == 0 {
		t.Fatal("the validator accepted workflow.yaml naming a task no registry holds")
	}
	if !strings.Contains(diagnosticText(beforeQuery), "sql.query") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "sql.query", diagnosticText(beforeQuery))
	}

	beforeExec, err := flowfile.ValidateSource(execSource)
	if err != nil {
		t.Fatalf("ValidateSource(transfer.yaml): unexpected error: %v", err)
	}
	if len(beforeExec) == 0 {
		t.Fatal("the validator accepted transfer.yaml naming a task no registry holds")
	}
	if !strings.Contains(diagnosticText(beforeExec), "sql.exec") {
		t.Errorf("the diagnostics do not name %q; diagnostics:\n%s", "sql.exec", diagnosticText(beforeExec))
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

	t.Run("the validator accepts the real query example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(querySource)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/sql/workflow.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("the validator accepts the real exec example", func(t *testing.T) {
		diags, err := flowfile.ValidateSource(execSource)
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(diags) != 0 {
			t.Errorf("this plugin's tasks are registered and `flow validate` still refuses "+
				"examples/plugins/sql/transfer.yaml: %s", diagnosticText(diags))
		}
	})

	t.Run("both SQL tasks refuse a literal DSN before compilation", func(t *testing.T) {
		for name, source := range map[string][]byte{"sql.query": querySource, "sql.exec": execSource} {
			t.Run(name, func(t *testing.T) {
				literal := "literal-dsn-must-not-enter-history"
				changed := strings.Replace(string(source), "${secret('env:SQL_DSN')}", literal, 1)
				diags, err := flowfile.ValidateSource([]byte(changed))
				if err != nil {
					t.Fatalf("ValidateSource: unexpected error: %v", err)
				}
				got := diagnosticText(diags)
				if !strings.Contains(got, "whole secret reference") {
					t.Fatalf("literal DSN diagnostics = %q, want required-secret refusal", got)
				}
				if strings.Contains(got, literal) {
					t.Fatalf("literal DSN leaked into diagnostics: %q", got)
				}
			})
		}
	})

	t.Run("engine is a closed enum, checked like a built-in's", func(t *testing.T) {
		// engine is Engine in sql.v1.QueryInputs; the only way the validator
		// can know its choices is the descriptor this plugin shipped over
		// its socket at launch, reconstructed by the host.
		wrongEngine, err := flowfile.ValidateSource([]byte(strings.Replace(
			string(querySource), "ENGINE_POSTGRES", "ENGINE_ORACLE", 1)))
		if err != nil {
			t.Fatalf("ValidateSource: unexpected error: %v", err)
		}
		if len(wrongEngine) == 0 {
			t.Error("\"ENGINE_ORACLE\" was accepted for engine, which this build does not support")
		}
		if !strings.Contains(diagnosticText(wrongEngine), "sqlite") || !strings.Contains(diagnosticText(wrongEngine), "postgres") {
			t.Errorf("the diagnostic does not list what this build supports; diagnostics:\n%s", diagnosticText(wrongEngine))
		}
	})

	t.Run("the task's qualifier comes from discovery, not from what the plugin calls itself", func(t *testing.T) {
		spoofDir := t.TempDir()
		spoofed := filepath.Join(spoofDir, plugin.BinaryPrefix+"notsql")
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

		p, ok := spoofHost.Lookup("notsql")
		if !ok {
			t.Fatal("the renamed binary was not launched")
		}
		if got := p.Manifest().GetName(); got != "sql" {
			t.Fatalf("the renamed binary's own manifest name = %q, want %q", got, "sql")
		}

		registry := flowstatev1.NewRegistry()
		if err := spoofHost.Register(registry, nil); err != nil {
			t.Fatalf("Register: %v", err)
		}

		if _, ok := registry.Lookup("sql.query"); ok {
			t.Error("a plugin launched as \"notsql\" registered a task under \"sql.query\" - " +
				"its self-declared name overrode discovery")
		}
		if _, ok := registry.Lookup("notsql.query"); !ok {
			t.Error("a plugin discovered as \"notsql\" did not register \"notsql.query\"")
		}
	})
}

func buildPlugin(t *testing.T, output string) {
	pluginreachtest.BuildPlugin(t, sqlModule, output)
}

func readExample(t *testing.T, name string) []byte {
	return pluginreachtest.ReadFile(t, filepath.Join(exampleDir, name))
}

var copyFile = pluginreachtest.CopyFile

func openHost(t *testing.T, cfg plugin.Config) *plugin.Host {
	return pluginreachtest.OpenHost(t, cfg)
}

func diagnosticText(diags flowfile.Diagnostics) string {
	return pluginreachtest.DiagnosticText(diags)
}

var testLogger = pluginreachtest.Logger
