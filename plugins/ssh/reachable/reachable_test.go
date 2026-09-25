// Package reachable proves that examples/plugins/ssh/workflow.yaml can reach
// the "ssh" plugin, and that an unconfigured one refuses rather than improvises.
//
// Its own package for the reason plugins/vcs records: a test beside main.go
// would register ssh/v1/ssh.proto in this binary's global proto registry before
// the test ran, and a process holding the schema cannot tell a working
// descriptor reconstruction from a hit against its own registry.
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

const (
	sshModule  = "github.com/picatz/flowstate/plugins/ssh"
	exampleDir = "../../../examples/plugins/ssh"

	// grantsEnv is the variable an operator points at a grants file with. It is
	// spelled here rather than imported because this package deliberately
	// imports nothing of the plugin's own.
	grantsEnv = "FLOWSTATE_SSH_GRANTS"
)

// TestAFlowfileCanNameTheSSHPluginsTask registers into the default registry,
// which is a one-way door, so it is the only test in this binary that does.
func TestAFlowfileCanNameTheSSHPluginsTask(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"ssh")
	pluginreachtest.BuildPlugin(t, sshModule, binaryPath)

	source := pluginreachtest.ReadFile(t, filepath.Join(exampleDir, "workflow.yaml"))

	if _, ok := flowstatev1.LookupTask("ssh.run"); ok {
		t.Fatal("ssh.run is in the default registry before this test registered it, so nothing below " +
			"distinguishes a working seam from a task that was always there")
	}

	before, err := flowfile.ValidateSource(source)
	if err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("the validator accepted a step naming a task no registry holds")
	}

	// Launched with the example's own grants file, which is what an operator
	// configures this plugin with - and what nothing could do before
	// Config.EnvByPlugin existed.
	host := pluginreachtest.OpenHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         10 * time.Second,
		HealthTimeout:       5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              pluginreachtest.Logger(t),
		EnvByPlugin: map[string][]string{
			"ssh": {grantsEnv + "=" + mustAbs(t, filepath.Join(exampleDir, "grants.yaml"))},
		},
	})

	if err := host.Register(flowstatev1.DefaultRegistry(), nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Run("the validator accepts the real example file", func(t *testing.T) {
		after, err := flowfile.ValidateSource(source)
		if err != nil {
			t.Fatalf("ValidateSource: %v", err)
		}
		if len(after) != 0 {
			t.Errorf("the example does not validate against the descriptors the plugin shipped:\n%s",
				pluginreachtest.DiagnosticText(after))
		}
	})

	t.Run("the task takes no credential input at all", func(t *testing.T) {
		// The design claim, read off the launched plugin's own manifest: a key
		// is named by the operator's grant and never travels as a task input,
		// so there is nothing here for a Flowfile to supply or leak.
		for _, def := range host.TaskDefs() {
			if len(def.SecretInputs) != 0 {
				t.Errorf("%s declares secret inputs %v; an SSH identity is operator authority, not a per-call credential",
					def.Name, def.SecretInputs)
			}
		}
	})
}

// TestAnUnconfiguredPluginRefusesRatherThanImprovises is the fail-closed
// direction of the grants contract, through a real launched process: with no
// grants file there is no host and no command, and a call says so.
func TestAnUnconfiguredPluginRefusesRatherThanImprovises(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"ssh")
	pluginreachtest.BuildPlugin(t, sshModule, binaryPath)

	host := pluginreachtest.OpenHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         10 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              pluginreachtest.Logger(t),
		// No EnvByPlugin: the operator configured nothing.
	})

	defs := host.TaskDefs()
	if len(defs) != 1 || defs[0].Name != "ssh.run" {
		t.Fatalf("the launched plugin does not offer exactly ssh.run: %v", defs)
	}

	_, err := defs[0].Fn(t.Context(), map[string]*flowstatev1.Value{
		"host":    flowstatev1.NewValue("web-prod"),
		"command": flowstatev1.NewValue("restart-service"),
	}, nil)
	if err == nil {
		t.Fatal("an unconfigured ssh plugin ran something")
	}
	if !strings.Contains(err.Error(), grantsEnv) {
		t.Fatalf("the refusal does not name what an operator would set: %v", err)
	}
}

// mustAbs resolves a path for the launched plugin, which runs with a working
// directory of its own.
func mustAbs(t *testing.T, path string) string {
	t.Helper()

	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolving %q: %v", path, err)
	}
	return absolute
}
