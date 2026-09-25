// Package reachable proves that examples/plugins/oci/workflow.yaml can
// actually reach the "oci" plugin - the property AGENTS.md requires of every
// capability (a feature is incomplete until an author can express it) and
// which a plugin's README can otherwise only assert in prose.
//
// It is deliberately its own package rather than a _test.go file beside
// main.go, for the reason plugins/vcs records: the main package imports its own
// generated types, which would register oci/v1/oci.proto in the test binary's
// global proto registry before the test ran, and a process that already has the
// schema compiled in cannot tell a working descriptor reconstruction from an
// accidental hit against its own registry. This package imports the host side
// and nothing under plugins/oci/gen, so its registry starts as bare as a real
// worker's.
package reachable

import (
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/internal/pluginreachtest"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// ociModule is this plugin's own module path, built as a real, separately
// compiled binary the same way its README says to build it.
const ociModule = "github.com/picatz/flowstate/plugins/oci"

// exampleDir is where the worked example lives, relative to this package.
const exampleDir = "../../../examples/plugins/oci"

// deniedHost is the registry the operator policy below refuses. It is denied by
// name, so the refusal happens before any name resolution and this test reaches
// no network.
const deniedHost = "ghcr.io"

// operatorPolicyDenying is the file an operator writes to stop this plugin
// reading from one registry - the deployment deciding which registries a
// workflow may pull evidence from, without uninstalling the plugin.
const operatorPolicyDenying = "egress:\n  deny:\n    - host == \"" + deniedHost + "\"\n"

// TestAFlowfileCanNameTheOCIPluginsTasks is deliberately one test: registering
// into [flowstatev1.DefaultRegistry] is a one-way door with no Unregister, so
// at most one test in this binary may do it, and this is that one.
//
// It never calls a task against a real registry. What it proves is the seam a
// Flowfile depends on - that a step naming these tasks is refused before the
// plugin is registered and accepted once it is, against the descriptors the
// plugin really shipped rather than ones this build knew in advance.
func TestAFlowfileCanNameTheOCIPluginsTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"oci")
	pluginreachtest.BuildPlugin(t, ociModule, binaryPath)

	source := pluginreachtest.ReadFile(t, filepath.Join(exampleDir, "workflow.yaml"))
	tasks := []string{"oci.resolve", "oci.referrers", "oci.blob"}

	// The premise. Before any host registers these tasks, the example is a file
	// naming nothing - which is what an author who has not installed the plugin
	// has, and what makes the assertions after registration mean something.
	for _, name := range tasks {
		if _, ok := flowstatev1.LookupTask(name); ok {
			t.Fatalf("%q is already in the default registry before this test registered it, so nothing "+
				"below distinguishes a working seam from a task that was always there", name)
		}
	}

	before, err := flowfile.ValidateSource(source)
	if err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("the validator accepted a step naming a task no registry holds")
	}
	beforeText := pluginreachtest.DiagnosticText(before)
	for _, name := range tasks {
		if !strings.Contains(beforeText, name) {
			t.Errorf("the diagnostics do not name %q, so an author who has not installed this plugin gets "+
				"nothing to search for; diagnostics:\n%s", name, beforeText)
		}
	}

	host := pluginreachtest.OpenHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         10 * time.Second,
		HealthTimeout:       5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              pluginreachtest.Logger(t),
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

	t.Run("every task the plugin advertises is one the example could name", func(t *testing.T) {
		defs := host.TaskDefs()
		if len(defs) != len(tasks) {
			t.Fatalf("the launched plugin offers %d tasks, want %v", len(defs), tasks)
		}
		for _, def := range defs {
			if !strings.HasPrefix(def.Name, "oci.") {
				t.Errorf("task %q is not qualified by the name discovery gave this binary", def.Name)
			}
		}
	})

	t.Run("a password may only be written as a whole secret reference", func(t *testing.T) {
		// The declaration the host enforces before a specification enters
		// durable history. Asserting it here, off the launched plugin's own
		// manifest, is what keeps the claim from being prose in a README.
		for _, def := range host.TaskDefs() {
			if !slices.Contains(def.RequiredSecretInputs, "password") {
				t.Errorf("%s does not require password to be a secret reference, so a registry credential "+
					"could be written into a Flowfile as a literal", def.Name)
			}
		}
	})
}

// TestAnOperatorDenyRuleStopsARegistryRead is the other half of this plugin's
// accepting posture toward the deployment default: it accepts a policy no
// operator wrote, which is only safe because an operator who does write one is
// obeyed - on the real dial path, through a launched process.
func TestAnOperatorDenyRuleStopsARegistryRead(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"oci")
	pluginreachtest.BuildPlugin(t, ociModule, binaryPath)

	host := pluginreachtest.OpenHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              pluginreachtest.Logger(t),

		// The field, not a hand-composed Env entry: the host owns encoding the
		// grant, and this is the launch path a worker actually takes.
		EgressPolicy: []byte(operatorPolicyDenying),
	})

	var resolve *flowstatev1.TaskDef
	for _, def := range host.TaskDefs() {
		if def.Name == "oci.resolve" {
			resolve = &def
			break
		}
	}
	if resolve == nil {
		t.Fatal("the launched plugin offers no oci.resolve, so nothing below tests the policy")
	}

	// An anonymous read of a public image: no credential is involved, so what
	// is under test is the destination and nothing else.
	_, err := resolve.Fn(t.Context(), map[string]*flowstatev1.Value{
		"reference": flowstatev1.NewValue(deniedHost + "/acme/api:1.4.2"),
	}, nil)
	if err == nil {
		t.Fatal("oci.resolve read from a registry the operator's egress policy denies")
	}
	if !strings.Contains(err.Error(), "egress policy denied reaching "+deniedHost) {
		t.Fatalf("oci.resolve failed for some reason other than the operator's policy: %v", err)
	}
}
