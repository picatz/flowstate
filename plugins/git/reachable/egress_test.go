package reachable

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// deniedHost is the destination the operator's policy below refuses. It is
// denied by name, so the refusal happens before any name resolution and the
// test needs no network of its own.
const deniedHost = "git.example.com"

// operatorPolicyDenying is the file an operator writes to stop this plugin
// reaching one host — the shape of policy #1321 says a deployment must be able
// to express, and could not, while this plugin built its own.
const operatorPolicyDenying = "egress:\n  deny:\n    - host == \"" + deniedHost + "\"\n"

// TestAnOperatorDenyRuleStopsAGitTask is #1321's acceptance stated as a test:
// the deployment's own --egress-policy governs a git.* task on the real
// connection path, through a real launched plugin process.
//
// Before this, the plugin built its own safe-default policy and never read the
// grant, so an operator who wrote this deny rule governed the built-in http task
// and nothing else — the deny rule reached no git task at all, which is the
// false promise the flag's help had to carry until now.
//
// The task is invoked through the host's own TaskDef rather than a Flowfile,
// because what is under test is the connection this process makes, not the
// author-facing spelling the neighbouring test already covers.
func TestAnOperatorDenyRuleStopsAGitTask(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"git")
	buildPlugin(t, binaryPath)

	host := openHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		HealthTimeout:       5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              testLogger(t),

		// The field, not a hand-composed Env entry: the host owns encoding the
		// grant, and this is the launch path a worker actually takes.
		EgressPolicy: []byte(operatorPolicyDenying),
	})

	var lsRemote *flowstatev1.TaskDef
	for _, def := range host.TaskDefs() {
		if def.Name == "git.ls_remote" {
			lsRemote = &def
			break
		}
	}
	if lsRemote == nil {
		t.Fatal("the launched plugin offers no git.ls_remote, so nothing below tests the policy")
	}

	_, err := lsRemote.Fn(t.Context(), map[string]*flowstatev1.Value{
		"url": flowstatev1.NewValue("https://" + deniedHost + "/an/org/repo.git"),
	}, nil)
	if err == nil {
		t.Fatal("git.ls_remote reached a host the operator's egress policy denies")
	}
	if !strings.Contains(err.Error(), "denied by egress policy") {
		t.Fatalf("git.ls_remote failed for some reason other than the operator's policy: %v", err)
	}
}
