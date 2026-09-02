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

// deniedHost is GitHub's own API host, which is where an unauthenticated read
// goes when no base_url is configured. Denying it by name is what makes this a
// test of the operator's policy rather than of the network: the refusal happens
// before any name resolution, so nothing here reaches the internet.
const deniedHost = "api.github.com"

// operatorPolicyDenying is the file an operator writes to stop this plugin
// reaching one API host — the shape of policy #1323 says a deployment must be
// able to express, and could not, while this plugin built its own.
const operatorPolicyDenying = "egress:\n  deny:\n    - host == \"" + deniedHost + "\"\n"

// TestAnOperatorDenyRuleStopsAGitHubTask is #1323's acceptance stated as a test:
// the deployment's own --egress-policy governs a github.* call on its real
// transport path, through a real launched plugin process.
//
// Before this, the plugin built its own safe-default policy and never read the
// grant — so a GitHub Enterprise operator could not authorize their private API
// network through the shared surface, and a deny rule they wrote reached no
// github.* task at all.
//
// The task is invoked through the host's own TaskDef rather than a Flowfile,
// because what is under test is the request this process makes, not the
// author-facing spelling the neighbouring test already covers.
func TestAnOperatorDenyRuleStopsAGitHubTask(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"github")
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

	var pullRequestGet *flowstatev1.TaskDef
	for _, def := range host.TaskDefs() {
		if def.Name == "github.pull_request_get" {
			pullRequestGet = &def
			break
		}
	}
	if pullRequestGet == nil {
		t.Fatal("the launched plugin offers no github.pull_request_get, so nothing below tests the policy")
	}

	// Unauthenticated, exactly as the shipped example reads a public pull
	// request: no credential is involved in the destination decision, which is
	// the separation both this issue and the docs insist on.
	_, err := pullRequestGet.Fn(t.Context(), map[string]*flowstatev1.Value{
		"owner":  flowstatev1.NewValue("golang"),
		"repo":   flowstatev1.NewValue("go"),
		"number": flowstatev1.NewValue(int64(1)),
	}, nil)
	if err == nil {
		t.Fatal("github.pull_request_get reached an API host the operator's egress policy denies")
	}
	if !strings.Contains(err.Error(), "denied by egress policy") {
		t.Fatalf("github.pull_request_get failed for some reason other than the operator's policy: %v", err)
	}
}
