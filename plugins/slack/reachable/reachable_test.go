// Package reachable proves that the shipped Slack Flowfile is checked against
// the descriptor produced by a real, separately compiled plugin binary.
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
	slackModule = "github.com/picatz/flowstate/plugins/slack"
	examplePath = "../../../examples/plugins/slack/approval.yaml"
)

func TestTheSlackApprovalFlowReachesTheRealPluginContract(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("Go toolchain unavailable")
	}

	source := pluginreachtest.ReadFile(t, examplePath)
	before, err := flowfile.ValidateSource(source)
	if err != nil {
		t.Fatalf("validating before registration: %v", err)
	}
	if !strings.Contains(diagnosticText(before), "slack.post") {
		t.Fatalf("pre-registration diagnostics do not prove slack.post was unknown: %s", diagnosticText(before))
	}

	dir := t.TempDir()
	binary := filepath.Join(dir, plugin.BinaryPrefix+"slack")
	buildPlugin(t, binary)
	host := openHost(t, plugin.Config{
		SearchPath: []string{dir}, DisableHealthChecks: true,
		HandshakeTimeout: 10 * time.Second, DescribeTimeout: 10 * time.Second,
		CallTimeout: 10 * time.Second, ShutdownGrace: 5 * time.Second,
		// The field, not a hand-composed Env entry: the host owns encoding the
		// grant, and this is the launch path a worker actually takes.
		EgressPolicy: []byte("egress:\n  schemes: [https]\n"),
	})
	if err := host.Register(flowstatev1.DefaultRegistry(), nil); err != nil {
		t.Fatalf("registering plugin: %v", err)
	}

	after, err := flowfile.ValidateSource(source)
	if err != nil {
		t.Fatalf("validating registered example: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("registered Slack example has diagnostics: %s", diagnosticText(after))
	}

	// This assertion comes from the manifest delivered by the real process. It
	// is the author-time half of the host's repeated pre-dispatch enforcement.
	literal := strings.Replace(string(source), "${secret('env:SLACK_BOT_TOKEN')}", "literal-token", 1)
	diags, err := flowfile.ValidateSource([]byte(literal))
	if err != nil {
		t.Fatalf("validating literal credential mutation: %v", err)
	}
	text := diagnosticText(diags)
	if !strings.Contains(text, "whole secret reference") || strings.Contains(text, "literal-token") {
		t.Fatalf("literal-token diagnostics = %q, want redacted whole-secret refusal", text)
	}

	p, ok := host.Lookup("slack")
	if !ok || len(p.Manifest().GetTasks()) != 1 || p.Manifest().GetTasks()[0].GetName() != "post" {
		t.Fatalf("catalog manifest did not expose exactly slack.post: %#v", p)
	}
}

func buildPlugin(t *testing.T, output string) {
	pluginreachtest.BuildPlugin(t, slackModule, output)
}

func openHost(t *testing.T, cfg plugin.Config) *plugin.Host {
	cfg.Logger = pluginreachtest.Logger(t)
	return pluginreachtest.OpenHost(t, cfg)
}

func diagnosticText(diags flowfile.Diagnostics) string {
	return pluginreachtest.DiagnosticText(diags)
}
