// Package reachable proves that the shipped webhook Flowfile is checked against
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
	webhookModule = "github.com/picatz/flowstate/plugins/webhook"
	examplePath   = "../../../examples/plugins/webhook/sender.yaml"
)

func TestTheSignedDeliveryFlowReachesTheRealPluginContract(t *testing.T) {
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
	if !strings.Contains(diagnosticText(before), "webhook.send") {
		t.Fatalf("pre-registration diagnostics do not prove webhook.send was unknown: %s", diagnosticText(before))
	}

	dir := t.TempDir()
	binary := filepath.Join(dir, plugin.BinaryPrefix+"webhook")
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
		t.Fatalf("registered webhook example has diagnostics: %s", diagnosticText(after))
	}

	// This assertion comes from the manifest delivered by the real process. It
	// is the author-time half of the host's repeated pre-dispatch enforcement.
	literal := strings.Replace(string(source), "${secret('env:PEER_WEBHOOK_KEY')}", "literal-key", 1)
	diags, err := flowfile.ValidateSource([]byte(literal))
	if err != nil {
		t.Fatalf("validating literal credential mutation: %v", err)
	}
	text := diagnosticText(diags)
	if !strings.Contains(text, "whole secret reference") || strings.Contains(text, "literal-key") {
		t.Fatalf("literal-key diagnostics = %q, want redacted whole-secret refusal", text)
	}

	p, ok := host.Lookup("webhook")
	if !ok || len(p.Manifest().GetTasks()) != 1 || p.Manifest().GetTasks()[0].GetName() != "send" {
		t.Fatalf("catalog manifest did not expose exactly webhook.send: %#v", p)
	}
}

func buildPlugin(t *testing.T, output string) {
	pluginreachtest.BuildPlugin(t, webhookModule, output)
}

func openHost(t *testing.T, cfg plugin.Config) *plugin.Host {
	cfg.Logger = pluginreachtest.Logger(t)
	return pluginreachtest.OpenHost(t, cfg)
}

func diagnosticText(diags flowfile.Diagnostics) string {
	return pluginreachtest.DiagnosticText(diags)
}
