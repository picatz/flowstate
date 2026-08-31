// Package reachable proves that the shipped Slack Flowfile is checked against
// the descriptor produced by a real, separately compiled plugin binary.
package reachable

import (
	"context"
	"encoding/base64"
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

const (
	slackModule = "github.com/picatz/flowstate/plugins/slack"
	examplePath = "../../../examples/plugins/slack/approval.yaml"
	policyEnv   = "FLOWSTATE_SLACK_EGRESS_POLICY_B64"
)

func TestTheSlackApprovalFlowReachesTheRealPluginContract(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("Go toolchain unavailable")
	}

	source, err := os.ReadFile(examplePath)
	if err != nil {
		t.Fatalf("reading example: %v", err)
	}
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
		Env: []string{policyEnv + "=" + base64.StdEncoding.EncodeToString([]byte("egress:\n  schemes: [https]\n"))},
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
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	args := append([]string{"build"}, covbuild.BuildArgs()...)
	args = append(args, "-o", output, slackModule)
	cmd := exec.CommandContext(ctx, "go", args...)
	if wd, err := os.Getwd(); err == nil {
		cmd.Dir = wd
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building Slack plugin: %v: %s", err, out)
	}
}

func openHost(t *testing.T, cfg plugin.Config) *plugin.Host {
	t.Helper()
	cfg.Env = append(cfg.Env, covbuild.Env()...)
	cfg.Logger = slog.New(slog.NewTextHandler(testWriter{t}, nil))
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
	for _, diag := range diags {
		b.WriteString(diag.Message)
		b.WriteByte('\n')
	}
	return b.String()
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	defer func() { _ = recover() }()
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
