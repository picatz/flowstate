// Package reachable proves that the shipped Slack Flowfile is checked against
// the descriptor produced by a real, separately compiled plugin binary.
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
	before, err := flowfile.ValidateSourceAt(source, examplePath)
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

	after, err := flowfile.ValidateSourceAt(source, examplePath)
	if err != nil {
		t.Fatalf("validating registered example: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("registered Slack example has diagnostics: %s", diagnosticText(after))
	}

	// This assertion comes from the manifest delivered by the real process. It
	// is the author-time half of the host's repeated pre-dispatch enforcement.
	// The binding is a reference and nothing else: a literal there is refused
	// where it is written, without echoing it.
	literal := strings.Replace(string(source), "${secret('env:SLACK_BOT_TOKEN')}", "literal-token", 1)
	diags, err := flowfile.ValidateSourceAt([]byte(literal), examplePath)
	// The compiler refuses the binding itself, so the refusal arrives as the
	// error and not as a validation diagnostic.
	if err == nil {
		t.Fatalf("a literal binding was accepted: %s", diagnosticText(diags))
	}
	text := err.Error()
	if !strings.Contains(text, "plugins.slack.credentials.bot_token") || !strings.Contains(text, "must be bound to a whole reference") || strings.Contains(text, "literal-token") {
		t.Fatalf("literal binding diagnostics = %q, want redacted whole-reference refusal at the binding", text)
	}

	// The plugin does not declare bot_token federated, so the declaration it
	// delivered binds it to a stored secret and a credential reference is refused
	// at the binding and at a step's own input, naming the declaration and not the
	// target written.
	federated := strings.Replace(string(source), "${secret('env:SLACK_BOT_TOKEN')}", "${credential('leaky-target')}", 1)
	_, err = flowfile.ValidateSourceAt([]byte(federated), examplePath)
	if err == nil {
		t.Fatal("a credential reference was accepted for a credential the plugin does not declare federated")
	}
	if text := err.Error(); !strings.Contains(text, "is not declared federated") || strings.Contains(text, "leaky-target") {
		t.Fatalf("federated binding diagnostics = %q, want the declaration named and the target not echoed", text)
	}

	// A step's own input overrides the binding, and is held to the same claim
	// the host repeats before dispatch: a literal there is refused too.
	override := strings.Replace(string(source), "      channel: ${inputs.channel}\n      idempotency_key: ${inputs.request_message_key}\n",
		"      channel: ${inputs.channel}\n      idempotency_key: ${inputs.request_message_key}\n      token: literal-token\n", 1)
	if override == string(source) {
		t.Fatal("the override mutation did not apply to the example")
	}
	diags, err = flowfile.ValidateSourceAt([]byte(override), examplePath)
	if err != nil {
		t.Fatalf("validating literal override mutation: %v", err)
	}
	text = diagnosticText(diags)
	if !strings.Contains(text, "which is not federated and takes a whole secret reference") || strings.Contains(text, "literal-token") {
		t.Fatalf("literal override diagnostics = %q, want redacted whole-secret refusal", text)
	}

	credentialOverride := strings.Replace(override, "token: literal-token", "token: ${credential('leaky-target')}", 1)
	diags, err = flowfile.ValidateSourceAt([]byte(credentialOverride), examplePath)
	if err != nil {
		t.Fatalf("validating credential override mutation: %v", err)
	}
	text = diagnosticText(diags)
	if !strings.Contains(text, "which is not federated") || strings.Contains(text, "leaky-target") {
		t.Fatalf("credential override diagnostics = %q, want the declaration named and the target not echoed", text)
	}

	p, ok := host.Lookup("slack")
	var tasks []string
	for _, task := range p.Manifest().GetTasks() {
		tasks = append(tasks, task.GetName())
	}
	if !ok || !slices.Equal(tasks, []string{"post", "update", "respond"}) {
		t.Fatalf("catalog manifest exposes tasks %v, want exactly slack.post, slack.update and slack.respond", tasks)
	}

	// A block list written wholly as literals is checked structurally by the host
	// from the plugin's own descriptor: a misspelt member is named at the line
	// that wrote it, before any run. (An input that mixes in an expression is
	// checked by the plugin when the step runs, before any request.)
	const literalBlocks = "edition: v2026.4\nname: literal-blocks\nplugins:\n  slack: v0.2.0\nsteps:\n" +
		"  - id: a\n    slack.post:\n      channel: C0123456789\n" +
		"      idempotency_key: 018f0e6c-7b42-7cc1-8a31-65c0f8758f4a\n" +
		"      blocks:\n        - sectoin: {text: {plain: hi}}\n" +
		"      token: ${secret('env:SLACK_BOT_TOKEN')}\n"
	diags, err = flowfile.ValidateSourceAt([]byte(literalBlocks), examplePath)
	if err != nil {
		t.Fatalf("validating misspelt block member: %v", err)
	}
	if text := diagnosticText(diags); !strings.Contains(text, `no field "sectoin"`) {
		t.Fatalf("misspelt block member diagnostics = %q, want the unknown member named", text)
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
