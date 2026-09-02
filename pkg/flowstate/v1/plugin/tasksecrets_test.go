package plugin

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// This file is the negative-direction half of Gap A: a plugin task consuming a
// host-managed secret. CLAUDE.md's rule is to test that A cannot reach B, not
// that A can reach A, so what matters here is not that a declared input
// resolves — [TestAFlowfileCanNameAPluginTask] already proves that end to end —
// but that everything *not* declared is refused, and that whatever a plugin
// echoes back is contained.

// TestResolvePluginSecretInputsRefusesUndeclaredInput is the deny-by-default
// direction of the manifest's secret_inputs field: an input holding a reference
// that the task's own manifest did not name is refused rather than resolved,
// so a plugin cannot receive a secret a Flowfile did not explicitly route to
// it.
func TestResolvePluginSecretInputsRefusesUndeclaredInput(t *testing.T) {
	t.Parallel()

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), hostSecretRuntime(t, "TOKEN", "s3cr3t"))

	_, _, err := resolvePluginSecretInputs(ctx, "example.task", []string{"other"}, nil, map[string]*flowstatev1.Value{
		"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"message"`)
	assert.Contains(t, err.Error(), "did not declare")

	// Named, so the diagnostic points somewhere rather than only refusing.
	assert.Contains(t, err.Error(), "other")

	var taskErr *flowstatev1.TaskError
	require.ErrorAs(t, err, &taskErr)
	assert.Equal(t, flowstatev1.ErrorKindInvalidInput, taskErr.Kind)
	assert.False(t, taskErr.Retryable(), "an undeclared secret input is a specification mistake, not a transient one")
}

// TestResolvePluginSecretInputsRefusesWithNoDeclaredInputsAtAll checks the
// boundary of the helper above: a task that declares no secret inputs still
// gets the same refusal, worded so an author knows there is nowhere to put
// one.
func TestResolvePluginSecretInputsRefusesWithNoDeclaredInputsAtAll(t *testing.T) {
	t.Parallel()

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), hostSecretRuntime(t, "TOKEN", "s3cr3t"))

	_, _, err := resolvePluginSecretInputs(ctx, "example.task", nil, nil, map[string]*flowstatev1.Value{
		"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "declares no inputs that accept one")
}

// TestResolvePluginSecretInputsRefusesNestedReference checks the other shape a
// reference can arrive in: nested inside a list or a mapping rather than as
// the whole value of an input. No plugin task declares [flowstatev1.TaskDef
// .NestedSecretInputs] today — [Plugin.taskDef] never sets it — so this is
// refused unconditionally, defense in depth for a specification built by hand
// rather than compiled from a Flowfile, where the compiler already refuses
// this shape at `flow validate` time.
func TestResolvePluginSecretInputsRefusesNestedReference(t *testing.T) {
	t.Parallel()

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), hostSecretRuntime(t, "TOKEN", "s3cr3t"))

	nested := flowstatev1.NewStructureMap(map[string]*flowstatev1.Value{
		"Authorization": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	})

	_, _, err := resolvePluginSecretInputs(ctx, "example.task", []string{"headers"}, nil, map[string]*flowstatev1.Value{
		"headers": nested,
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nested inside a list or a mapping")
	assert.Contains(t, err.Error(), "no plugin task input accepts")
}

// TestResolvePluginSecretInputsFailsClosedWithoutRuntime checks the same rule
// [TaskRuntime] enforces for a built-in task's own secret input: no runtime
// installed on the context is a denial, not a pass-through. A process that
// launched a workflow with nothing configured for secrets must not forward a
// reference to a plugin unresolved — the whole reason it is refused rather
// than silently sent as-is.
func TestResolvePluginSecretInputsFailsClosedWithoutRuntime(t *testing.T) {
	t.Parallel()

	_, _, err := resolvePluginSecretInputs(t.Context(), "example.task", []string{"message"}, nil, map[string]*flowstatev1.Value{
		"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not configured")
}

// TestResolvePluginSecretInputsPassesOrdinaryInputsThrough checks that none of
// this touches an input that never held a reference, whole or nested.
func TestResolvePluginSecretInputsPassesOrdinaryInputsThrough(t *testing.T) {
	t.Parallel()

	resolved, _, err := resolvePluginSecretInputs(t.Context(), "example.task", nil, nil, map[string]*flowstatev1.Value{
		"name": flowstatev1.NewLiteral("world"),
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, "world", resolved["name"].GetLiteral().GetStringValue())
}

func TestResolvePluginSecretInputsRefusesLiteralForRequiredInput(t *testing.T) {
	t.Parallel()

	for _, task := range []string{"sql.query", "sql.exec"} {
		t.Run(task, func(t *testing.T) {
			_, _, err := resolvePluginSecretInputs(t.Context(), task, []string{"dsn"}, []string{"dsn"}, map[string]*flowstatev1.Value{
				"dsn": flowstatev1.NewLiteral("postgres://credential@example.invalid/database"),
			}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), `input "dsn" must be a whole secret reference`)
			assert.NotContains(t, err.Error(), "credential@example.invalid")
		})
	}
}

// TestScrubPluginOutputsRefusesBareSecretReference checks the last line of
// defense on the way back: a plugin's response holding a bare
// [flowstatev1.SecretRef] as an output value — which the host never sent it,
// since resolution turns a reference into a value before the request leaves —
// is refused rather than forwarded into a step output, which is durable
// workflow history.
func TestScrubPluginOutputsRefusesBareSecretReference(t *testing.T) {
	t.Parallel()

	scrubber := secrets.NewScrubber()
	outputs := &flowstatev1.Node_Outputs{
		NamedValues: map[string]*flowstatev1.Value{
			"leaked": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
				Scheme: "env", Name: "TOKEN",
			}}},
		},
	}

	err := scrubPluginOutputs(scrubber, outputs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"leaked"`)
	assert.Contains(t, err.Error(), "must never be")
}

// TestScrubPluginOutputsRedactsRegisteredValues checks the ordinary case: a
// resolved secret a plugin reflected back is redacted in place, everywhere in
// a nested output, not merely at the top level.
func TestScrubPluginOutputsRedactsRegisteredValues(t *testing.T) {
	t.Parallel()

	const material = "host-secret-must-not-survive-the-round-trip"

	scrubber := secrets.NewScrubber()
	scrubber.AddValue(material)

	outputs := &flowstatev1.Node_Outputs{
		NamedValues: map[string]*flowstatev1.Value{
			"echo": flowstatev1.NewLiteral("received: " + material),
			"list": flowstatev1.NewLiteralList("a", material, "b"),
			"map":  flowstatev1.NewValue(map[string]any{"nested": material}),
		},
	}

	require.NoError(t, scrubPluginOutputs(scrubber, outputs))

	rendered := outputs.String()
	assert.NotContains(t, rendered, material)
	assert.Contains(t, rendered, secrets.Redacted)
}

// TestScrubPluginOutputsRedactsEveryValueVariant covers the response shapes
// outside Value.literal. These are wire-valid plugin outputs too, and every
// string they hold must be scrubbed before the output reaches workflow
// history.
func TestScrubPluginOutputsRedactsEveryValueVariant(t *testing.T) {
	t.Parallel()

	const material = "host-secret-in-a-non-literal-value"

	scrubber := secrets.NewScrubber()
	scrubber.AddValue(material)

	outputs := &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
		"expression": {
			Kind: &flowstatev1.Value_Expr{Expr: &expr.ParsedExpr{Expr: &expr.Expr{
				ExprKind: &expr.Expr_ConstExpr{ConstExpr: &expr.Constant{
					ConstantKind: &expr.Constant_StringValue{StringValue: material},
				}},
			}}},
		},
		"error": {
			Kind: &flowstatev1.Value_Error_{Error: &flowstatev1.Value_Error{
				Message: "backend reflected " + material,
				Code:    flowstatev1.Value_Error_CODE_INTERNAL,
			}},
		},
		"structure": flowstatev1.NewStructureMap(map[string]*flowstatev1.Value{
			"key-" + material: flowstatev1.NewStructureList(
				flowstatev1.NewLiteral("nested " + material),
			),
		}),
	}}

	require.NoError(t, scrubPluginOutputs(scrubber, outputs))

	rendered := outputs.String()
	assert.NotContains(t, rendered, material)
	assert.Contains(t, rendered, secrets.Redacted)
}

// TestPluginTaskResolvesAndScrubsHostSecret runs the whole path a workflow
// takes: [Plugin.taskDef]'s Fn, over the real RPC transport, to a plugin that
// declared "message" as a secret input and echoes back what it received.
//
// It is the positive and the negative case in one, because both matter here:
// the plugin *did* receive the resolved value (received=true, computed inside
// a process this test does not control) and the value itself never survives
// the round trip back into this run's outputs (echo is redacted).
func TestPluginTaskResolvesAndScrubsHostSecret(t *testing.T) {
	t.Parallel()

	const material = "host-secret-that-must-not-enter-a-step-output"

	host := openHost(t, testConfig(t, pluginDir(t, "secret-task")))

	defs := host.TaskDefs()
	require.Len(t, defs, 1)
	def := defs[0]
	require.Equal(t, "secret-task.task", def.Name)

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), hostSecretRuntime(t, "TOKEN", material))

	outputs, err := def.Fn(ctx, map[string]*flowstatev1.Value{
		"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	}, nil)
	require.NoError(t, err)

	got := outputs.GetNamedValues()
	assert.True(t, got["received"].GetLiteral().GetBoolValue(),
		"the plugin process reports it received no value, so the host never resolved the reference")

	echoed := got["echo"].GetLiteral().GetStringValue()
	assert.NotEqual(t, material, echoed, "the plugin's echo of the secret was not scrubbed")
	assert.Contains(t, echoed, secrets.Redacted)

	rendered := outputs.String()
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		assert.NotContains(t, fmtSprint(verb, outputs), material, "leaked via "+verb)
	}
	assert.NotContains(t, rendered, material)
}

// TestPluginTaskScrubsSecretFromRPCFailure checks the error path the http
// task's own scrubbing exists for, on the plugin transport: a backend's
// failure message reflecting the resolved value back must not surface it in
// the step error, which is shown to users and written to workflow history.
func TestPluginTaskScrubsSecretFromRPCFailure(t *testing.T) {
	t.Parallel()

	const material = "host-secret-that-must-not-enter-a-task-error"

	host := openHost(t, testConfig(t, pluginDir(t, "secret-task-error")))

	defs := host.TaskDefs()
	require.Len(t, defs, 1)
	def := defs[0]

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), hostSecretRuntime(t, "TOKEN", material))

	_, err := def.Fn(ctx, map[string]*flowstatev1.Value{
		"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	}, nil)
	require.Error(t, err)

	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		assert.NotContains(t, fmtSprint(verb, err), material, "leaked via "+verb)
	}
	assert.Contains(t, err.Error(), secrets.Redacted)

	// The same check on a struct holding the error and a slice of those, per
	// CLAUDE.md's containment matrix: a redacting method on a value protects it
	// printed directly and does nothing when it sits inside something else,
	// unless the redaction happened before either was built.
	holder := struct{ Err error }{Err: err}
	slice := []error{err, err}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		assert.NotContains(t, fmtSprint(verb, holder), material, "leaked via "+verb+" on a struct")
		assert.NotContains(t, fmtSprint(verb, slice), material, "leaked via "+verb+" on a slice")
	}
}

func TestPluginTaskScrubsResolvedSecretFromStderr(t *testing.T) {
	t.Parallel()

	const material = `host-secret-"for-plugin-log"`
	var logged capturedLogs
	cfg := testConfig(t, pluginDir(t, "secret-task-log"))
	cfg.Logger = newCapturingLogger(t, &logged)
	host := openHost(t, cfg)

	defs := host.TaskDefs()
	require.Len(t, defs, 1)
	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), hostSecretRuntime(t, "TOKEN", material))
	_, err := defs[0].Fn(ctx, map[string]*flowstatev1.Value{
		"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	}, nil)
	require.NoError(t, err)

	require.True(t, waitFor(t, time.Second, func() bool {
		return strings.Count(logged.String(), "scrubbed=true") >= 4
	}), "the plugin's immediate and post-return log lines were not all relayed")
	host.Close(t.Context())

	output := logged.String()
	assert.NotContains(t, output, material)
	assert.NotContains(t, output, base64.StdEncoding.EncodeToString([]byte(material)))
	assert.GreaterOrEqual(t, strings.Count(output, secrets.Redacted), 4)
}

func TestPluginHealthScrubsBeforeBoundingItsMessage(t *testing.T) {
	t.Parallel()

	const material = "host-secret-crossing-the-health-message-bound"
	host := openHost(t, testConfig(t, pluginDir(t, "secret-task-health")))
	defs := host.TaskDefs()
	require.Len(t, defs, 1)
	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), hostSecretRuntime(t, "TOKEN", material))
	_, err := defs[0].Fn(ctx, map[string]*flowstatev1.Value{
		"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	}, nil)
	require.NoError(t, err)

	p, ok := host.Lookup("secret-task-health")
	require.True(t, ok)
	health := p.CheckHealth(t.Context())
	require.Equal(t, HealthNotServing, health.Status)
	assert.NotContains(t, health.Message, material)
	assert.Contains(t, health.Message, secrets.Redacted)
	assert.True(t, health.messageScrubbed)
}

// TestPluginTaskFn spells out the sorted-name message
// [acceptedPluginSecretInputsHelp] renders, so a change to its wording is
// caught here rather than only by a human reading a diagnostic in a terminal.
func TestAcceptedPluginSecretInputsHelpNamesEveryDeclaredInput(t *testing.T) {
	t.Parallel()

	help := acceptedPluginSecretInputsHelp([]string{"token", "credential"})
	assert.True(t, strings.Contains(help, "credential") && strings.Contains(help, "token"))
}

// TestScrubPluginOutputsRefusesCollidingScrubbedMapKeys covers the one case
// where scrubbing a map cannot be done faithfully: two distinct keys that
// redact to the same text. Writing both back drops one, and which one survives
// follows protobuf's deliberately unspecified map iteration order — so the same
// plugin response could scrub to one output locally and another durably, which
// is the drivers disagreeing about a value.
//
// The refusal is asserted rather than the surviving entry, because there is no
// correct surviving entry to assert.
func TestScrubPluginOutputsRefusesCollidingScrubbedMapKeys(t *testing.T) {
	t.Parallel()

	const material = "host-secret-used-as-a-map-key"

	scrubber := secrets.NewScrubber()
	scrubber.AddValue(material)

	outputs := &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
		"structure": flowstatev1.NewStructureMap(map[string]*flowstatev1.Value{
			material:         flowstatev1.NewLiteral("one"),
			secrets.Redacted: flowstatev1.NewLiteral("two"),
		}),
	}}

	err := scrubPluginOutputs(scrubber, outputs)
	require.Error(t, err, "a map whose keys collide once scrubbed must be refused, not silently reduced")
	assert.Contains(t, err.Error(), "redact to the same key")
	assert.NotContains(t, err.Error(), material, "the refusal must not itself leak the material")
}
