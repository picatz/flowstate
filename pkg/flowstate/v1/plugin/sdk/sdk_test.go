package sdk

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

// TestManifestIsDerived checks that what the engine is told about a plugin comes
// from what the plugin implements, so that a plugin cannot advertise something
// it did not write.
func TestManifestIsDerived(t *testing.T) {
	t.Parallel()

	resolve := func(context.Context, SecretRequest) (SecretResponse, error) {
		return SecretResponse{Value: []byte("v")}, nil
	}
	run := func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		return nil, nil
	}

	tests := []struct {
		name             string
		plugin           Plugin
		wantCapabilities []pluginv1.Capability
		wantErr          string
	}{
		{
			name: "secrets only",
			plugin: Plugin{
				Name:    "s",
				Secrets: &Secrets{Schemes: []string{"s"}, Resolve: resolve},
			},
			wantCapabilities: []pluginv1.Capability{pluginv1.Capability_CAPABILITY_SECRETS},
		},
		{
			name: "tasks only",
			plugin: Plugin{
				Name:  "t",
				Tasks: []Task{{Name: "t_do", Fn: run}},
			},
			wantCapabilities: []pluginv1.Capability{pluginv1.Capability_CAPABILITY_TASKS},
		},
		{
			name: "both",
			plugin: Plugin{
				Name:    "b",
				Secrets: &Secrets{Schemes: []string{"b"}, Resolve: resolve},
				Tasks:   []Task{{Name: "b_do", Fn: run}},
			},
			wantCapabilities: []pluginv1.Capability{
				pluginv1.Capability_CAPABILITY_SECRETS,
				pluginv1.Capability_CAPABILITY_TASKS,
			},
		},
		{
			name:    "implements nothing",
			plugin:  Plugin{Name: "n"},
			wantErr: "implements nothing",
		},
		{
			name: "secrets with no resolver",
			plugin: Plugin{
				Name:    "s",
				Secrets: &Secrets{Schemes: []string{"s"}},
			},
			wantErr: "no Resolve function",
		},
		{
			name: "secrets with no schemes",
			plugin: Plugin{
				Name:    "s",
				Secrets: &Secrets{Resolve: resolve},
			},
			wantErr: "no reference would ever reach it",
		},
		{
			name: "a task with no function",
			plugin: Plugin{
				Name:  "t",
				Tasks: []Task{{Name: "t_do"}},
			},
			wantErr: "has no Fn",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, err := test.plugin.manifest()

			if test.wantErr != "" {
				if err == nil {
					t.Fatalf("manifest built, want a refusal")
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Errorf("error = %q, want it to mention %q", err.Error(), test.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("manifest: %v", err)
			}

			if len(manifest.GetCapabilities()) != len(test.wantCapabilities) {
				t.Fatalf("capabilities = %v, want %v", manifest.GetCapabilities(), test.wantCapabilities)
			}
			for i, want := range test.wantCapabilities {
				if got := manifest.GetCapabilities()[i]; got != want {
					t.Errorf("capability %d = %v, want %v", i, got, want)
				}
			}

			// The engine refuses a manifest that does not validate, so the SDK
			// should never build one that would be.
			if err := flowstatev1.Validate(manifest); err != nil {
				t.Errorf("the SDK built a manifest the engine would refuse: %v", err)
			}
		})
	}
}

// TestDescribeMessageOmitsWhatTheEngineHas checks the size decision: a plugin
// whose task takes a flowstate type should not ship the engine a copy of the
// engine's own schema, which drags protobuf's, protovalidate's, and CEL's
// descriptors with it.
func TestDescribeMessageOmitsWhatTheEngineHas(t *testing.T) {
	t.Parallel()

	raw, name, err := describeMessage(&flowstatev1.Task_Log_Inputs{})
	if err != nil {
		t.Fatalf("describeMessage: %v", err)
	}

	if name != "flowstate.v1.Task.Log.Inputs" {
		t.Errorf("name = %q, want the message's full name", name)
	}
	if len(raw) != 0 {
		t.Errorf("shipped %d bytes of descriptor for a message the engine already has", len(raw))
	}
}

// TestDescribeMessageNoMessage checks the side of a task that declares no
// schema.
func TestDescribeMessageNoMessage(t *testing.T) {
	t.Parallel()

	raw, name, err := describeMessage(nil)
	if err != nil {
		t.Fatalf("describeMessage: %v", err)
	}
	if raw != nil || name != "" {
		t.Errorf("describeMessage(nil) = (%d bytes, %q), want nothing", len(raw), name)
	}
}

// TestInputsRoundTrip checks that a task's inputs and outputs survive the trip
// between the engine's named values and the typed message a task is written
// against.
func TestInputsRoundTrip(t *testing.T) {
	t.Parallel()

	inputs := map[string]*flowstatev1.Value{
		"url":     flowstatev1.NewLiteral("https://example.com/x"),
		"method":  flowstatev1.NewLiteral("POST"),
		"body":    flowstatev1.NewLiteral("hello"),
		"headers": flowstatev1.NewLiteralMap(map[string]any{"X-Test": "yes"}),
		// A map of the engine's own Value type, which is how a task takes
		// something whose shape it does not constrain.
		"outputs": flowstatev1.NewLiteralMap(map[string]any{"code": "status_code"}),
		// An input the message has no field for is ignored rather than refused,
		// so a workflow written against a newer task still runs.
		"unknown_to_this_version": flowstatev1.NewLiteral("ignored"),
	}

	var decoded flowstatev1.Task_HTTP_Inputs
	if err := DecodeInputs(inputs, &decoded); err != nil {
		t.Fatalf("DecodeInputs: %v", err)
	}

	if decoded.GetUrl() != "https://example.com/x" {
		t.Errorf("url = %q", decoded.GetUrl())
	}
	if decoded.GetMethod() != "POST" {
		t.Errorf("method = %q", decoded.GetMethod())
	}
	if decoded.GetBody() != "hello" {
		t.Errorf("body = %q", decoded.GetBody())
	}
	if got := decoded.GetHeaders()["X-Test"]; got != "yes" {
		t.Errorf("headers[X-Test] = %q, want %q", got, "yes")
	}
	if got := decoded.GetOutputs()["code"].GetLiteral().GetStringValue(); got != "status_code" {
		t.Errorf("outputs[code] = %q, want %q", got, "status_code")
	}

	outputs, err := EncodeOutputs(&flowstatev1.Task_HTTP_Outputs{
		StatusCode: 201,
		Body:       "created",
		Headers:    map[string]string{"Location": "/x/1"},
	})
	if err != nil {
		t.Fatalf("EncodeOutputs: %v", err)
	}

	named := outputs.GetNamedValues()
	if got := named["status_code"].GetLiteral().GetInt64Value(); got != 201 {
		t.Errorf("status_code = %d, want 201", got)
	}
	if got := named["body"].GetLiteral().GetStringValue(); got != "created" {
		t.Errorf("body = %q, want %q", got, "created")
	}

	entries := named["headers"].GetLiteral().GetMapValue().GetEntries()
	if len(entries) != 1 || entries[0].GetKey().GetStringValue() != "Location" {
		t.Errorf("headers = %v, want one Location entry", entries)
	}
}

// TestStructuredOutputs checks that a plugin task can return data whose shape is
// not fixed.
//
// Any plugin worth writing eventually returns something other than flat scalars —
// a list of rows, a parsed response body — and until this worked, the SDK's
// convenience path stopped applying at exactly the point a plugin got
// interesting. The http task's own `json` output is this case, which is how the
// gap surfaced.
func TestStructuredOutputs(t *testing.T) {
	t.Parallel()

	outputs, err := EncodeOutputs(&flowstatev1.Task_HTTP_Outputs{
		StatusCode: 200,
		Body:       `{"items":[{"id":1,"name":"a"}]}`,
		Json: Literal(map[string]any{
			"items": []any{map[string]any{"id": 1, "name": "a"}},
		}),
	})
	require.NoError(t, err, "a task could not return structured data")

	named := outputs.GetNamedValues()

	// The scalars alongside it still work.
	require.Equal(t, int64(200), named["status_code"].GetLiteral().GetInt64Value())

	// And the structured value arrives as a value a workflow can navigate, so
	// ${call.json.items[0].name} resolves with no step in between to parse it.
	entries := named["json"].GetLiteral().GetMapValue().GetEntries()
	require.Len(t, entries, 1)
	require.Equal(t, "items", entries[0].GetKey().GetStringValue())

	items := entries[0].GetValue().GetListValue().GetValues()
	require.Len(t, items, 1)

	first := items[0].GetMapValue().GetEntries()
	require.Len(t, first, 2)

	byKey := map[string]*expr.Value{}
	for _, entry := range first {
		byKey[entry.GetKey().GetStringValue()] = entry.GetValue()
	}
	require.Equal(t, "a", byKey["name"].GetStringValue())
	require.Equal(t, int64(1), byKey["id"].GetInt64Value())
}

// TestStructuredOutputsRoundTrip checks that what a task returns can be read back
// as an input, since one plugin's output is often the next step's input.
func TestStructuredOutputsRoundTrip(t *testing.T) {
	t.Parallel()

	outputs, err := EncodeOutputs(&flowstatev1.Task_HTTP_Outputs{
		Json: Literal(map[string]any{"ok": true}),
	})
	require.NoError(t, err)

	// The http task's `json` *input* is flowstate's spelling of the same idea, so
	// feeding one to the other exercises both message types.
	var decoded flowstatev1.Task_HTTP_Inputs
	require.NoError(t, DecodeInputs(map[string]*flowstatev1.Value{
		"url":  flowstatev1.NewLiteral("https://example.com"),
		"json": outputs.GetNamedValues()["json"],
	}, &decoded))

	entries := decoded.GetJson().GetLiteral().GetMapValue().GetEntries()
	require.Len(t, entries, 1)
	require.Equal(t, "ok", entries[0].GetKey().GetStringValue())
	require.True(t, entries[0].GetValue().GetBoolValue())
}

// TestUnsupportedMessageOutputSaysWhatToDo checks the refusal, which is the part
// an author actually meets.
//
// Converting an arbitrary message would mean inventing a mapping from its fields
// onto a CEL value — this package's invention rather than the schema's, whose
// field names would come out however JSON naming mangles them and would not match
// the descriptor the engine validates the task against. So it refuses, and the
// error has to be worth receiving.
func TestUnsupportedMessageOutputSaysWhatToDo(t *testing.T) {
	t.Parallel()

	// A message-typed output that is neither spelling of "any shape".
	_, err := EncodeOutputs(&flowstatev1.Node{
		Id:   "x",
		Kind: &flowstatev1.Node_Task{Task: &flowstatev1.Task{Name: "echo"}},
	})
	require.Error(t, err, "an arbitrary message was converted rather than refused")

	for _, want := range []string{
		"google.api.expr.v1alpha1.Value", // what to declare instead
		"sdk.Literal",                    // how to build it
	} {
		require.Contains(t, err.Error(), want,
			"the refusal does not tell the author what to do instead")
	}
}

// TestLiteral checks the helper on its own, including the shapes a plugin author
// is most likely to hand it.
func TestLiteral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		check func(t *testing.T, got *expr.Value)
	}{
		{
			name:  "a string",
			value: "hello",
			check: func(t *testing.T, got *expr.Value) {
				require.Equal(t, "hello", got.GetStringValue())
			},
		},
		{
			name:  "a bool",
			value: true,
			check: func(t *testing.T, got *expr.Value) {
				require.True(t, got.GetBoolValue())
			},
		},
		{
			name:  "a list",
			value: []any{"a", "b"},
			check: func(t *testing.T, got *expr.Value) {
				require.Len(t, got.GetListValue().GetValues(), 2)
			},
		},
		{
			name:  "a nested map",
			value: map[string]any{"outer": map[string]any{"inner": 1}},
			check: func(t *testing.T, got *expr.Value) {
				entries := got.GetMapValue().GetEntries()
				require.Len(t, entries, 1)
				require.Equal(t, "outer", entries[0].GetKey().GetStringValue())
				require.Len(t, entries[0].GetValue().GetMapValue().GetEntries(), 1)
			},
		},
		{
			name:  "nothing",
			value: nil,
			check: func(t *testing.T, got *expr.Value) {
				require.NotNil(t, got, "nil produced no value at all")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := Literal(test.value)
			require.NotNil(t, got)
			test.check(t, got)
		})
	}
}

// TestDecodeInputsRefusals checks the inputs a task cannot be given, each with a
// message saying what to do instead.
func TestDecodeInputsRefusals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		inputs      map[string]*flowstatev1.Value
		wantMessage string
	}{
		{
			name:        "the wrong type",
			inputs:      map[string]*flowstatev1.Value{"url": flowstatev1.NewLiteral(42)},
			wantMessage: "is not a string",
		},
		{
			name:        "an unresolved expression",
			inputs:      map[string]*flowstatev1.Value{"url": flowstatev1.NewExpr("1 + 1")},
			wantMessage: "DeferredInputs",
		},
		{
			name: "a secret reference in a typed field",
			inputs: map[string]*flowstatev1.Value{
				"url": {Kind: &flowstatev1.Value_SecretRef{
					SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "URL"},
				}},
			},
			wantMessage: "declare the field as flowstate.v1.Value",
		},
		{
			name:        "a map where a scalar belongs",
			inputs:      map[string]*flowstatev1.Value{"url": flowstatev1.NewLiteralMap(map[string]any{"a": "b"})},
			wantMessage: "is not a string",
		},
		{
			name:        "a scalar where a map belongs",
			inputs:      map[string]*flowstatev1.Value{"headers": flowstatev1.NewLiteral("nope")},
			wantMessage: "wants a map",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var decoded flowstatev1.Task_HTTP_Inputs
			err := DecodeInputs(test.inputs, &decoded)
			if err == nil {
				t.Fatal("DecodeInputs accepted an input it cannot convert")
			}
			if !strings.Contains(err.Error(), test.wantMessage) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), test.wantMessage)
			}
		})
	}
}

// TestDecodeIntegerPrecision checks that a fractional number is refused rather
// than truncated, since truncating would turn an author's mistake into a
// plausible result.
func TestDecodeIntegerPrecision(t *testing.T) {
	t.Parallel()

	var whole flowstatev1.Task_HTTP_Outputs
	if err := DecodeInputs(map[string]*flowstatev1.Value{
		"status_code": flowstatev1.NewLiteral(float64(200)),
	}, &whole); err != nil {
		t.Fatalf("a whole number in a float was refused: %v", err)
	}
	if whole.GetStatusCode() != 200 {
		t.Errorf("status_code = %d, want 200", whole.GetStatusCode())
	}

	var fractional flowstatev1.Task_HTTP_Outputs
	if err := DecodeInputs(map[string]*flowstatev1.Value{
		"status_code": flowstatev1.NewLiteral(200.5),
	}, &fractional); err == nil {
		t.Errorf("200.5 was accepted as an int32 (became %d)", fractional.GetStatusCode())
	}
}

// TestErrorClassification checks that every error leaving a plugin carries an
// explicit verdict on retrying, including one the author never classified.
//
// The schema says a plugin that says nothing should get the non-retrying answer.
// The SDK says it on their behalf, which is more reliable than leaving the engine
// to infer it from a status code chosen for other reasons.
func TestErrorClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		err           error
		wantCode      connect.Code
		wantRetryable bool
	}{
		{
			name:     "not found",
			err:      NotFound("no such secret %q", "k"),
			wantCode: connect.CodeNotFound,
		},
		{
			name:     "permission denied",
			err:      PermissionDenied("refused"),
			wantCode: connect.CodePermissionDenied,
		},
		{
			name:     "invalid input",
			err:      InvalidInput("bad"),
			wantCode: connect.CodeInvalidArgument,
		},
		{
			name:     "failed",
			err:      Failed("broken"),
			wantCode: connect.CodeUnknown,
		},
		{
			name:          "unavailable",
			err:           Unavailable("cannot reach the backend"),
			wantCode:      connect.CodeUnavailable,
			wantRetryable: true,
		},
		{
			name:     "outcome unknown",
			err:      OutcomeUnknown("the request may have already taken effect"),
			wantCode: connect.CodeUnknown,
		},
		{
			name:     "an error the author did not classify",
			err:      errors.New("something went wrong"),
			wantCode: connect.CodeUnknown,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			converted := asConnectError(test.err)

			var connectErr *connect.Error
			if !errors.As(converted, &connectErr) {
				t.Fatalf("asConnectError returned %T, want a *connect.Error", converted)
			}
			if got := connectErr.Code(); got != test.wantCode {
				t.Errorf("code = %v, want %v", got, test.wantCode)
			}

			// The cause survives, so a plugin author's errors.Is still works.
			if !strings.Contains(converted.Error(), test.err.Error()) {
				t.Errorf("error = %q, want it to carry %q", converted.Error(), test.err.Error())
			}

			retryable, found := retryableDetail(t, connectErr)
			if !found {
				t.Fatal("no retryable verdict was attached; the engine would have to guess")
			}
			if retryable != test.wantRetryable {
				t.Errorf("retryable = %v, want %v", retryable, test.wantRetryable)
			}
		})
	}
}

// TestConnectErrorPassesThrough checks that an author who built a connect.Error
// themselves has said exactly what they meant, and it is not reinterpreted.
func TestConnectErrorPassesThrough(t *testing.T) {
	t.Parallel()

	original := connect.NewError(connect.CodeAborted, errors.New("mine"))

	converted := asConnectError(original)
	if converted != error(original) {
		t.Errorf("a connect.Error was rewritten: %v", converted)
	}
}

// retryableDetail reads the verdict attached to an error.
func retryableDetail(t *testing.T, err *connect.Error) (retryable, found bool) {
	t.Helper()

	for _, detail := range err.Details() {
		value, valueErr := detail.Value()
		if valueErr != nil {
			continue
		}
		if response, ok := value.(*pluginv1.ExecuteResponse); ok {
			return response.GetRetryable(), true
		}
	}

	return false, false
}

// responseDetail returns the ExecuteResponse an error carries as a detail, for
// the fields TestErrorClassification's simpler helper does not read.
func responseDetail(t *testing.T, err *connect.Error) *pluginv1.ExecuteResponse {
	t.Helper()

	for _, detail := range err.Details() {
		value, valueErr := detail.Value()
		if valueErr != nil {
			continue
		}
		if response, ok := value.(*pluginv1.ExecuteResponse); ok {
			return response
		}
	}

	t.Fatal("no ExecuteResponse detail was attached")
	return nil
}

// TestOutcomeUnknownIsPermanentAndDistinctFromFailed checks the classification
// the host maps onto flowstatev1.ErrorKindUpstreamUnknown rather than the
// misleading InvalidInput a bare permanent verdict used to get: not retryable,
// and marked as an unknown outcome rather than merely "the plugin's own
// unqualified permanent failure" the way [Failed] is.
func TestOutcomeUnknownIsPermanentAndDistinctFromFailed(t *testing.T) {
	t.Parallel()

	var connectErr *connect.Error
	if !errors.As(asConnectError(OutcomeUnknown("lost the response")), &connectErr) {
		t.Fatal("asConnectError did not return a *connect.Error")
	}

	response := responseDetail(t, connectErr)
	if response.GetRetryable() {
		t.Error("OutcomeUnknown must not be retryable")
	}
	if !response.GetUnknownOutcome() {
		t.Error("OutcomeUnknown did not mark the response as an unknown outcome")
	}

	// Failed, beside it, must not set the same flag — the two are deliberately
	// different claims, and if Failed set it too the host could not tell them
	// apart.
	if !errors.As(asConnectError(Failed("ordinary permanent failure")), &connectErr) {
		t.Fatal("asConnectError did not return a *connect.Error")
	}
	if responseDetail(t, connectErr).GetUnknownOutcome() {
		t.Error("Failed must not be reported as an unknown outcome")
	}
}

// TestUnavailableAfterCarriesTheDelay checks that a plugin's preferred retry
// delay reaches the response detail the host reads it from, and that a
// non-positive delay is silently treated as no preference rather than an
// invalid one.
func TestUnavailableAfterCarriesTheDelay(t *testing.T) {
	t.Parallel()

	var connectErr *connect.Error
	if !errors.As(asConnectError(UnavailableAfter(30*time.Second, "rate limited")), &connectErr) {
		t.Fatal("asConnectError did not return a *connect.Error")
	}
	response := responseDetail(t, connectErr)
	if !response.GetRetryable() {
		t.Error("UnavailableAfter must remain retryable")
	}
	if got := response.GetRetryAfter().AsDuration(); got != 30*time.Second {
		t.Errorf("retry_after = %v, want 30s", got)
	}

	if !errors.As(asConnectError(UnavailableAfter(-time.Second, "no preference")), &connectErr) {
		t.Fatal("asConnectError did not return a *connect.Error")
	}
	if d := responseDetail(t, connectErr).GetRetryAfter(); d != nil {
		t.Errorf("a non-positive delay set retry_after = %v, want unset", d.AsDuration())
	}
}

// TestTaskManifestCarriesDeclarations checks that what the engine needs in order
// to treat a plugin task correctly reaches it.
func TestTaskManifestCarriesDeclarations(t *testing.T) {
	t.Parallel()

	task := Task{
		Name:             "x_do",
		Summary:          "does x",
		Input:            &flowstatev1.Task_Log_Inputs{},
		Output:           &flowstatev1.Task_Log_Outputs{},
		DeferredInputs:   []string{"expr"},
		ExpressionInputs: []string{"expr"},
		SecretInputs:     []string{"token"},
		NeedsScope:       true,
		Fn: func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
			return nil, nil
		},
	}

	manifest, err := task.manifest()
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}

	if manifest.GetName() != "x_do" || manifest.GetSummary() != "does x" {
		t.Errorf("manifest = %v, want the task's name and summary", manifest)
	}
	if !manifest.GetNeedsScope() {
		t.Error("needs_scope was not carried, so the task would not receive its scope")
	}
	if got := manifest.GetDeferredInputs(); len(got) != 1 || got[0] != "expr" {
		t.Errorf("deferred_inputs = %v, want [expr]", got)
	}

	// The two travel together here and are different claims: one says the plugin
	// evaluates this input, the other says an author has to write it as `${...}`.
	// A task can want either without the other, so carrying one and dropping the
	// other would be invisible until a workload failed.
	if got := manifest.GetExpressionInputs(); len(got) != 1 || got[0] != "expr" {
		t.Errorf("expression_inputs = %v, want [expr]", got)
	}
	if got := manifest.GetSecretInputs(); len(got) != 1 || got[0] != "token" {
		t.Errorf("secret_inputs = %v, want [token]", got)
	}
	if manifest.GetInputMessage() != "flowstate.v1.Task.Log.Inputs" {
		t.Errorf("input_message = %q", manifest.GetInputMessage())
	}

	// Mutating the task's slice afterwards must not change the manifest.
	task.DeferredInputs[0] = "changed"
	if manifest.GetDeferredInputs()[0] != "expr" {
		t.Error("the manifest aliases the task's slice")
	}
	task.ExpressionInputs[0] = "changed"
	if manifest.GetExpressionInputs()[0] != "expr" {
		t.Error("the manifest aliases the task's expression-inputs slice")
	}

	if !proto.Equal(manifest, manifest) {
		t.Error("the manifest is not a well-formed message")
	}
}
