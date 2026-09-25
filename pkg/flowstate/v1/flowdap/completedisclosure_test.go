package flowdap_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

func TestFunctionBreakpointResponseKeepsOneRedactionSnapshot(t *testing.T) {
	const sensitive = "sensitive-declared-step"
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls int

	session, err := flowdebug.New(flowdebug.Options{
		Steps: []flowdebug.Step{{ID: sensitive}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	session.SetRedactor(func(text string) string {
		calls++
		if calls == 1 {
			close(entered)
			<-release
		}
		return strings.ReplaceAll(text, sensitive, "[redacted]")
	})

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "setFunctionBreakpoints", map[string]any{
		"breakpoints": []map[string]any{{"name": "unknown-one"}, {"name": "unknown-two"}},
	})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first breakpoint notice never reached the redactor")
	}
	session.SetRedactor(nil)
	close(release)

	response := c.await("response", "setFunctionBreakpoints")
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), sensitive)
	require.Equal(t, 2, strings.Count(string(encoded), "[redacted]"),
		"one response switched redaction posture between breakpoint entries")
}

func TestFunctionBreakpointRefusalKeepsTheRequestRedactor(t *testing.T) {
	const sensitive = "sensitive-breakpoint"
	entered := make(chan struct{})
	release := make(chan struct{})

	session, err := flowdebug.New(flowdebug.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	first := true
	session.SetRedactor(func(text string) string {
		if first {
			first = false
			close(entered)
			<-release
		}
		return strings.ReplaceAll(text, sensitive, "[redacted]")
	})

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "setFunctionBreakpoints", map[string]any{
		"breakpoints": []map[string]any{{"name": "valid"}, {"name": sensitive + " bad"}},
	})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("breakpoint validation never reached the request redactor")
	}
	session.SetRedactor(nil)
	close(release)

	response := c.await("response", "setFunctionBreakpoints")
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), sensitive)
	require.Equal(t, 2, strings.Count(string(encoded), "[redacted]"),
		"whole-set validation switched redactors after per-entry checks")
}

func TestVariablesResponseKeepsOnePauseSnapshot(t *testing.T) {
	const sensitive = "sensitive-value-between-pauses"
	entered := make(chan struct{})
	release := make(chan struct{})

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	blocked := false
	session.SetRedactor(func(text string) string {
		if !blocked && strings.Contains(text, sensitive) {
			blocked = true
			close(entered)
			<-release
		}
		return strings.ReplaceAll(text, sensitive, "[redacted]")
	})

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	runDone := make(chan error, 1)
	go func() {
		<-server.Launched()
		scope := &v1.Scope{
			Profile: v1.CurrentProfile,
			Inputs: map[string]*v1.Value{
				"first":  v1.NewLiteral(sensitive),
				"second": v1.NewLiteral(sensitive),
			},
		}
		for _, id := range []string{"first", "second"} {
			if stepErr := session.BeforeStep(t.Context(), &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewLiteral("done")}}, scope); stepErr != nil {
				runDone <- stepErr
				return
			}
		}
		runDone <- nil
	}()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": "workflow.yaml"})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	c.send(4, "scopes", map[string]any{"frameId": 1})
	var inputsReference float64
	for _, item := range c.await("response", "scopes")["body"].(map[string]any)["scopes"].([]any) {
		group := item.(map[string]any)
		if group["name"] == "inputs" {
			inputsReference = group["variablesReference"].(float64)
		}
	}
	require.NotZero(t, inputsReference)

	c.send(5, "variables", map[string]any{"variablesReference": inputsReference})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first variable never reached the redactor")
	}
	// Changing the live posture cannot alter the first pause's snapshot. Move
	// to a second pause under that new posture before the first response has
	// finished rendering; every row must still describe the first pause.
	session.SetRedactor(nil)
	moved := make(chan error, 1)
	go func() {
		_, moveErr := session.Step(t.Context())
		moved <- moveErr
	}()
	select {
	case moveErr := <-moved:
		require.NoError(t, moveErr)
	case <-time.After(2 * time.Second):
		t.Fatal("the run did not reach its second pause")
	}
	close(release)

	response := c.await("response", "variables")
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), sensitive)
	require.Equal(t, 2, strings.Count(string(encoded), "[redacted]"),
		"one variables response mixed values from two pause redaction postures")
	// The copied reference remains in the adapter because this test moved the
	// session directly, bypassing the adapter's ordinary new-stop cleanup. Its
	// generation must still keep a later request from reading the second pause.
	c.send(6, "variables", map[string]any{"variablesReference": inputsReference})
	stale := c.await("response", "variables")
	require.Empty(t, stale["body"].(map[string]any)["variables"],
		"a reference minted for the first pause read values from the second")
	finishMove := make(chan error, 1)
	go func() {
		_, moveErr := session.Continue(t.Context())
		finishMove <- moveErr
	}()
	require.NoError(t, <-runDone)
	require.NoError(t, session.Close())
	require.ErrorIs(t, <-finishMove, flowdebug.ErrRunOver)
}

func TestAdapterPreservesSessionRedactionForEvaluateAndVariables(t *testing.T) {
	const (
		sensitive           = "s3cr3t_value_nothing_may_print"
		expressionSensitive = "inputs.token"
	)

	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		Steps:      []flowdebug.Step{{ID: sensitive, Workflow: sensitive, Via: sensitive}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	session.SetRedactor(func(text string) string {
		text = strings.ReplaceAll(text, sensitive, "[redacted]")
		return strings.ReplaceAll(text, expressionSensitive, "[redacted]")
	})
	session.SetValueRedactor(func(value any) any {
		if value == sensitive || value == expressionSensitive {
			return "[redacted]"
		}
		return value
	})

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	go func() {
		<-server.Launched()
		scope := &v1.Scope{
			Profile: v1.CurrentProfile,
			Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}},
			Inputs: map[string]*v1.Value{
				"public":  v1.NewLiteral(sensitive),
				"token":   v1.NewLiteral(expressionSensitive),
				sensitive: v1.NewLiteral(sensitive),
			},
		}
		_ = session.BeforeStep(t.Context(), &v1.Node{
			Id: sensitive,
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
				Name: sensitive,
			}}},
		}, scope)
	}()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "setFunctionBreakpoints", map[string]any{
		"breakpoints": []map[string]any{{"name": "unrelated"}},
	})
	breakpoints := c.await("response", "setFunctionBreakpoints")
	require.Contains(t, breakpoints["body"].(map[string]any)["breakpoints"].([]any)[0].(map[string]any)["message"], "[redacted]")
	c.send(3, "launch", map[string]any{"program": "workflow.yaml"})
	c.await("response", "launch")
	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")
	stopped := c.await("event", "stopped")
	require.Contains(t, stopped["body"].(map[string]any)["description"], "[redacted]")
	pausedAt, pausedIndex, _, paused := session.PausedStepPosition()
	require.True(t, paused)
	require.NotContains(t, pausedAt.Step+pausedAt.Workflow+pausedAt.Kind, sensitive)
	require.Zero(t, pausedIndex, "redacting the displayed position changed which inventory row it resolves to")
	waitedAt, err := session.WaitForPause(t.Context())
	require.NoError(t, err)
	require.NotContains(t, waitedAt.Step+waitedAt.Workflow+waitedAt.Kind, sensitive)
	position, paused := session.PositionProto()
	require.True(t, paused)
	positionJSON, err := protojson.Marshal(position)
	require.NoError(t, err)
	require.Contains(t, string(positionJSON), "[redacted]")
	require.NotContains(t, string(positionJSON), sensitive)
	windowJSON, err := protojson.Marshal(session.StepWindowProto(0, 10))
	require.NoError(t, err)
	require.Contains(t, string(windowJSON), "[redacted]")
	require.NotContains(t, string(windowJSON), sensitive)
	for _, limit := range []int{0, 10} {
		wireScope, scopeErr := session.ScopeProto(t.Context(), limit)
		require.NoError(t, scopeErr)
		wireScopeJSON, marshalErr := protojson.Marshal(wireScope)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(wireScopeJSON), sensitive)
		require.NotContains(t, string(wireScopeJSON), expressionSensitive)
		if limit > 0 {
			require.Contains(t, string(wireScopeJSON), "[redacted]")
		}
	}

	c.send(5, "stackTrace", map[string]any{"threadId": 1})
	stack := c.await("response", "stackTrace")
	require.Equal(t, true, stack["success"])
	stackJSON, err := json.Marshal(stack)
	require.NoError(t, err)
	require.Contains(t, string(stackJSON), "[redacted]")

	c.send(6, "evaluate", map[string]any{"expression": "inputs.public", "frameId": 1})
	evaluated := c.await("response", "evaluate")
	require.Equal(t, true, evaluated["success"])
	require.Contains(t, evaluated["body"].(map[string]any)["result"], "[redacted]")

	c.send(7, "scopes", map[string]any{"frameId": 1})
	scopes := c.await("response", "scopes")
	var inputsReference float64
	for _, item := range scopes["body"].(map[string]any)["scopes"].([]any) {
		group := item.(map[string]any)
		if group["name"] == "inputs" {
			inputsReference = group["variablesReference"].(float64)
		}
	}
	require.NotZero(t, inputsReference)

	c.send(8, "variables", map[string]any{"variablesReference": inputsReference})
	variables := c.await("response", "variables")
	require.Equal(t, true, variables["success"])
	rows := variables["body"].(map[string]any)["variables"].([]any)
	require.Len(t, rows, 1)
	public := rows[0].(map[string]any)
	require.Equal(t, "public", public["name"])
	require.Contains(t, public["value"], "[redacted]")
	require.NotContains(t, public["value"], sensitive)

	encoded, err := json.Marshal([]any{breakpoints, stopped, stack, evaluated, scopes, variables})
	require.NoError(t, err)
	require.Contains(t, string(encoded), "[redacted]")
	require.NotContains(t, string(encoded), sensitive)
	require.NotContains(t, string(encoded), expressionSensitive)
}

func TestAdapterRedactsAStackLabelAfterJoiningItsFields(t *testing.T) {
	const sensitive = `build (call "callee")`

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	session.SetRedactor(func(text string) string {
		return strings.ReplaceAll(text, sensitive, "[redacted]")
	})

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()
	go func() {
		<-server.Launched()
		_ = session.BeforeStep(t.Context(), &v1.Node{
			Id: "build",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
				Name: "callee",
			}}},
		}, v1.NewScope(v1.CurrentProfile, nil))
	}()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": "workflow.yaml"})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	c.send(4, "stackTrace", map[string]any{"threadId": 1})
	stack := c.await("response", "stackTrace")
	encoded, err := json.Marshal(stack)
	require.NoError(t, err)
	require.Contains(t, string(encoded), "[redacted]")
	require.NotContains(t, string(encoded), sensitive)
}
