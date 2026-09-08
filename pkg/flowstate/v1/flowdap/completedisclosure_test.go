package flowdap_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

func TestAdapterPreservesSessionRedactionForEvaluateAndVariables(t *testing.T) {
	const sensitive = "s3cr3t_value_nothing_may_print"

	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		Steps:      []flowdebug.Step{{ID: sensitive}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	session.SetRedactor(func(text string) string {
		return strings.ReplaceAll(text, sensitive, "[redacted]")
	})
	session.SetValueRedactor(func(value any) any {
		if value == sensitive {
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
				"token":   v1.NewLiteral(sensitive),
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

	c.send(5, "stackTrace", map[string]any{"threadId": 1})
	stack := c.await("response", "stackTrace")
	require.Equal(t, true, stack["success"])
	stackJSON, err := json.Marshal(stack)
	require.NoError(t, err)
	require.Contains(t, string(stackJSON), "[redacted]")

	c.send(6, "evaluate", map[string]any{"expression": "inputs.token", "frameId": 1})
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
	token := rows[0].(map[string]any)
	require.Equal(t, "token", token["name"])
	require.Contains(t, token["value"], "[redacted]")
	require.NotContains(t, token["value"], sensitive)

	encoded, err := json.Marshal([]any{breakpoints, stopped, stack, evaluated, variables})
	require.NoError(t, err)
	require.Contains(t, string(encoded), "[redacted]")
	require.NotContains(t, string(encoded), sensitive)
}
