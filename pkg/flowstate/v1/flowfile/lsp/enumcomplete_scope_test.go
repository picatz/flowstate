package lsp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func enumProbeDef(name string) v1.TaskDef {
	return v1.TaskDef{
		Name:    name,
		Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&decisionv1.Answer{}).ProtoReflect().Descriptor(),
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}
}

// TestEnumValuesAreOfferedOnlyWhereTheCompilerResolvesThem pins that completion
// agrees with the compiler: a name is offered for a task the default registry
// holds, and not for one only a server's own registry knows, which the compiler
// would leave unresolved.
func TestEnumValuesAreOfferedOnlyWhereTheCompilerResolvesThem(t *testing.T) {
	const (
		shared   = "test_enum_scope_shared"
		injected = "test_enum_scope_injected"
	)
	require.NoError(t, v1.DefaultRegistry().Register(enumProbeDef(shared)))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(shared) })

	own := v1.NewRegistry()
	require.NoError(t, own.Register(enumProbeDef(injected)))

	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), Tasks: own})
	c.initialize()

	for task, offered := range map[string]bool{shared: true, injected: false} {
		src, pos := splitCursor(t, `edition: v2026.4
name: c
steps:
  - id: a
    `+task+`:
      message: hi
  - id: b
    log:
      message: ${CALIBRATION_N|}
`)
		uri := "file:///enum-scope-" + task + ".yaml"
		c.open(uri, src)
		got := c.complete(uri, pos.Line, pos.Character)
		if offered {
			assert.Contains(t, labels(got.Items), "CALIBRATION_NONE", task)
		} else {
			assert.NotContains(t, labels(got.Items), "CALIBRATION_NONE", task)
		}
	}
}

// TestEnumValuesYieldToWhatIsBoundHere pins that a name a step var already
// holds is not offered a second time as an enum value.
func TestEnumValuesYieldToWhatIsBoundHere(t *testing.T) {
	const task = "test_enum_scope_taken"
	require.NoError(t, v1.DefaultRegistry().Register(enumProbeDef(task)))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(task) })

	c := newClient(t)
	c.initialize()

	src, pos := splitCursor(t, `edition: v2026.4
name: c
steps:
  - id: a
    `+task+`:
      message: hi
  - id: b
    vars:
      CALIBRATION_NONE: 1
    log:
      message: ${CALIBRATION_|}
`)
	c.open("file:///enum-taken.yaml", src)
	got := c.complete("file:///enum-taken.yaml", pos.Line, pos.Character)

	count := 0
	for _, label := range labels(got.Items) {
		if label == "CALIBRATION_NONE" {
			count++
		}
	}
	assert.Equal(t, 1, count, "one candidate, the step's own var")
	assert.Contains(t, labels(got.Items), "CALIBRATION_SELF_REPORTED")
}
