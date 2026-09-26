package flowstatev1_test

import (
	"context"
	"fmt"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// ExampleRunWithInputs compiles a Flowfile and runs it with the local driver,
// in this process: no Temporal server and no network. The arguments are checked
// against the workflow's declared inputs, as the server checks a submission.
func ExampleRunWithInputs() {
	workflow, _, err := flowfile.Parse([]byte(`
edition: v2026.3
name: greeter
inputs:
  name:
    type: string
    required: true
steps:
  - id: greeting
    value: ${"hello, " + inputs.name}
`))
	if err != nil {
		fmt.Println("parse:", err)
		return
	}

	outputs, err := flowstatev1.RunWithInputs(context.Background(), workflow, map[string]*flowstatev1.Value{
		"name": flowstatev1.NewValue("world"),
	})
	if err != nil {
		fmt.Println("run:", err)
		return
	}

	// A value: step's result is its one output, read in a Flowfile as
	// ${steps.greeting.value}.
	value := outputs.GetStepValues()["greeting"].GetNamedValues()["value"]
	greeting, err := flowstatev1.LiteralToGo(value.GetLiteral())
	if err != nil {
		fmt.Println("read:", err)
		return
	}
	fmt.Println(greeting)

	// Output:
	// hello, world
}

// ExampleNewContextWithRegistry runs a workflow against a task set of the
// caller's own, without registering anything in the process-wide
// DefaultRegistry. That is how a program provides a task in Go, and how a test
// stubs one, without affecting any other run.
func ExampleNewContextWithRegistry() {
	registry := flowstatev1.NewRegistry()
	err := registry.Register(flowstatev1.TaskDef{
		Name:    "shout",
		Summary: "Upper-cases a message.",
		Fn: func(_ context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
			message := inputs["message"].GetLiteral().GetStringValue()
			return &flowstatev1.Node_Outputs{NamedValues: flowstatev1.NewNamedValues(map[string]any{
				"loud": fmt.Sprintf("%s!", message),
			})}, nil
		},
	})
	if err != nil {
		fmt.Println("register:", err)
		return
	}

	workflow, _, err := flowfile.Parse([]byte(`
edition: v2026.3
name: announce
steps:
  - id: announce
    shout:
      message: deploy finished
`))
	if err != nil {
		fmt.Println("parse:", err)
		return
	}

	ctx := flowstatev1.NewContextWithRegistry(context.Background(), registry)
	outputs, err := flowstatev1.RunWithInputs(ctx, workflow, nil)
	if err != nil {
		fmt.Println("run:", err)
		return
	}
	fmt.Println(outputs.GetStepValues()["announce"].GetNamedValues()["loud"].GetLiteral().GetStringValue())

	_, builtin := flowstatev1.LookupTask("shout")
	fmt.Println("in the default registry:", builtin)

	// Output:
	// deploy finished!
	// in the default registry: false
}

// ExampleEvaluator_EvalString evaluates a CEL expression under the default
// limits, with the names it reads supplied by an activation.
func ExampleEvaluator_EvalString() {
	result, err := flowstatev1.DefaultEvaluator().EvalString(
		context.Background(),
		`items.filter(i, i.size() > 3).map(i, i.upperAscii())`,
		[]string{"strings"},
		map[string]any{"items": []string{"api", "worker", "web", "scheduler"}},
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result.Value())

	// Output:
	// [WORKER SCHEDULER]
}
