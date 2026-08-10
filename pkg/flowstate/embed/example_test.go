package embed_test

import (
	"context"
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/embed"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Example_compileAndRun walks the whole supported path an embedder takes: give
// the program's own Go function a task name, compile a Flowfile from bytes that
// names it, run that workflow in this process, and read a step's result back
// out. No Temporal server, no network, nothing outside this process: this is
// the facade's whole pitch, kept honest by go test compiling and running it.
//
// The task set is passed on [embed.RunOptions] rather than installed globally,
// because a run reads its tasks from its own options and does not need
// [embed.Tasks.Install]. Install is for making a task visible to
// [flowfile.Validate]; running does not require it. See [embed.RunOptions.Tasks].
func Example_compileAndRun() {
	// 1. Register the program's own Go function as a task a Flowfile can call.
	tasks := embed.NewTasks()
	err := tasks.Register(embed.Task{
		Name:    "greet",
		Summary: "greets whoever the step names",
		Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			name := inputs["name"].GetLiteral().GetStringValue()
			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{
				"greeting": "hello, " + name,
			})}, nil
		},
	})
	if err != nil {
		fmt.Println("register:", err)
		return
	}

	// 2. Compile a Flowfile from bytes, the same compile boundary flow validate uses.
	workflow, diags, err := embed.Compile([]byte(`
edition: v2026.2
name: greeter
steps:
  - id: welcome
    greet:
      name: world
`))
	if err != nil {
		fmt.Println("compile:", err, diags)
		return
	}

	// 3. Run it locally, in-process, with the custom task available to it.
	outputs, err := embed.RunLocal(context.Background(), workflow, embed.RunOptions{
		Tasks: tasks,
	})
	if err != nil {
		fmt.Println("run:", err)
		return
	}

	// 4. Read a step's output back out of the result.
	greeting, ok := embed.StepOutputString(outputs, "welcome", "greeting")
	if !ok {
		fmt.Println("no greeting output from step welcome")
		return
	}
	fmt.Println(greeting)

	// Output:
	// hello, world
}
