package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	examplev1 "github.com/picatz/flowstate/pkg/flowstate/v1/plugin/examples/flowstate-plugin-example/gen/example/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// ExampleMain is the whole of a plugin's func main: a manifest, and the
// function behind each task. A Flowfile then calls the task as
// `example.greet:`. Main serves until the worker that launched the plugin stops
// it, so this example is compiled but not run.
func ExampleMain() {
	sdk.Main(sdk.Plugin{
		Name:        "example",
		Version:     "0.1.0",
		Description: "Greets people.",
		Tasks: []sdk.Task{{
			Name:    "greet",
			Summary: "Greet someone by name.",
			Input:   &examplev1.GreetInputs{},
			Output:  &examplev1.GreetOutputs{},
			Fn: func(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				var in examplev1.GreetInputs
				if err := sdk.DecodeInputs(inputs, &in); err != nil {
					return nil, err
				}
				return sdk.EncodeOutputs(&examplev1.GreetOutputs{Message: "Hello, " + in.GetName() + "!"})
			},
		}},
	})
}

// ExampleTaskFunc shows the shape every task function has: decode the inputs
// into the message the task declared, do the work, and encode the outputs. It is
// called here the way the engine calls it, with inputs the engine has already
// resolved to values.
func ExampleTaskFunc() {
	var greet sdk.TaskFunc = func(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		var in examplev1.GreetInputs
		if err := sdk.DecodeInputs(inputs, &in); err != nil {
			return nil, err // already classified as InvalidInput
		}
		if in.GetName() == "" {
			return nil, sdk.InvalidInput("name is required")
		}
		message := in.GetGreeting() + ", " + in.GetName() + "!"
		return sdk.EncodeOutputs(&examplev1.GreetOutputs{
			Message: message,
			Length:  int64(len(message)),
		})
	}

	outputs, err := greet(context.Background(), map[string]*flowstatev1.Value{
		"name":     flowstatev1.NewValue("Ada"),
		"greeting": flowstatev1.NewValue("Hello"),
	}, nil)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	// A later step reads these as ${steps.<id>.message} and ${steps.<id>.length}.
	for _, name := range []string{"message", "length"} {
		value, err := flowstatev1.LiteralToGo(outputs.GetNamedValues()[name].GetLiteral())
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("%s: %v\n", name, value)
	}

	// A missing name is the workflow's mistake, so it is not retried.
	_, err = greet(context.Background(), nil, nil)
	fmt.Println("invalid input:", sdk.IsInvalidInput(err))

	// Output:
	// message: Hello, Ada!
	// length: 11
	// invalid input: true
}

// ExampleUnavailable classifies a failure so the engine knows whether another
// attempt could succeed. Unavailable is the one retryable classification, and
// the classification survives wrapping.
func ExampleUnavailable() {
	lookup := func(key string) error {
		// A backend that timed out, and said when to try again.
		return sdk.UnavailableAfter(30*time.Second, "backend did not answer for %q", key)
	}

	err := fmt.Errorf("resolve: %w", lookup("api-key"))
	fmt.Println("retryable:", sdk.IsUnavailable(err))

	err = fmt.Errorf("resolve: %w", sdk.NotFound("no secret %q", "api-key"))
	fmt.Println("retryable:", sdk.IsUnavailable(err), "not found:", sdk.IsNotFound(err))

	// A bare error is treated as permanent, the safe answer.
	err = errors.New("something broke")
	fmt.Println("retryable:", sdk.IsUnavailable(err))

	// Output:
	// retryable: true
	// retryable: false not found: true
	// retryable: false
}
