package flowfile_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func ExampleMarshal() {
	flow := &v1.Workflow{
		Name: "hello",
		Steps: []*v1.Node{
			{
				Id: "a",
				Kind: &v1.Node_Task{
					Task: &v1.Task{
						Name: "echo",
						Inputs: map[string]*v1.Value{
							"message": v1.NewLiteral("hello world"),
						},
					},
				},
			},
			{
				Id: "b",
				Kind: &v1.Node_Task{
					Task: &v1.Task{
						Name: "echo",
						Inputs: map[string]*v1.Value{
							"message": v1.NewExpr("a.result"),
						},
					},
				},
			},
		},
	}

	b, err := flowfile.Marshal(flow)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(b))
	// Output:
	// name: hello
	// steps:
	// - id: a
	//   task:
	//     name: echo
	//     inputs:
	//       message: hello world
	// - id: b
	//   task:
	//     name: echo
	//     inputs:
	//       message: ${a.result}
}

func ExampleUnmarshal() {
	inputYAML := `
name: hello
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: "hello world"
  - id: b
    task:
      name: echo
      inputs:
        message: ${a.result}
`

	flow, err := flowfile.Unmarshal([]byte(inputYAML))
	if err != nil {
		panic(err)
	}

	fmt.Println(flow.Name, len(flow.Steps))
	// Output:
	// hello 2
}

func TestFlowFileRoundTrip(t *testing.T) {
	inputYAML := `
name: hello
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: "hello world"
  - id: b
    task:
      name: echo
      inputs:
        message: ${a.result}
`

	flow, err := flowfile.Unmarshal([]byte(inputYAML))
	require.NoError(t, err)

	flowBytes, err := flowfile.Marshal(flow)
	require.NoError(t, err)

	flow2, err := flowfile.Unmarshal(flowBytes)
	require.NoError(t, err)

	require.True(
		t,
		proto.Equal(flow, flow2),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(flow, flow2, protocmp.Transform()),
	)
}

// FuzzRoundTrip tests the round-trip conversion of a Flowfile YAML-based DSL
// representation to a flowstatev1.Workflow proto representation and back.
func FuzzRoundTrip(f *testing.F) {
	// Add a basic test case to start with.
	f.Add(`name: hello
steps:
- id: a
  task:
    name: echo
    inputs:
      message: "hello world"
- id: b
  task:
    name: echo
    inputs:
      message: ${a.result}
`)

	f.Fuzz(func(t *testing.T, input string) {
		// Unmarshal the input string into a Workflow proto.
		flow, err := flowfile.Unmarshal([]byte(input))
		if err != nil {
			t.Skipf("Skipping invalid input: %v", err)
		}

		// Marshal the Workflow proto back to a Flowfile YAML-based DSL representation.
		data, err := flowfile.Marshal(flow)
		if err != nil {
			t.Fatalf("Failed to marshal workflow: %v", err)
		}

		// Unmarshal the marshaled data back into a Workflow proto.
		flow2, err := flowfile.Unmarshal(data)
		if err != nil {
			t.Fatalf("Failed to unmarshal workflow: %v", err)
		}

		// Check if the original and final Workflow protos are equal.
		if !proto.Equal(flow, flow2) {
			t.Errorf("Round-trip conversion failed:\n%s\n%s",
				cmp.Diff(flow, flow2, protocmp.Transform()),
				cmp.Diff(data, []byte(input)),
			)
		}
	})
}
