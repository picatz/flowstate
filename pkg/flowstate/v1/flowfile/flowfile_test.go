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
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url": v1.NewLiteral("https://example.com"),
						},
					},
				},
			},
			{
				Id: "b",
				Kind: &v1.Node_Task{
					Task: &v1.Task{
						Name: "log",
						Inputs: map[string]*v1.Value{
							"message": v1.NewExpr("steps.a.body"),
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
	// edition: v2026.3
	// name: hello
	// steps:
	// - id: a
	//   http:
	//     url: https://example.com
	// - id: b
	//   log:
	//     message: ${steps.a.body}
}

func ExampleUnmarshal() {
	inputYAML := `
edition: v2026.3
name: hello
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${steps.a.body}
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
edition: v2026.3
name: hello
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${steps.a.body}
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

func TestFlowfile_MapWithExprValues(t *testing.T) {
	data := []byte(`
edition: v2026.3
name: http-with-headers
steps:
  - id: web
    http:
      url: https://example.com
      method: GET
      headers:
        A: "1"
        B: ${string(2)}
`)
	wf, err := flowfile.Unmarshal(data)
	require.NoError(t, err)
	// headers should be encoded as a CEL expression map, not a literal
	headers := wf.Steps[0].GetTask().Inputs["headers"]
	require.NotNil(t, headers)
	require.NotNil(t, headers.GetExpr())
}

func TestFlowfile_ListWithExprValues(t *testing.T) {
	data := []byte(`
edition: v2026.3
name: list-exprs
steps:
  - id: s
    log:
      # mixed list: contains an embedded ${...} so parser should encode as CEL expr
      lst:
        - 1
        - ${1 + 1}
        - 3
`)
	wf, err := flowfile.Unmarshal(data)
	require.NoError(t, err)
	v := wf.Steps[0].GetTask().Inputs["lst"]
	require.NotNil(t, v)
	require.NotNil(t, v.GetExpr())

	// all-literal list remains literal
	data2 := []byte(`
edition: v2026.3
name: list-literals
steps:
  - id: s
    log:
      lst:
        - 1
        - 2
        - 3
`)
	wf2, err := flowfile.Unmarshal(data2)
	require.NoError(t, err)
	v2 := wf2.Steps[0].GetTask().Inputs["lst"]
	require.NotNil(t, v2)
	require.NotNil(t, v2.GetLiteral())
	require.NotNil(t, v2.GetLiteral().GetListValue())
}

// FuzzRoundTrip tests the round-trip conversion of a Flowfile YAML-based DSL
// representation to a flowstatev1.Workflow proto representation and back.
//
// The seeds are every shape of the DSL, because the fuzzer explores outward from
// them: with only a two-step workflow to start from it never reaches a loop body or
// a policy, and those are where a round trip is most easily lost.
func FuzzRoundTrip(f *testing.F) {
	for _, seed := range []string{
		// A basic case to start with.
		`edition: v2026.3
name: hello
steps:
- id: a
  http:
    url: https://example.com
- id: b
  log:
    message: ${steps.a.body}
`,
		// Conditions and policy, in both the fenced and bare spellings.
		`edition: v2026.3
name: policy
description: ""
steps:
- id: a
  if: ${gate.result == 'go'}
  timeout: 30s
  retry:
    attempts: 3
    interval: 1s
    backoff: 2
    max_interval: 10s
  continue_on_error: true
  log:
    message: go
- id: b
  if: a.result != ''
  retry: {}
  log:
    message: ""
`,
		// Nested control flow, including a loop inside a branch.
		`edition: v2026.3
name: control
steps:
- id: loop
  for_each:
    items: ${['a', 'b']}
    as: n
    max_parallel: 2
    steps:
    - id: body
      log:
        message: ${n}
- id: fan
  parallel:
  - steps:
    - id: left
      for_each:
        items: [1, 2]
        steps:
        - id: inner
          log:
            message: ${string(item)}
  - steps:
    - id: right
      log:
        message: right
`,
		// Structures, expressions inside them, and the zero values the engine
		// relies on surviving.
		`edition: v2026.3
name: shapes
steps:
- id: a
  http:
    url: https://example.com
    headers:
      A: "1"
      B: ${string(2)}
    outputs: "${ {'status': status_code, 'body': body} }"
- id: b
  vars:
    args:
    - ${steps.a.status}
    - 0
    - false
    - ""
    nested:
      k: v
      n: 0
      list: [1, 2, 3]
  log:
    message: ""
`,
		// A `value:` step in every shape the key admits: a bare expression, a
		// fenced one, a whole structure, and one carrying the step properties
		// that stay legal beside it. The kind writes its expression through the
		// same writer `if:` uses, so a seed here covers the position where the
		// two could come apart.
		`edition: v2026.3
name: values
steps:
- id: over
  value: ${inputs.amount >= 100}
- id: shape
  value: "${ {'regions': ['eu', 'us'], 'count': 2} }"
- id: guarded
  description: a value with the properties that stay legal on this kind
  if: ${steps.over.value}
  vars:
    factor: 2
  value: size(steps.shape.value.regions) * factor
- id: say
  log:
    message: ${string(steps.guarded.value)}
`,
		// A `switch:` in every shape the construct admits: a scalar case, a
		// list case, an empty body, a default, and one nested inside a
		// for_each body dispatching on the loop's own binding — the position
		// where the rewriter has bitten before. The empty `steps: []` is a
		// seed on purpose: it is the one body shape Marshal must write back
		// rather than drop.
		`edition: v2026.3
name: dispatch
steps:
- id: route
  switch:
    value: ${inputs.action}
    cases:
    - case: opened
      steps:
      - id: triage
        log:
          message: triage
    - case: [closed, merged]
      steps:
      - id: archive
        log:
          message: archive
    - case: ignore
      steps: []
    default:
      steps:
      - id: unhandled
        log:
          message: unhandled
- id: process
  for_each:
    items: ${['bucket', 'instance']}
    as: resource
    steps:
    - id: dispatch
      switch:
        value: ${resource}
        cases:
        - case: bucket
          steps:
          - id: check_bucket
            log:
              message: bucket
        - case: [1, 2.5, true]
          steps: []
`,
	} {
		f.Add(seed)
	}

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
