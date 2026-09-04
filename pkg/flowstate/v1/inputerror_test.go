package flowstatev1_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestASubmitRefusalIsNotReportedAsAFlowstateDefect is #1552 stated as the
// property the type exists for.
//
// Every refusal at the submit boundary was a bare fmt.Errorf, so ClassifyError
// fell through to ErrorKindInternal — which errors.go defines as "a defect in
// Flowstate itself". An embedder calling RunWithInputs with a wrong-typed
// argument was told Flowstate had broken, and could only have known better by
// matching the sentence.
//
// The negative direction is the whole assertion: Internal is what these must
// never be, so it is asserted by name rather than only InvalidInput being
// asserted positively.
func TestASubmitRefusalIsNotReportedAsAFlowstateDefect(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Name: "onboard",
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "tenant", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
			{Name: "shards", Type: v1.InputDeclaration_TYPE_INT},
		},
	}

	for name, test := range map[string]struct {
		submitted map[string]*v1.Value
		input     string
		declared  string
		got       string
	}{
		"a required input nobody gave": {
			submitted: map[string]*v1.Value{},
			input:     "tenant",
		},
		"a name the workflow does not declare": {
			submitted: map[string]*v1.Value{
				"tenant": v1.NewLiteral("acme"),
				"tenat":  v1.NewLiteral("acme"),
			},
			input: "tenat",
		},
		"a value of the wrong type": {
			submitted: map[string]*v1.Value{
				"tenant": v1.NewLiteral("acme"),
				"shards": v1.NewLiteral("many"),
			},
			input:    "shards",
			declared: "int",
			got:      "string",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := v1.BindRunInputs(workflow, test.submitted)
			require.Error(t, err)

			assert.Equal(t, v1.ErrorKindInvalidInput, v1.ClassifyError(err),
				"a caller's own bad argument is reported as %q", v1.ClassifyError(err))
			assert.NotEqual(t, v1.ErrorKindInternal, v1.ClassifyError(err),
				"a submission the workflow refuses is reported as a defect in Flowstate")

			var inputErr *v1.InputError
			require.ErrorAs(t, err, &inputErr,
				"the refusal carries nothing a caller can read the input's name off")
			assert.Equal(t, test.input, inputErr.Input)
			assert.Equal(t, test.declared, inputErr.Declared)
			assert.Equal(t, test.got, inputErr.Got)

			// Wrapping did not cost the sentence, which is what every existing
			// caller and every existing test reads.
			assert.Equal(t, err.Error(), inputErr.Error())
			assert.Contains(t, err.Error(), test.input)

			// Retrying an identical submission is refused identically.
			assert.False(t, inputErr.Retryable())

			// errors.Is still reaches through to whatever was wrapped.
			assert.True(t, errors.Is(err, inputErr.Unwrap()))
		})
	}
}

// TestAMustViolationIsAlsoTheCallersArgument covers the fourth refusal the
// issue names, which travels a different function ([v1.CheckInputConstraints])
// and would have been missed by a test that only exercised the type check.
func TestAMustViolationIsAlsoTheCallersArgument(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Name:    "onboard",
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{
				Name:     "shards",
				Type:     v1.InputDeclaration_TYPE_INT,
				Required: true,
				Must:     proto.String("this > 0"),
			},
		},
	}

	_, err := v1.BindRunInputs(workflow, map[string]*v1.Value{"shards": v1.NewLiteral(int64(0))})
	require.Error(t, err)

	assert.Equal(t, v1.ErrorKindInvalidInput, v1.ClassifyError(err))

	var inputErr *v1.InputError
	require.ErrorAs(t, err, &inputErr)
	assert.Equal(t, "shards", inputErr.Input)

	// No types were compared, so none are claimed.
	assert.Empty(t, inputErr.Declared)
	assert.Empty(t, inputErr.Got)
}

// TestATaskFailureStillClassifiesItself is the direction the new arm in
// ClassifyError must not take over.
//
// A TaskError carries its own kind and is checked first, so a task that failed
// upstream stays Upstream even when the chain beneath it holds a submit
// refusal — which it does whenever a `call:` binds its callee's arguments.
func TestATaskFailureStillClassifiesItself(t *testing.T) {
	t.Parallel()

	err := &v1.TaskError{
		Task: "http",
		Kind: v1.ErrorKindUpstream,
		Err:  &v1.InputError{Input: "tenant", Err: errors.New("input \"tenant\" is required and was not given")},
	}

	assert.Equal(t, v1.ErrorKindUpstream, v1.ClassifyError(err),
		"the InputError arm took a classification the task had already made")
}
