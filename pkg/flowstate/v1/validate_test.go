package flowstatev1_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"
)

// failure is an expected violation, identified by the field path and rule ID
// only. The human-readable message protovalidate produces is not asserted on,
// so that rewording it upstream does not break these tests.
type failure struct {
	field string
	rule  string
}

func (f failure) String() string { return f.field + " (" + f.rule + ")" }

// requireViolations asserts that err reports exactly the given violations, in
// any order, since rules are not evaluated in field declaration order.
func requireViolations(t *testing.T, err error, want []failure) {
	t.Helper()

	var invalid *v1.ValidationError
	require.ErrorAsf(t, err, &invalid, "want a *v1.ValidationError, got %[1]T: %[1]v", err)

	got := make([]failure, 0, len(invalid.Violations))
	for _, v := range invalid.Violations {
		got = append(got, failure{field: v.Field, rule: v.Rule})
	}
	require.ElementsMatch(t, want, got, "violations: %v", invalid)
}

// validWorkflow returns the smallest workflow that satisfies every rule the
// schema declares, for use as a valid building block in tests.
func validWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "example",
		Steps: []*v1.Node{{
			Id: "step-1",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
			}},
		}},
	}
}

// TestValidate checks messages against the rules declared as buf.validate
// options in proto/flowstate/v1/flowstate.proto.
func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		msg  proto.Message
		// want lists the violations the message must produce. An empty want
		// means the message must validate cleanly.
		want []failure
	}{
		// Task.HTTP.Inputs.url declares required and string.uri.
		{
			name: "http inputs valid",
			msg:  &v1.Task_HTTP_Inputs{Url: "https://example.com/health"},
		},
		{
			name: "http inputs url not a uri",
			msg:  &v1.Task_HTTP_Inputs{Url: "not a url"},
			want: []failure{{"url", "string.uri"}},
		},
		{
			name: "http inputs url must be absolute",
			msg:  &v1.Task_HTTP_Inputs{Url: "/relative/only"},
			want: []failure{{"url", "string.uri"}},
		},
		{
			name: "http inputs url missing",
			msg:  &v1.Task_HTTP_Inputs{},
			want: []failure{{"url", "required"}},
		},

		// Task.HTTP.Inputs.method declares a case-insensitive pattern of the
		// verbs the task supports, plus a 3 to 6 character length range.
		{
			name: "http inputs method valid",
			msg:  &v1.Task_HTTP_Inputs{Url: "https://example.com", Method: proto.String("POST")},
		},
		{
			name: "http inputs method is case insensitive",
			msg:  &v1.Task_HTTP_Inputs{Url: "https://example.com", Method: proto.String("get")},
		},
		{
			name: "http inputs method unsupported verb",
			msg:  &v1.Task_HTTP_Inputs{Url: "https://example.com", Method: proto.String("BREW")},
			want: []failure{{"method", "string.pattern"}},
		},
		{
			name: "http inputs method too long",
			msg:  &v1.Task_HTTP_Inputs{Url: "https://example.com", Method: proto.String("OPTIONS")},
			want: []failure{
				{"method", "string.max_len"},
				{"method", "string.pattern"},
			},
		},
		{
			name: "http inputs method too short",
			msg:  &v1.Task_HTTP_Inputs{Url: "https://example.com", Method: proto.String("GO")},
			want: []failure{
				{"method", "string.min_len"},
				{"method", "string.pattern"},
			},
		},

		// Task.name declares a pattern, and Task.inputs is required.
		{
			name: "task valid",
			msg: &v1.Task{
				Name:   "http",
				Inputs: map[string]*v1.Value{"url": v1.NewLiteral("https://example.com")},
			},
		},
		{
			name: "task empty",
			msg:  &v1.Task{},
			want: []failure{
				{"name", "required"},
				{"inputs", "required"},
			},
		},
		{
			name: "task name has illegal characters",
			msg: &v1.Task{
				Name:   "bad name!",
				Inputs: map[string]*v1.Value{"a": v1.NewLiteral("b")},
			},
			want: []failure{{"name", "string.pattern"}},
		},
		{
			name: "task name too long",
			msg: &v1.Task{
				Name:   strings.Repeat("a", 129),
				Inputs: map[string]*v1.Value{"a": v1.NewLiteral("b")},
			},
			want: []failure{{"name", "string.max_len"}},
		},
		{
			name: "task inputs empty",
			msg:  &v1.Task{Name: "log"},
			want: []failure{{"inputs", "required"}},
		},

		// Node requires an id and exactly one kind.
		{
			name: "node empty",
			msg:  &v1.Node{},
			want: []failure{
				{"id", "required"},
				{"kind", "required"},
			},
		},

		// Value requires exactly one kind.
		{
			name: "value valid literal",
			msg:  v1.NewLiteral("hello"),
		},
		{
			name: "value empty",
			msg:  &v1.Value{},
			want: []failure{{"kind", "required"}},
		},

		// Workflow requires a name matching a pattern and at least one step.
		{
			name: "workflow valid",
			msg:  validWorkflow(),
		},
		{
			name: "workflow empty",
			msg:  &v1.Workflow{},
			want: []failure{
				{"name", "required"},
				{"steps", "required"},
			},
		},
		{
			name: "workflow description too long",
			msg: func() *v1.Workflow {
				wf := validWorkflow()
				wf.Description = proto.String(strings.Repeat("d", 257))
				return wf
			}(),
			want: []failure{{"description", "string.max_len"}},
		},

		// RunRequest requires a workflow, and rules nest into it.
		{
			name: "run request valid",
			msg:  &v1.RunRequest{Workflow: validWorkflow()},
		},
		{
			name: "run request missing workflow",
			msg:  &v1.RunRequest{},
			want: []failure{{"workflow", "required"}},
		},
		{
			name: "run request nested workflow violations",
			msg:  &v1.RunRequest{Workflow: &v1.Workflow{}},
			want: []failure{
				{"workflow.name", "required"},
				{"workflow.steps", "required"},
			},
		},
		{
			name: "run request nested step violations",
			msg: &v1.RunRequest{Workflow: &v1.Workflow{
				Name:  "example",
				Steps: []*v1.Node{{Id: "step-1", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}},
			}},
			want: []failure{{"workflow.steps[0].task.inputs", "required"}},
		},

		// GetRequest requires a workflow ID, and run_id must be a UUID if set.
		{
			name: "get request valid",
			msg:  &v1.GetRequest{WorkflowId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		},
		{
			name: "get request valid with run id",
			msg: &v1.GetRequest{
				WorkflowId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
				RunId:      proto.String("6ba7b811-9dad-11d1-80b4-00c04fd430c8"),
			},
		},
		{
			name: "get request missing workflow id",
			msg:  &v1.GetRequest{},
			want: []failure{{"workflow_id", "required"}},
		},
		{
			name: "get request run id not a uuid",
			msg: &v1.GetRequest{
				WorkflowId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
				RunId:      proto.String("not-a-uuid"),
			},
			want: []failure{{"run_id", "string.uuid"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := v1.Validate(test.msg)

			if len(test.want) == 0 {
				require.NoErrorf(t, err, "want %v to be valid", test.msg)
				return
			}

			requireViolations(t, err, test.want)
		})
	}
}

// TestValidateHTTPInputsEnforcesURLAndMethod is the regression test for the
// validation outage this package was written to fix: the schema declared
// string.uri on url and a verb pattern on method, but the generated validator
// that used to be called enforced neither, so both of these messages were
// silently accepted and handed to net/http.
func TestValidateHTTPInputsEnforcesURLAndMethod(t *testing.T) {
	for _, inputs := range []*v1.Task_HTTP_Inputs{
		{Url: "definitely not a url"},
		{Url: "https://example.com", Method: proto.String("NOPE")},
	} {
		err := v1.Validate(inputs)
		require.Errorf(t, err, "want %v to be rejected", inputs)

		var invalid *v1.ValidationError
		require.ErrorAs(t, err, &invalid)
		require.Equal(t, "flowstate.v1.Task.HTTP.Inputs", invalid.MessageName)
		require.NotEmpty(t, invalid.Violations)
	}
}

// TestValidateRejectsNilMessage checks that a missing message is reported as
// invalid rather than passing, which is what protovalidate does on its own.
func TestValidateRejectsNilMessage(t *testing.T) {
	tests := []struct {
		name        string
		msg         proto.Message
		messageName string
	}{
		{name: "nil interface", msg: nil},
		{name: "typed nil", msg: (*v1.Workflow)(nil), messageName: "flowstate.v1.Workflow"},
		{name: "typed nil request", msg: (*v1.RunRequest)(nil), messageName: "flowstate.v1.RunRequest"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := v1.Validate(test.msg)

			var invalid *v1.ValidationError
			require.ErrorAs(t, err, &invalid)
			require.Equal(t, test.messageName, invalid.MessageName)
			require.Len(t, invalid.Violations, 1)
			require.Equal(t, "required", invalid.Violations[0].Rule)
		})
	}
}

// TestValidateErrorClassification checks that callers can tell a message that
// failed its rules from a validator that could not reach a verdict, which is
// what lets a server answer with invalid-argument instead of internal.
func TestValidateErrorClassification(t *testing.T) {
	err := v1.Validate(&v1.Task_HTTP_Inputs{Url: "not a url"})
	require.Error(t, err)

	var invalid *v1.ValidationError
	require.ErrorAs(t, err, &invalid)
	require.NotErrorIs(t, err, v1.ErrValidatorUnavailable,
		"a rule violation must not be reported as an unavailable validator")

	// The underlying protovalidate error stays reachable, so violations can be
	// attached to an RPC error as machine-readable detail.
	require.NotNil(t, invalid.Unwrap())

	require.NoError(t, v1.Validate(&v1.Task_HTTP_Inputs{Url: "https://example.com"}))
}

// TestValidationErrorMessage checks that the error text names the offending
// field and the rule it failed, which is what a workflow author needs to fix it.
func TestValidateErrorMessage(t *testing.T) {
	single := v1.Validate(&v1.Task_HTTP_Inputs{Url: "not a url"})
	require.EqualError(t, single,
		"invalid flowstate.v1.Task.HTTP.Inputs: url: must be a valid URI (string.uri)")

	multi := v1.Validate(&v1.Workflow{})
	require.ErrorContains(t, multi, "invalid flowstate.v1.Workflow: 2 rules violated:")
	require.ErrorContains(t, multi, "\n  - name: value is required (required)")
	require.ErrorContains(t, multi, "\n  - steps: value is required (required)")
}

// TestValidateConcurrent checks that the one shared validator is safe to use
// from many goroutines at once, which is how a server will use it. Run under
// the race detector this also covers its lazy construction.
func TestValidateConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			require.NoError(t, v1.Validate(&v1.RunRequest{Workflow: validWorkflow()}))

			err := v1.Validate(&v1.Task_HTTP_Inputs{Url: fmt.Sprintf("bad url %d", i)})
			var invalid *v1.ValidationError
			require.ErrorAs(t, err, &invalid)
		}()
	}
	wg.Wait()
}

// TestViolationString checks how a violation reads on its own, including when
// it carries no field path because the rule applies to a whole message.
func TestValidateViolationString(t *testing.T) {
	tests := []struct {
		name string
		in   v1.Violation
		want string
	}{
		{
			name: "field and rule",
			in:   v1.Violation{Field: "url", Rule: "string.uri", Message: "must be a valid URI"},
			want: "url: must be a valid URI (string.uri)",
		},
		{
			name: "no field",
			in:   v1.Violation{Rule: "required", Message: "no message was provided"},
			want: "no message was provided (required)",
		},
		{
			name: "message only",
			in:   v1.Violation{Message: "something is off"},
			want: "something is off",
		},
		{
			name: "empty",
			in:   v1.Violation{},
			want: "rule not met",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.in.String())
		})
	}
}

// TestValidateSchemaRulesCompile checks that every message in the schema has
// rules protovalidate can actually compile, so a malformed rule surfaces here
// rather than as an unavailable validator in production.
func TestValidateSchemaRulesCompile(t *testing.T) {
	for _, msg := range []proto.Message{
		&v1.Workflow{}, &v1.Workflow_StepOutputs{}, &v1.Node{}, &v1.Node_Outputs{},
		&v1.Value{}, &v1.Value_Error{}, &v1.Task{},
		&v1.Task_Log_Inputs{}, &v1.Task_Log_Outputs{},
		&v1.Task_HTTP_Inputs{}, &v1.Task_HTTP_Outputs{},
		&v1.RunRequest{}, &v1.RunResponse{}, &v1.RunResponse_Error{},
		&v1.GetRequest{}, &v1.GetResponse{}, &v1.RunState{},
	} {
		err := v1.Validate(msg)
		require.NotErrorIsf(t, err, v1.ErrValidatorUnavailable,
			"rules for %T must compile, got: %v", msg, err)

		if err != nil {
			var invalid *v1.ValidationError
			require.ErrorAsf(t, err, &invalid, "%T: unexpected error kind", msg)
		}
	}
}

// TestValidateRefusesEmptyMapKeys is the test for a rule that used to be written
// down and never ran.
//
// The schema said `keys: {required: true}` on every string-keyed map — task
// inputs, labels, step outputs, named values, scope vars. It
// reads like a constraint and it is not one: `required` cannot apply to a map key,
// because a key is never absent. protovalidate ignores it and `buf lint` reports
// it as unenforceable, which is why lint was not in CI.
//
// So the constraint was decorative and the hole was real: a workflow naming a task
// input `""` validated cleanly. That name is what an expression would have to
// reference, and nothing can reference the empty name — so the run was accepted
// and could never be correct.
//
// The rule now says the thing that is true and enforceable, min_len: 1. The same
// edit removed `values:`/`items: {required: true}`, which were unenforceable *and*
// redundant: protovalidate already recurses into a nested message, which is what
// actually refuses a nil value — as the second half of this test pins, so that
// removing them cannot quietly remove the check somebody thought they were.
func TestValidateRefusesEmptyMapKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		workflow *v1.Workflow
	}{
		{
			name: "an empty task input name",
			workflow: &v1.Workflow{
				Name: "empty-input-name",
				Steps: []*v1.Node{{
					Id: "a",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "log",
						Inputs: map[string]*v1.Value{"": v1.NewLiteral("hi")},
					}},
				}},
			},
		},
		{
			name: "an empty label key",
			workflow: &v1.Workflow{
				Name:   "empty-label-key",
				Labels: map[string]string{"": "value"},
				Steps: []*v1.Node{{
					Id:   "a",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
				}},
			},
		},
		// A third case covered `Workflow.inputs`, which is gone: the field was a
		// map nothing wrote and nothing read, and its number and name are reserved.
		// The rule it exercised is the same one the two cases above check, on the
		// two string-keyed maps that a workflow still has.
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, v1.Validate(test.workflow),
				"a map key nothing can reference was accepted")
		})
	}
}

// TestValidateStillRefusesAnAbsentMapValue is the other direction of the same
// edit.
//
// `values: {required: true}` was removed as unenforceable, and the check it looked
// like it was doing has to still happen — by recursion into the value message
// rather than by the rule that was deleted. If this ever stops failing, the
// removal took something real with it.
func TestValidateStillRefusesAnAbsentMapValue(t *testing.T) {
	t.Parallel()

	err := v1.Validate(&v1.Workflow{
		Name: "nil-input-value",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": nil},
			}},
		}},
	})

	require.Error(t, err, "a task input with no value at all was accepted")
	require.Contains(t, err.Error(), "kind",
		"the refusal did not come from recursing into the value, so it may not survive")
}
