package flowstatev1_test

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func sensitiveInput(name string) *v1.InputDeclaration {
	return &v1.InputDeclaration{Name: name, Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}
}

func callOf(callee *v1.Workflow) *v1.Node {
	return &v1.Node{Id: "sub", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}
}

// TestRunFailureSensitiveValuesCoverCallees: a callee's sensitive input is
// bound from an expression at the call, so its value cannot be enumerated
// from the run's own inputs, and failure text is withheld whole.
func TestRunFailureSensitiveValuesCoverCallees(t *testing.T) {
	t.Parallel()

	const token = "synthetic-token-7a1e"
	inputs := map[string]*v1.Value{"token": v1.NewLiteral(token)}

	root := &v1.Workflow{Name: "root", DeclaredInputs: []*v1.InputDeclaration{sensitiveInput("token")}}
	own := v1.RunFailureSensitiveValues(root, inputs)
	require.False(t, own.WithholdAll(), "the run's own inputs can be enumerated")
	require.NotContains(t, own.RedactText("GET /"+token+" failed", "[withheld]"), token)

	plain := &v1.Workflow{Name: "plain", Steps: []*v1.Node{callOf(&v1.Workflow{Name: "callee"})}}
	require.True(t, v1.RunFailureSensitiveValues(plain, nil).Empty(), "nothing is declared anywhere")

	nested := &v1.Workflow{Name: "caller", Steps: []*v1.Node{callOf(&v1.Workflow{
		Name:           "callee",
		DeclaredInputs: []*v1.InputDeclaration{sensitiveInput("password")},
	})}}
	require.True(t, v1.RunFailureSensitiveValues(nested, nil).WithholdAll(),
		"a callee's sensitive input cannot be enumerated, so failure text must be withheld whole")

	// An output is computed from values that are not themselves declared, so
	// what it carries cannot be enumerated from the run's inputs either.
	output := &v1.Workflow{Name: "root", DeclaredOutputs: []*v1.OutputDeclaration{{Name: "token", Sensitive: true}}}
	require.True(t, v1.RunFailureSensitiveValues(output, nil).WithholdAll(),
		"a sensitive output's source value can reach failure text, so failure text must be withheld whole")
}

// TestACalleesDeclarationsAreSeenFromTheCaller: a caller reads a callee's
// outputs by expression and passes it values the same way, so a declaration
// anywhere below the root is one the root's own names cannot redact by.
func TestACalleesDeclarationsAreSeenFromTheCaller(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		wf   *v1.Workflow
		want bool
	}{
		"nothing embedded": {wf: &v1.Workflow{Name: "root", DeclaredInputs: []*v1.InputDeclaration{sensitiveInput("token")}}},
		"a plain callee":   {wf: &v1.Workflow{Name: "root", Steps: []*v1.Node{callOf(&v1.Workflow{Name: "callee"})}}},
		"a sensitive input below": {wf: &v1.Workflow{Name: "root", Steps: []*v1.Node{callOf(&v1.Workflow{
			Name: "callee", DeclaredInputs: []*v1.InputDeclaration{sensitiveInput("password")},
		})}}, want: true},
		"a sensitive output below": {wf: &v1.Workflow{Name: "root", Steps: []*v1.Node{callOf(&v1.Workflow{
			Name: "callee", DeclaredOutputs: []*v1.OutputDeclaration{{Name: "token", Sensitive: true}},
		})}}, want: true},
	} {
		got, err := v1.CalleeDeclaresSensitiveValues(tc.wf)
		require.NoError(t, err, name)
		require.Equal(t, tc.want, got, name)
	}
}

// TestAValueSetIsCostedByWhatItHolds: a cache of value sets is bounded by
// RetainedBytes, so it must count a sensitive default the run's inputs never
// named as surely as an input the caller passed.
func TestAValueSetIsCostedByWhatItHolds(t *testing.T) {
	t.Parallel()

	large := strings.Repeat("d", 32<<10)
	declared := sensitiveInput("token")
	declared.Default = v1.NewLiteral(large)
	wf := &v1.Workflow{Name: "root", DeclaredInputs: []*v1.InputDeclaration{declared}}

	fromDefault := v1.RunFailureSensitiveValues(wf, nil)
	require.False(t, fromDefault.WithholdAll())
	require.GreaterOrEqual(t, fromDefault.RetainedBytes(), len(large), "a bound default was not counted")

	fromInput := v1.RunFailureSensitiveValues(wf, map[string]*v1.Value{"token": v1.NewLiteral(large)})
	require.GreaterOrEqual(t, fromInput.RetainedBytes(), len(large), "a passed input was not counted")

	require.Zero(t, v1.SensitiveValues{}.RetainedBytes())
}

// TestAWithheldTranscriptDoesNotGrowPastWhatArrived: a marker is longer than
// a short output, so censoring a transcript of many of them would multiply a
// response admitted under its bound. Past the entity-state allowance, the
// steps keep their ids and lose their output names instead.
func TestAWithheldTranscriptDoesNotGrowPastWhatArrived(t *testing.T) {
	t.Parallel()

	few := map[string]*v1.Node_Outputs{"fetch": {NamedValues: map[string]*v1.Value{"body": v1.NewLiteral("")}}}
	kept := v1.RedactStepValues(few, v1.CarriedValuesDeclared)
	require.Contains(t, kept["fetch"].GetNamedValues(), "body", "a small transcript keeps its shape")

	many := map[string]*v1.Node_Outputs{}
	arrived := 0
	for i := range 2000 {
		outputs := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"o": v1.NewLiteral("")}}
		many[fmt.Sprintf("s%d", i)] = outputs
		arrived += proto.Size(outputs)
	}
	withheld := 0
	for _, outputs := range v1.RedactStepValues(many, v1.CarriedValuesDeclared) {
		withheld += proto.Size(outputs)
	}
	require.LessOrEqual(t, withheld, max(arrived, v1.RedactedEntityStateAllowance), "censoring grew the transcript past its bound")
}

// TestAWithheldFailureDoesNotGrowPastWhatArrived: a failure quoting a one-byte
// sensitive value many times would come back several times its bounded size
// once each occurrence became a marker, so past the allowance it is withheld
// whole, and a failure that fits is redacted as before.
func TestAWithheldFailureDoesNotGrowPastWhatArrived(t *testing.T) {
	t.Parallel()

	values := v1.SensitiveInputValues(map[string]*v1.Value{"pin": v1.NewLiteral("x")}, map[string]bool{"pin": true})

	short := "pin x rejected"
	require.Equal(t, values.RedactText(short, "withheld"), values.RedactTextWithin(short, "withheld", 64),
		"a failure within the bound is redacted exactly as RedactText does")
	exact := values.RedactText(short, "withheld")
	require.Equal(t, exact, values.RedactTextWithin(short, "withheld", len(exact)),
		"the measured size disagrees with the built text")

	long := strings.Repeat("x.", 32<<10)
	got := values.RedactTextWithin(long, "withheld", v1.RedactedEntityStateAllowance)
	require.Equal(t, "withheld", got, "redaction grew the failure past its bound")

	resp := &v1.GetResponse{Kind: &v1.GetResponse_Error{Error: &v1.RunResponse_Error{Message: long}}}
	v1.RedactGetResponseFailures(resp, values)
	require.LessOrEqual(t, len(resp.GetError().GetMessage()), max(len(long), v1.RedactedEntityStateAllowance))
}

// TestASensitiveBytesInputIsRedactedFromFailureText: a task writes a bytes
// input into text raw (an HTTP query) or as base64 (protobuf JSON), and
// either spelling in a failure is removed as a string input's would be.
func TestASensitiveBytesInputIsRedactedFromFailureText(t *testing.T) {
	t.Parallel()

	// A bytes value reaches an input as a struct's field: a struct binds with
	// its leaves as they are, where a bare bytes literal does not bind at all
	// and so already withholds failure text whole.
	secret := []byte("synthetic-bytes-token-4f2e")
	declared := &v1.InputDeclaration{Name: "creds", Type: v1.InputDeclaration_TYPE_STRUCT, Sensitive: true}
	wf := &v1.Workflow{Name: "root", DeclaredInputs: []*v1.InputDeclaration{declared}}
	values := v1.RunFailureSensitiveValues(wf, map[string]*v1.Value{"creds": v1.NewLiteralMap(map[string]any{"key": secret})})
	require.False(t, values.WithholdAll(), "the set should be enumerable, or this proves nothing")

	raw := "GET https://api.example/?k=" + string(secret) + " returned 403"
	require.NotContains(t, values.RedactText(raw, "withheld"), string(secret), "the raw spelling was left")
	encoded := "decoding " + base64.StdEncoding.EncodeToString(secret) + " failed"
	require.NotContains(t, values.RedactText(encoded, "withheld"), base64.StdEncoding.EncodeToString(secret),
		"the base64 spelling was left")
}
