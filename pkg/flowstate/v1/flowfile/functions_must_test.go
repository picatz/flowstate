package flowfile_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// A declared function in a `must:`, from the file's side: that it is callable in
// every position a `must:` is written, that the stored `must` is plain CEL the
// runtime evaluates while `must_source` keeps the call for `flow fmt`, and that
// what a body may not do in an expression it may not do here either.

const mustFile = `edition: v2026.4
name: musts
types:
  Tag:
    must: isCode(this.alias)
    fields:
      code:
        type: string
        must: isCode(this)
      alias:
        type: string
functions:
  isCode:
    description: Whether text is a three-letter, two-digit code.
    params:
      s: string
    returns: bool
    body: ${s.matches("^[a-z]{3}-[0-9]{2}$")}
inputs:
  id:
    type: string
    required: true
    must: isCode(this)
  tag:
    type: Tag
steps:
  - id: a
    value: ${inputs.id}
outputs:
  result:
    value: ${steps.a.value}
    type: string
    must: isCode(this) && this != "zzz-00"
`

func TestAFunctionIsCallableInEveryMust(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(mustFile))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))
	require.NoError(t, v1.Validate(wf))

	tag := wf.GetDeclaredTypes()[0]
	for name, got := range map[string][2]string{
		"input":        {wf.GetDeclaredInputs()[0].GetMust(), wf.GetDeclaredInputs()[0].GetMustSource()},
		"output":       {wf.GetDeclaredOutputs()[0].GetMust(), wf.GetDeclaredOutputs()[0].GetMustSource()},
		"record field": {tag.GetFields()[0].GetMust(), tag.GetFields()[0].GetMustSource()},
		"record type":  {tag.GetMust(), tag.GetMustSource()},
	} {
		must, source := got[0], got[1]
		assert.NotEmpty(t, source, name)
		assert.NotContains(t, must, "isCode", "%s: a call to the function survived into the rule the runtime evaluates", name)
		assert.Contains(t, must, "cel.bind(s, ", name)
		assert.Contains(t, must, `matches("^[a-z]{3}-[0-9]{2}$")`, name)
		assert.NotEqual(t, source, must, name)
	}
	assert.Equal(t, "isCode(this)", wf.GetDeclaredInputs()[0].GetMustSource())
	assert.Equal(t, "isCode(this.alias)", tag.GetMustSource())
	assert.Equal(t, "isCode(this) && this != \"zzz-00\"", wf.GetDeclaredOutputs()[0].GetMustSource())
	assert.Equal(t, conformance.FunctionMustExpansion, wf.GetDeclaredInputs()[0].GetMust(),
		"the shared driver cases are held to a rule the compiler no longer produces")
	assert.NotNil(t, wf.GetDeclaredInputs()[1].GetValueType())
	assert.Nil(t, wf.GetDeclaredInputs()[1].MustSource, "a declaration with no call keeps no source")
	assert.Nil(t, tag.GetFields()[1].MustSource)
}

func TestAFunctionMustWritesBackAsAuthoredAndIsAFixedPoint(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(mustFile))
	require.NoError(t, err)

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Equal(t, mustFile, string(written), "the calls are written as the author wrote them, not as their expansion")

	formatted, err := flowfile.Format([]byte(mustFile), wf)
	require.NoError(t, err)
	assert.Equal(t, mustFile, string(formatted))

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	assert.True(t, proto.Equal(wf, again), "the written file compiles to a different workflow")
}

// The rule a run evaluates is the expansion alone: a specification that carries a
// source with nothing expanded behind it, as one built without the compiler can,
// is read by `must`.
func TestAMustSourceIsNeverEvaluated(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(mustFile))
	require.NoError(t, err)

	wf.DeclaredInputs[0].MustSource = proto.String("neverDeclared(this)")
	require.Empty(t, flowfile.Validate(wf))
	require.NoError(t, v1.Validate(wf))
}

func TestAFunctionInAMustIsHeldToTheSameRulesAsAnywhere(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		// replace swaps one line of the file for another.
		from, to string
		want     string
	}{
		{
			name: "a body that reads inputs is still refused",
			from: `body: ${s.matches("^[a-z]{3}-[0-9]{2}$")}`,
			to:   `body: ${inputs.id.matches("^[a-z]{3}-[0-9]{2}$")}`,
			want: "a function sees only its parameters",
		},
		{
			name: "recursion is still refused",
			from: `body: ${s.matches("^[a-z]{3}-[0-9]{2}$")}`,
			to:   `body: ${isCode(s)}`,
			want: "is recursive: isCode calls isCode",
		},
		{
			name: "an argument of the wrong type is refused where the call is written",
			from: "    must: isCode(this)\n  tag:",
			to:   "    must: isCode(1)\n  tag:",
			want: "isCode",
		},
		{
			name: "a call with the wrong number of arguments is refused",
			from: "    must: isCode(this)\n  tag:",
			to:   "    must: isCode(this, this)\n  tag:",
			want: "isCode",
		},
		{
			name: "a rule that is not a predicate is still refused",
			from: "    must: isCode(this)\n  tag:",
			to:   "    must: size(this)\n  tag:",
			want: "rather than a bool",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := replaceOnce(t, mustFile, test.from, test.to)
			wf, _, err := flowfile.Parse([]byte(source))
			if err == nil {
				err = flowfile.Validate(wf).Err()
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

// A type-checking failure of the rule is reported at the `must:` it was written in,
// not at the function, and names what was written there.
func TestAFunctionMustIsReportedWhereItIsWritten(t *testing.T) {
	t.Parallel()

	source := replaceOnce(t, mustFile, "    must: isCode(this)\n  tag:", "    must: isCode(1)\n  tag:")
	_, _, err := flowfile.Parse([]byte(source))
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.True(t, errors.As(err, &ds))
	require.NotEmpty(t, ds)
	assert.Equal(t, 23, ds[0].Line, "the diagnostic is on the line of the `must:`")
}

// A `must:` spends the budget an expression does, so a few hundred rules cannot
// copy a large composed function where a few hundred steps could not.
func TestAFunctionMustSharesTheFilesExpansionBudget(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: t\nfunctions:\n  f0:\n    params:\n      k: int\n    returns: int\n    body: ${k + k}\n")
	for i := 1; i <= 11; i++ {
		fmt.Fprintf(&b, "  f%d:\n    params:\n      k: int\n    returns: int\n    body: ${f%d(k) + f%d(k + 1)}\n", i, i-1, i-1)
	}
	b.WriteString("inputs:\n")
	for i := range 300 {
		fmt.Fprintf(&b, "  n%d:\n    type: int\n    must: f11(this) > %d\n", i, i)
	}
	b.WriteString("steps:\n  - id: a\n    value: ${1}\n")

	_, _, err := flowfile.Parse([]byte(b.String()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expand past 100000 CEL nodes in this file altogether")

	var ds flowfile.Diagnostics
	require.True(t, errors.As(err, &ds))
	assert.Len(t, ds, 1, "the limit is reported once, not at every later use")
}

func replaceOnce(t *testing.T, source, from, to string) string {
	t.Helper()

	require.Contains(t, source, from)

	return strings.Replace(source, from, to, 1)
}
