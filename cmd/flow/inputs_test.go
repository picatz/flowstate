package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a run is started with, from a command line.
//
// The tests here are about coercion and about refusals, and they are two different
// things on purpose. Coercion is this CLI's own job — a shell hands over words, and
// only the declaration knows that `replicas=3` is a number — so it is tested against
// the declared type rather than against what the characters look like. Refusals are
// [v1.BindRunInputs]'s job, and what is tested about them is that the binder's own
// text reaches the person who typed the command, rather than that this file restates
// the rule.

// takesInputs is a workflow declaring one of every shape an argument has: required,
// defaulted, and each type the coercion has to tell apart.
const takesInputs = `edition: v2026.3
name: takes-inputs

inputs:
  service:
    type: string
    required: true
    description: which service to deploy
  region:
    type: string
    default: eu-west-1
  replicas:
    type: int
    default: 2
  ratio:
    type: float
    default: 0.5
  dry_run:
    type: bool
    default: false
  targets:
    type: list
    default: [alpha]

outputs:
  where:
    value: ${inputs.service + ' in ' + inputs.region}
  replicas:
    value: ${inputs.replicas}
  first_target:
    value: ${inputs.targets[0]}

steps:
  - id: plan
    log:
      message: ${'planning ' + inputs.service}
`

// TestRunLocalTakesEveryDeclaredType is the whole flag surface at once, because the
// coercion table is exactly the thing that is easy to get right for one type and
// wrong for the next.
//
// Read back through the run's declared outputs rather than through the log line: an
// output is a value the run computed from the argument, so `2` arriving as the
// string "2" fails here, where a message would have rendered both the same.
func TestRunLocalTakesEveryDeclaredType(t *testing.T) {
	stdout, stderr, err := runLocal(t, takesInputs, "-o", "json",
		"--input", "service=checkout",
		"--input", "replicas=3",
		"--input", "ratio=0.25",
		"--input", "dry_run=true",
		"--input", `targets=["beta","gamma"]`)
	require.NoError(t, err, stderr)

	outputs := runOutputsOf(t, stdout)

	assert.Equal(t, "checkout in eu-west-1", outputs["where"],
		"the supplied string and the declaration's default did not both reach the run")
	assert.Equal(t, json.Number("3"), outputs["replicas"],
		"an int input arrived as something other than a number")
	assert.Equal(t, "beta", outputs["first_target"],
		"a list input written as JSON did not arrive as a list")
}

// TestARunWithNoArgumentsGetsTheDeclaredDefaults is the other half, and the one an
// example depends on: a workflow whose inputs all have defaults runs as written.
func TestARunWithNoArgumentsGetsTheDeclaredDefaults(t *testing.T) {
	stdout, stderr, err := runLocal(t, takesInputs, "-o", "json", "--input", "service=checkout")
	require.NoError(t, err, stderr)

	outputs := runOutputsOf(t, stdout)

	assert.Equal(t, "checkout in eu-west-1", outputs["where"])
	assert.Equal(t, json.Number("2"), outputs["replicas"],
		"the declaration's default did not reach the run")
	assert.Equal(t, "alpha", outputs["first_target"])
}

// TestRunLocalReportsTheBindersOwnRefusal is the direction that decides whether this
// surface is usable: the message an author reads is the binder's, naming the input
// and what to do about it, rather than a Go error about a map.
//
// Each case also asserts that *nothing ran*. A refusal that arrives after the first
// `log:` step has narrated two lines reads as a run that got somewhere first, and a
// step with a side effect would have had it.
func TestRunLocalReportsTheBindersOwnRefusal(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags []string
		says  []string
	}{
		{
			name:  "a required input left out",
			flags: nil,
			says:  []string{`input "service" is required`, "which service to deploy"},
		},
		{
			name:  "a name the workflow does not declare",
			flags: []string{"--input", "service=x", "--input", "regoin=eu-west-1"},
			says:  []string{`"regoin" is not declared`, "service, region, replicas"},
		},
		{
			name:  "a value of the wrong type",
			flags: []string{"--input", "service=x", "--input", "replicas=many"},
			says:  []string{"replicas", "declared int", "whole number"},
		},
		{
			name:  "a bool that is not one",
			flags: []string{"--input", "service=x", "--input", "dry_run=maybe"},
			says:  []string{"dry_run", "declared bool", "true or false"},
		},
		{
			name:  "a list that is not JSON",
			flags: []string{"--input", "service=x", "--input", "targets=alpha,beta"},
			says:  []string{"targets", "declared list", "JSON"},
		},
		{
			name:  "a flag with no value at all",
			flags: []string{"--input", "service"},
			says:  []string{"needs a name and a value", "name=value"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stdout, stderr, err := runLocal(t, takesInputs, test.flags...)
			require.Error(t, err, "the run was accepted with arguments it should refuse")

			said := err.Error() + stderr
			for _, want := range test.says {
				assert.Contains(t, said, want,
					"the refusal does not say what is wrong or what to do instead")
			}

			assert.Empty(t, strings.TrimSpace(stdout),
				"a refused run wrote an answer, so a reader cannot tell it did not happen")
			assert.NotContains(t, stderr, "planning",
				"a step ran before the arguments were refused")
		})
	}
}

// TestAnExpressionIsNotAnArgument is the security posture of the flag, and it is the
// binder's rule rather than this file's: an argument is data a caller sends, and an
// expression is something a reviewed file says. A CLI that quietly compiled one for
// the caller would put the decision back where it must not be.
func TestAnExpressionIsNotAnArgument(t *testing.T) {
	stdout, stderr, err := runLocal(t, takesInputs, "--input", "service=${1 + 1}")
	require.NoError(t, err, stderr)

	// Accepted, and accepted *as text*: the characters travel as a string, which is
	// what the declaration says the input is. Nothing evaluated them.
	assert.Contains(t, stderr, "planning ${1 + 1}",
		"the fence was compiled rather than carried as the value it was given as")
	assert.NotEmpty(t, stdout)
}

// TestAnInputFileCarriesTypesTheFlagsCannot is why the file exists: a struct and a
// list are documents, and a shell word is not.
func TestAnInputFileCarriesTypesTheFlagsCannot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inputs.json")
	require.NoError(t, os.WriteFile(path, []byte(
		`{"service": "checkout", "replicas": 4, "ratio": 0.75, "targets": ["beta", "gamma"]}`), 0o600))

	stdout, stderr, err := runLocal(t, takesInputs, "-o", "json", "--input-file", path)
	require.NoError(t, err, stderr)

	outputs := runOutputsOf(t, stdout)

	assert.Equal(t, "checkout in eu-west-1", outputs["where"])

	// The one a naive decoder gets wrong. encoding/json reads every number as a
	// float64, so `4` would arrive as 4.0 and be refused against an `int`
	// declaration for a difference the document does not contain.
	assert.Equal(t, json.Number("4"), outputs["replicas"],
		"a whole number in the file did not arrive as an int")
	assert.Equal(t, "beta", outputs["first_target"])
}

// TestAFlagWinsOverTheInputFile pins the precedence, which is the one thing about
// having two sources that a reader has to be told.
func TestAFlagWinsOverTheInputFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inputs.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"service": "from-file", "replicas": 4}`), 0o600))

	stdout, stderr, err := runLocal(t, takesInputs, "-o", "json",
		"--input-file", path, "--input", "service=from-flag")
	require.NoError(t, err, stderr)

	outputs := runOutputsOf(t, stdout)

	assert.Equal(t, "from-flag in eu-west-1", outputs["where"],
		"the file overrode the flag, which is the wrong way round")
	assert.Equal(t, json.Number("4"), outputs["replicas"],
		"the flag replaced the whole file rather than the one value it named")
}

// TestAnInputFileThatIsNotAnObjectIsRefused, because the failure is otherwise a
// decode error about a Go type, which says nothing about what to write instead.
func TestAnInputFileThatIsNotAnObjectIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inputs.json")
	require.NoError(t, os.WriteFile(path, []byte(`["service"]`), 0o600))

	_, stderr, err := runLocal(t, takesInputs, "--input-file", path)
	require.Error(t, err)
	assert.Contains(t, err.Error()+stderr, "object of arguments keyed by input name")
}

// TestAMissingInputFileNamesTheFlag: the path came from the command line, so the
// refusal is about the command line.
func TestAMissingInputFileNamesTheFlag(t *testing.T) {
	_, stderr, err := runLocal(t, takesInputs, "--input-file", filepath.Join(t.TempDir(), "nope.json"))
	require.Error(t, err)
	assert.Contains(t, err.Error()+stderr, "--input-file")
}

// TestDeclaredOutputsAreReportedToAPerson is the other end of the contract. The
// values are in the document on stdout either way; what this asserts is that
// somebody at a terminal is told what the run answered without having to read JSON.
//
// On stderr, which is where this CLI puts its account of a run — so
// `flow run local x | jq` still receives exactly one document.
func TestDeclaredOutputsAreReportedToAPerson(t *testing.T) {
	stdout, stderr, err := runLocal(t, takesInputs, "--input", "service=checkout")
	require.NoError(t, err, stderr)

	assert.Contains(t, stderr, "outputs", "a run with declared outputs did not name them")
	assert.Contains(t, stderr, "where", "an output was not named")
	assert.Contains(t, stderr, "checkout in eu-west-1", "an output's value was not reported")

	assert.NotContains(t, stdout, "outputs\n",
		"the human section was written to stdout, where the answer document lives")
	require.NotEmpty(t, stdout)
	assert.True(t, json.Valid([]byte(stdout)), "stdout stopped being one JSON document: %s", stdout)
}

// TestAWorkflowWithNoDeclaredOutputsSaysNothing: an "outputs" heading over nothing
// would read as a run that failed to produce what it promised.
func TestAWorkflowWithNoDeclaredOutputsSaysNothing(t *testing.T) {
	_, stderr, err := runLocal(t, `edition: v2026.3
name: plain
steps:
  - id: greet
    log:
      message: hello
`)
	require.NoError(t, err, stderr)
	assert.NotContains(t, stderr, "outputs")
}

// TestBothDriversReportDeclaredOutputsInOneField is invariant 3 at the one place a
// caller can see it: the field a run's answer arrives in.
//
// The durable driver's answer is assembled by the server, which copies the
// completion payload's outputs into `GetResponse.run_outputs`; the local driver's is
// assembled by [localRun]. Two assemblers, one document — and before this was
// asserted the local one left the top-level field unset, so `.runOutputs` answered
// null from a rehearsal and the values from production.
func TestBothDriversReportDeclaredOutputsInOneField(t *testing.T) {
	stdout, stderr, err := runLocal(t, takesInputs, "-o", "json", "--input", "service=checkout")
	require.NoError(t, err, stderr)

	var document struct {
		RunOutputs map[string]json.RawMessage `json:"runOutputs"`
		Outputs    struct {
			RunOutputs map[string]json.RawMessage `json:"runOutputs"`
		} `json:"outputs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &document))

	require.NotEmpty(t, document.RunOutputs,
		"a local run left `runOutputs` empty, where a durable run reports the values")
	assert.Equal(t, len(document.Outputs.RunOutputs), len(document.RunOutputs),
		"the two places the answer appears disagree about how many values there are")
}

// TestCoercionReadsTheDeclarationRatherThanTheCharacters is the rule stated as a
// table, and the negative direction is the point of it: the same word means
// different things under different declarations, and guessing from the characters
// would make an argument's type depend on what it happens to look like.
func TestCoercionReadsTheDeclarationRatherThanTheCharacters(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		declared v1.InputDeclaration_Type
		raw      string
		want     string
	}{
		{"true under a string", v1.InputDeclaration_TYPE_STRING, "true", "stringValue"},
		{"true under a bool", v1.InputDeclaration_TYPE_BOOL, "true", "boolValue"},
		{"3 under a string", v1.InputDeclaration_TYPE_STRING, "3", "stringValue"},
		{"3 under an int", v1.InputDeclaration_TYPE_INT, "3", "int64Value"},
		{"3 under a float", v1.InputDeclaration_TYPE_FLOAT, "3", "doubleValue"},
		{"a JSON list under a list", v1.InputDeclaration_TYPE_LIST, `["a"]`, "listValue"},
		{"a JSON object under a struct", v1.InputDeclaration_TYPE_STRUCT, `{"a":1}`, "mapValue"},
		// An undeclared name has no type to read, so the characters travel as they
		// were given and the binder refuses the *name*, which is the better message.
		{"an undeclared name", v1.InputDeclaration_TYPE_UNSPECIFIED, "3", "stringValue"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			declaration := &v1.InputDeclaration{Name: "x", Type: test.declared}
			if test.declared == v1.InputDeclaration_TYPE_UNSPECIFIED {
				declaration = nil
			}

			value, err := coerceInput("x", test.raw, declaration)
			require.NoError(t, err)

			encoded, err := marshalJSON(value.GetLiteral(), false)
			require.NoError(t, err)
			assert.Contains(t, string(encoded), test.want,
				"%q under a %s declaration became %s", test.raw,
				v1.DeclaredTypeName(test.declared), encoded)
		})
	}
}

// TestANestedNumberIsReadAsWritten covers the part of a struct input no declaration
// describes: a field inside it has no type of its own, so it is read the way the
// YAML parser reads a literal — whole numbers are ints, and the rest are floats.
func TestANestedNumberIsReadAsWritten(t *testing.T) {
	t.Parallel()

	value, err := coerceInput("x", `{"count": 2, "ratio": 0.5, "deep": {"n": 7}}`,
		&v1.InputDeclaration{Name: "x", Type: v1.InputDeclaration_TYPE_STRUCT})
	require.NoError(t, err)

	encoded, err := marshalJSON(value.GetLiteral(), false)
	require.NoError(t, err)

	assert.Contains(t, string(encoded), `"int64Value":"2"`, "a whole field became something else")
	assert.Contains(t, string(encoded), "doubleValue", "a fractional field became something else")
	assert.Contains(t, string(encoded), `"int64Value":"7"`, "a number one level down was not converted")
	assert.NotContains(t, string(encoded), "flowstatev1: unsupported type",
		"a json.Number reached NewValue, which does not know it")
}

// runOutputsOf reads the declared outputs out of the document a run wrote, the way
// a caller would: by name, off the top-level field, as JSON.
//
// Plain JSON rather than a decoded [v1.GetResponse], which is what this used to do,
// because plain JSON is what the document now carries: `.runOutputs.replicas` is
// `3`, not `{"literal":{"int64Value":"3"}}`. See rundoc.go. That makes this helper
// the same thing a `jq` expression is, which is the point of the tests below — an
// int input that arrived as a string still fails here, and now fails on the
// difference between `3` and `"3"` rather than on which arm of a union it carried.
//
// UseNumber, so a declared `int` output stays exactly the digits the run computed
// rather than becoming a float64 on the way to being compared.
func runOutputsOf(tb testing.TB, stdout string) map[string]any {
	tb.Helper()

	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.UseNumber()

	var document struct {
		RunOutputs map[string]any `json:"runOutputs"`
	}
	require.NoError(tb, decoder.Decode(&document),
		"the run's document did not decode: %s", stdout)

	require.NotEmpty(tb, document.RunOutputs,
		"the run reported no declared outputs: %s", stdout)

	return document.RunOutputs
}
