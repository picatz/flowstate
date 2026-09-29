package flowtest

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A schedule divergence is printed and emitted with `-o json`, so it must show
// no more of a run than the case's own report does (#2214). The local engine
// gives no legal Flowfile whose observables move with the schedule (see the
// note atop schedules_internal_test.go), so these tests stand in for the
// engine defect: the run is real, and a seeded schedule's transcript gains one
// step the written-order run's lacks.

// TestADivergenceShowsNoValueTheCaseWithholds: a called workflow's sensitive
// input, quoted by the run's error and carried at every depth of the
// transcript, appears in neither rendering, in the text or in the report.
func TestADivergenceShowsNoValueTheCaseWithholds(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	dir := t.TempDir()
	for name, source := range map[string]string{
		"child.yaml": `
edition: v2026.3
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: boom
    value: ${{"a":1}[inputs.api_key]}
`,
		"workflow.yaml": `
edition: v2026.3
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"` + secret + `"}
`,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600))
	}
	load := func() (*v1.Workflow, error) {
		workflow, _, err := flowfile.ParseFile(filepath.Join(dir, "workflow.yaml"))
		return workflow, err
	}

	accumulator := newScheduleAccumulator(dst.Budget{Schedules: 1, Seed0: 1})
	var shownSet sensitiveInputs
	accumulator.run(t.Context(), func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, caseShown, error) {
		result, spec, transcript, account, shown, runErr := runCase(ctx, &Test{Name: "diverges"}, "", load, false, fileVars{})
		require.ErrorContains(t, runErr, secret, "the run's error does not quote the secret, so this proves nothing")
		require.NotEmpty(t, transcript.GetStepValues(), "the run recorded nothing, so this proves nothing")
		shownSet = shown.sensitive
		if v1.SchedulerFromContext(ctx) != v1.WrittenOrder {
			transcript = proto.CloneOf(transcript)
			// Every shape a recorded value takes that can carry one: whole,
			// inside a larger string, at depth, in an error's message, and
			// inside a structure of values.
			transcript.StepValues["moved"] = &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"whole":  v1.NewLiteral(secret),
				"header": v1.NewLiteral("Bearer " + secret),
				"deep":   v1.NewValue(map[string]any{"keys": []any{"public", secret}}),
				"failed": {Kind: &v1.Value_Error_{Error: &v1.Value_Error{Message: "denied " + secret}}},
				"structure": {Kind: &v1.Value_Structure_{Structure: &v1.Value_Structure{
					Kind: &v1.Value_Structure_List_{List: &v1.Value_Structure_List{Values: []*v1.Value{v1.NewLiteral(secret)}}},
				}}},
				"mapped": {Kind: &v1.Value_Structure_{Structure: &v1.Value_Structure{
					Kind: &v1.Value_Structure_Map_{Map: &v1.Value_Structure_Map{Entries: map[string]*v1.Value{
						"k":                  v1.NewLiteral(secret),
						secret:               v1.NewLiteral(3),
						"x-" + secret + "-y": v1.NewLiteral(4),
					}}},
				}}},
				"public": v1.NewLiteral("shown as recorded"),
				// An output name is text a run chose, and an author can write a
				// sensitive value into one (exact-head review, #2214).
				secret:               v1.NewLiteral(1),
				"x-" + secret + "-y": v1.NewLiteral(2),
			}}
		}

		return result, spec, transcript, account, shown, runErr
	})
	require.True(t, shownSet.IsSensitive(secret), "the case's widened posture does not hold the callee's input")

	report := accumulator.result()
	require.NotNil(t, report.Divergence, "the seeded run's extra step was not reported, so this proves nothing")
	divergence := report.Divergence
	assert.NotEqual(t, divergence.WrittenOrder, divergence.Seeded, "what is shown must still show the difference")

	encoded, err := protojson.Marshal(report.Report())
	require.NoError(t, err)
	for _, text := range []string{divergence.WrittenOrder, divergence.Seeded, string(encoded)} {
		assert.NotContains(t, text, secret)
		assert.NotContains(t, text, hex.EncodeToString([]byte(secret)), "the transcript encoding carried the secret")
		assert.NotContains(t, text, hex.EncodeToString([]byte("hunter2")), "the transcript encoding carried part of the secret")
	}
	assert.Contains(t, divergence.Seeded, hex.EncodeToString([]byte("shown as recorded")),
		"a value with nothing to withhold must be shown as it was recorded")
	for _, rendering := range []string{divergence.WrittenOrder, divergence.Seeded} {
		assert.Contains(t, rendering, "no such key: [redacted]", "the run's error is not shown redacted")
		assert.Contains(t, rendering, "withheld: values this run does not disclose")
	}
}

// TestADivergenceShowsEachRunUnderWhatEitherWithholds: the written-order run
// withholds one value and the seeded run another, and both values reach both
// runs' transcripts, verdicts and errors. A reader sees both renderings at
// once, so neither may show what the other withholds (Codex, #2214).
func TestADivergenceShowsEachRunUnderWhatEitherWithholds(t *testing.T) {
	t.Parallel()

	const baselineOnly, seededOnly = "baseline-only-secret", "seeded-only-secret"
	accumulator := newScheduleAccumulator(dst.Budget{Schedules: 1, Seed0: 1})
	accumulator.run(t.Context(), func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, caseShown, error) {
		withholds, moved := baselineOnly, "written"
		if v1.SchedulerFromContext(ctx) != v1.WrittenOrder {
			withholds, moved = seededOnly, "seeded"
		}
		result := &v1.TestCase{Name: "moves", Failures: []*v1.Diagnostic{{
			Field:   "expect.outputs",
			Message: "want " + baselineOnly + ", got " + seededOnly,
		}}}
		transcript := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"pick": {NamedValues: map[string]*v1.Value{
				"candidates": v1.NewValue([]any{baselineOnly, seededOnly}),
				"moved":      v1.NewLiteral(moved),
			}},
		}}
		runErr := errors.New("no such key: " + baselineOnly + " or " + seededOnly)

		return result, nil, transcript, nil,
			caseShown{sensitive: v1.SensitiveValues{}.WithValues(withholds), runErr: runErr}, runErr
	})

	report := accumulator.result()
	require.NotNil(t, report.Divergence, "the moved value was not reported, so this proves nothing")
	divergence := report.Divergence
	assert.NotEqual(t, divergence.WrittenOrder, divergence.Seeded, "what is shown must still show the difference")
	for _, rendering := range []string{divergence.WrittenOrder, divergence.Seeded} {
		for _, secret := range []string{baselineOnly, seededOnly} {
			assert.NotContains(t, rendering, secret)
			assert.NotContains(t, rendering, hex.EncodeToString([]byte(secret)), "the transcript encoding carried a value the other run withholds")
		}
		assert.Contains(t, rendering, "withheld: values this run does not disclose")
	}
}

// TestADivergenceIsDecidedOnWhatIsNotShown: two runs differing only in a
// withheld value render alike, and still diverge, since the comparison reads
// the run and not what is shown of it.
func TestADivergenceIsDecidedOnWhatIsNotShown(t *testing.T) {
	t.Parallel()

	sensitive := v1.SensitiveValues{}.WithValues("first-secret-value", "second-secret-value")
	accumulator := newScheduleAccumulator(dst.Budget{Schedules: 1, Seed0: 1})
	accumulator.run(t.Context(), func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, caseShown, error) {
		value := "first-secret-value"
		if v1.SchedulerFromContext(ctx) != v1.WrittenOrder {
			value = "second-secret-value"
		}

		return &v1.TestCase{Name: "moves"}, nil,
			&v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"step": {NamedValues: map[string]*v1.Value{"token": v1.NewLiteral(value)}},
			}},
			nil, caseShown{sensitive: sensitive}, nil
	})

	report := accumulator.result()
	require.NotNil(t, report.Divergence, "a withheld difference must still be a divergence")
	assert.Equal(t, report.Divergence.WrittenOrder, report.Divergence.Seeded,
		"the difference is only in withheld values, so the renderings should read alike")
	assert.True(t, strings.HasSuffix(report.Divergence.Seeded, "the comparison read them\n"),
		"a reader of two identical renderings must be told why they diverged")
}

// TestAWithheldTranscriptWithholdsByValueAndKeepsItsShape: a value too short
// to be replaced as a substring is still withheld where it is the whole value,
// in a step's outputs and the run's alike; a posture that withholds everything
// withholds every value and message; one too deep to read is withheld whole;
// and the steps and names stay.
func TestAWithheldTranscriptWithholdsByValueAndKeepsItsShape(t *testing.T) {
	t.Parallel()

	transcript := &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"step": {NamedValues: map[string]*v1.Value{
				"pin":    v1.NewLiteral("7"),
				"public": v1.NewLiteral("public"),
				"failed": {Kind: &v1.Value_Error_{Error: &v1.Value_Error{Message: "denied"}}},
				"deep":   {Kind: &v1.Value_Literal{Literal: nestedPast(v1.MaxStructureDepth, "public")}},
			}},
		},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{"pin": v1.NewLiteral("7")}},
	}

	withheld := withheldTranscript(transcript, v1.SensitiveValues{}.WithValues("7"))
	step := withheld.GetStepValues()["step"].GetNamedValues()
	assert.Equal(t, sensitiveMarker, step["pin"].GetLiteral().GetStringValue())
	assert.Equal(t, sensitiveMarker, withheld.GetRunOutputs().GetValues()["pin"].GetLiteral().GetStringValue())
	assert.Same(t, transcript.GetStepValues()["step"].GetNamedValues()["public"], step["public"])
	assert.Equal(t, "denied", step["failed"].GetError().GetMessage())
	assert.Equal(t, sensitiveMarker, step["deep"].GetLiteral().GetStringValue(),
		"a value that cannot be read cannot be shown to be safe")
	assert.Equal(t, "7", transcript.GetStepValues()["step"].GetNamedValues()["pin"].GetLiteral().GetStringValue(),
		"the recorded transcript must be left as it was")

	all := withheldTranscript(transcript, v1.WithheldSensitiveValues()).GetStepValues()["step"].GetNamedValues()
	require.Len(t, all, 4, "a name withheld to one spelling must not overwrite another")
	for name, value := range all {
		assert.True(t, strings.HasPrefix(name, sensitiveMarker), "the name %q is shown under a posture that withholds everything", name)
		if value.GetError() != nil {
			assert.Equal(t, "[withheld]", value.GetError().GetMessage())
		} else {
			assert.Equal(t, sensitiveMarker, value.GetLiteral().GetStringValue())
		}
	}
	assert.Equal(t, sensitiveMarker, all[sensitiveMarker+"#4"].GetLiteral().GetStringValue(),
		"names are made distinct in the order they were recorded under, so the rendering does not depend on map order")
	assert.Equal(t, "[withheld]", all[sensitiveMarker+"#2"].GetError().GetMessage())
}

// nestedPast is leaf inside depth+1 lists, deeper than [v1.LiteralToGo] reads.
func nestedPast(depth int, leaf string) *expr.Value {
	value := &expr.Value{Kind: &expr.Value_StringValue{StringValue: leaf}}
	for range depth + 1 {
		value = &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: []*expr.Value{value}}}}
	}

	return value
}
