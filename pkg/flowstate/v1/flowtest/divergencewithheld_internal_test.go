package flowtest

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

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

	// Under a posture that withholds everything the step's id is withheld
	// too, as the case's account withholds it.
	everything := withheldTranscript(transcript, v1.WithheldSensitiveValues()).GetStepValues()
	require.Contains(t, everything, sensitiveMarker, "a step id is shown under a posture that withholds everything")
	all := everything[sensitiveMarker].GetNamedValues()
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

// TestTheReportBesideADivergenceWithholdsWhatADivergingRunWithholds is
// #2218: the written-order run's own report, its verdict and its account, is
// printed beside a divergence, so once there is one the written-order case is
// run again withholding what every explored run withheld, and its report is
// that run's. Two seeded schedules, the first withholding a value without
// diverging and the second diverging, so what is withheld is every run's and
// not only the diverging one's. With no divergence, the report is the run's
// as recorded, and the case runs once per schedule.
func TestTheReportBesideADivergenceWithholdsWhatADivergingRunWithholds(t *testing.T) {
	t.Parallel()

	const quietOnly = "quiet-seeded-secret"
	for _, diverges := range []bool{true, false} {
		var (
			runs     int
			reshowns []sensitiveInputs
		)
		accumulator := newScheduleAccumulator(dst.Budget{Schedules: 2, Seed0: 1})
		result, _, _, account := accumulator.run(t.Context(), func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, caseShown, error) {
			runs++
			shown, moved := caseShown{}, "written"
			if v1.SchedulerFromContext(ctx) != v1.WrittenOrder {
				switch runs {
				case 2:
					shown.sensitive = v1.SensitiveValues{}.WithValues(quietOnly)
				case 3:
					if diverges {
						moved = "seeded"
					}
				}
			}
			// The case's own renderers withhold what its posture holds; a
			// re-shown run's posture is widened by what it is asked to hold.
			reshown := reshownPosture(ctx)
			if !reshown.Empty() {
				reshowns = append(reshowns, reshown)
			}
			text := "no such key: " + quietOnly
			if reshown.IsSensitive(quietOnly) {
				text = "no such key: " + sensitiveMarker
			}
			transcript := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"pick": {NamedValues: map[string]*v1.Value{"moved": v1.NewLiteral(moved)}},
			}}

			// A diagnostic's step id is printed as the file names it, and here
			// spells the value a seeded run withholds.
			failures := []*v1.Diagnostic{{Field: "expect.ran", Step: quietOnly, Message: "step did not run"}}
			// So is a warning's and the case's own name, both the file's words.
			warnings := []*v1.Diagnostic{{Step: quietOnly, Message: "stub for " + quietOnly + " never answered"}}

			return &v1.TestCase{Name: "moves " + quietOnly, Error: text, Failures: failures, Warnings: warnings, Duration: durationpb.New(time.Duration(runs) * time.Second)},
				nil, transcript, []TranscriptLine{{Text: "pick → " + text}}, shown, nil
		})

		if !diverges {
			require.Nil(t, accumulator.result().Divergence, "the runs diverged, so this proves nothing")
			assert.Equal(t, 3, runs, "a case with no divergence ran again")
			assert.Empty(t, reshowns)
			assert.Contains(t, result.GetError(), quietOnly, "a report with no divergence beside it was withheld")
			assert.Equal(t, quietOnly, result.GetFailures()[0].GetStep(), "a step with no divergence beside it was withheld")
			assert.Equal(t, "moves "+quietOnly, result.GetName(), "a name with no divergence beside it was withheld")

			continue
		}
		require.NotNil(t, accumulator.result().Divergence, "the runs did not diverge, so this proves nothing")
		require.Len(t, reshowns, 1, "the written-order case was not run again for the report")
		assert.True(t, reshowns[0].IsSensitive(quietOnly), "the report was not shown under what a non-diverging run withheld")
		assert.NotContains(t, result.GetError(), quietOnly, "the case's error shows what a seeded run withholds")
		assert.NotContains(t, account[0].Text, quietOnly, "the case's account shows what a seeded run withholds")
		assert.NotContains(t, result.GetFailures()[0].GetStep(), quietOnly, "a diagnostic's step shows what a seeded run withholds")
		assert.Equal(t, "moves "+sensitiveMarker, result.GetName(), "the case's name shows what a seeded run withholds")
		assert.Equal(t, time.Second, result.GetDuration().AsDuration(), "the report is timed by the re-run that only withholds more")
		assert.Equal(t, "moves "+sensitiveMarker, accumulator.result().Divergence.Case, "the divergence names the case with what a seeded run withholds")
		assert.NotContains(t, result.GetWarnings()[0].GetStep(), quietOnly, "a warning's step shows what a seeded run withholds")
		assert.NotContains(t, result.GetWarnings()[0].GetMessage(), quietOnly, "a warning shows what a seeded run withholds")
	}
}

// TestACaseRunUnderAReshownPostureWithholdsItByValue: a case run to be shown
// beside a divergence withholds what it is asked to from the start, through the
// renderers every report goes through. So a value too short to be replaced as
// a substring of finished text, here a one-rune output, is still withheld in
// the account and the verdict, which redacting the text afterward could not do
// (#2218).
func TestACaseRunUnderAReshownPostureWithholdsItByValue(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`edition: v2026.3
name: one-rune
steps:
  - id: pick
    value: ${"7"}
outputs:
  picked:
    value: ${steps.pick.value}
`), 0o600))
	path := filepath.Join(dir, "workflow.test.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`tests:
  - name: picks
    workflow: ./workflow.yaml
    expect:
      outputs:
        picked: "8"
`), 0o600))

	for _, reshown := range []bool{false, true} {
		ctx := t.Context()
		if reshown {
			ctx = withReshownPosture(ctx, v1.SensitiveValues{}.WithValues("7"))
		}
		result := RunPath(ctx, path, RunOptions{})
		c := result.Report.GetCases()[0]
		require.NotEmpty(t, c.GetFailures(), "the case passed, so this proves nothing")
		var shown strings.Builder
		for _, line := range result.Transcripts[0] {
			shown.WriteString(line.Text + "\n")
		}
		shown.WriteString(c.GetFailures()[0].GetMessage() + c.GetFailures()[0].GetValue())
		if !reshown {
			require.Contains(t, shown.String(), `"7"`, "the value never reached the report, so this proves nothing")

			continue
		}
		assert.NotContains(t, shown.String(), `"7"`, "a one-rune value the report was asked to withhold was shown")
	}
}

// TestAReportThatCannotBeShownAgainKeepsItsVerdict: the run that shows a
// report beside a divergence shares the case's time, so it may be cut short,
// and a run that is cut short, or reaches another verdict, cannot replace the
// report: seeds never change which cases pass. The reported verdict stands and
// its text is withheld whole, as is its account.
func TestAReportThatCannotBeShownAgainKeepsItsVerdict(t *testing.T) {
	t.Parallel()

	const secret = "seeded-only-secret"
	for name, cutShort := range map[string]bool{"the case's time is spent": true, "the run reaches another verdict": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var reran bool
			accumulator := newScheduleAccumulator(dst.Budget{Schedules: 1, Seed0: 1})
			result, _, _, account := accumulator.run(ctx, func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, caseShown, error) {
				shown, moved, passed := caseShown{}, "written", true
				switch {
				case !reshownPosture(ctx).Empty():
					reran = true
					passed = false
				case v1.SchedulerFromContext(ctx) != v1.WrittenOrder:
					shown.sensitive = v1.SensitiveValues{}.WithValues(secret)
					moved = "seeded"
					if cutShort {
						cancel()
					}
				}
				transcript := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"pick": {NamedValues: map[string]*v1.Value{"moved": v1.NewLiteral(moved)}},
				}}

				failures := []*v1.Diagnostic{{Field: "expect.outputs", Message: "saw " + secret}}
				warnings := []*v1.Diagnostic{{Field: "stubs", Message: "stub never answered"}}

				return &v1.TestCase{Name: "moves " + secret, Passed: passed, Error: "saw " + secret, Failures: failures, Warnings: warnings}, nil, transcript,
					[]TranscriptLine{{Text: "pick → " + secret}}, shown, nil
			})

			require.NotNil(t, accumulator.result().Divergence, "the runs did not diverge, so this proves nothing")
			assert.Equal(t, !cutShort, reran, "whether the report was run again")
			assert.True(t, result.GetPassed(), "a report shown beside a divergence changed the case's verdict")
			assert.NotContains(t, result.GetError(), secret)
			// Named still, and without the secret: a report that cannot say
			// which case it is would be no report.
			assert.Equal(t, "moves "+sensitiveMarker, result.GetName())
			assert.Equal(t, "moves "+sensitiveMarker, accumulator.result().Divergence.Case)
			// A field path is the harness's word, from which the report's code
			// and position are derived, and survives even withholding it all.
			require.Len(t, result.GetFailures(), 1)
			assert.NotContains(t, result.GetFailures()[0].GetMessage(), secret)
			assert.Equal(t, "expect.outputs", result.GetFailures()[0].GetField())
			require.Len(t, result.GetWarnings(), 1)
			assert.Equal(t, "stubs", result.GetWarnings()[0].GetField())
			require.Len(t, account, 1)
			assert.NotContains(t, account[0].Text, secret)
		})
	}
}

// TestANameThatSpellsAWithheldValueIsWithheld: a step id, a task, a stub's
// target and a signal are the file's names, and an author can spell a
// sensitive value as one. The account withholds each as it withholds an
// output's name, and so does a divergence's rendering of a step id (Codex,
// #2224); a name that spells nothing withheld is kept.
func TestANameThatSpellsAWithheldValueIsWithheld(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-name"
	recorder := newRunRecorder(v1.NewVirtualClock(time.Unix(0, 0)))
	recorder.sensitive = v1.SensitiveValues{}.WithValues(secret)
	recorder.record(transcriptEvent{kind: eventStubAnswered, step: secret, task: secret, stubStep: secret, stubOrdinal: 1})
	recorder.record(transcriptEvent{kind: eventStepFinished, step: secret})
	recorder.record(transcriptEvent{kind: eventStepSkipped, step: secret})
	recorder.record(transcriptEvent{kind: eventWaitStarted, step: "wait", signal: secret, timeout: time.Minute, bounded: true})
	recorder.record(transcriptEvent{kind: eventSignalDelivered, signal: secret})
	recorder.record(transcriptEvent{kind: eventSignalRefused, signal: secret, failure: "no"})
	recorder.record(transcriptEvent{kind: eventStubAnswered, task: secret, stubOrdinal: 2})
	recorder.record(transcriptEvent{kind: eventStepFinished, step: "public"})

	lines := recorder.render()
	require.Len(t, lines, 7)
	for _, line := range lines {
		assert.NotContains(t, line.Text, secret)
	}
	assert.Contains(t, lines[0].Text, sensitiveMarker, "the step was not named at all, so this proves nothing")
	assert.Contains(t, lines[len(lines)-1].Text, "public", "a name that spells nothing withheld was withheld")

	steps := withheldTranscript(&v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		secret: {}, "public": {},
	}}, v1.SensitiveValues{}.WithValues(secret)).GetStepValues()
	assert.Contains(t, steps, sensitiveMarker)
	assert.Contains(t, steps, "public")
	assert.NotContains(t, steps, secret)
}

// TestARunThatWithholdsEverythingShowsNoStepOfItsOwn: a run whose posture
// withholds everything rendered its values that way, but a step id is the
// file's own text, printed as written in a diagnostic, in its message, and in
// the run's failure a stub diagnostic shaped, so a divergence's rendering of
// that run withholds it in each (Codex, #2224).
func TestARunThatWithholdsEverythingShowsNoStepOfItsOwn(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-step"
	result := &v1.TestCase{Name: "case " + secret, Failures: []*v1.Diagnostic{{
		Field: "expect.ran", Step: secret, Message: "expected step \"" + secret + "\" to have run",
	}}}
	// The run's failure is a diagnostic the stub boundary shaped, which the
	// case's own report prints as it is, naming the step as the file wrote it.
	shaped := &stubDiagnostic{text: "flow test: task \"http\" was invoked, but this case declares no stub for it", shaped: true}
	runErr := fmt.Errorf("step %q: task \"http\": %w", secret, shaped)
	require.Contains(t, renderedRunError(runErr, v1.WithheldSensitiveValues()), secret,
		"the case's own report withheld the shaped diagnostic, so this proves nothing about the divergence")
	shown := caseShown{sensitive: v1.WithheldSensitiveValues(), runErr: runErr}

	rendered := shownCase(result, nil, shown, shownPosture{sensitive: v1.WithheldSensitiveValues()})
	require.Error(t, rendered.Err, "the verdict was not rendered, so this proves nothing")
	assert.NotContains(t, rendered.Err.Error(), secret)
	assert.Equal(t, secret, result.GetFailures()[0].GetStep(), "the recorded verdict must be left as it was")
}
