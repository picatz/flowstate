package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const secretString = "sk-live-0123456789abcdef"

// TestSensitiveOutputNamesDistinguishesNoSpecFromNoSensitiveOutputs is the
// difference [sensitiveOutputNames]'s own comment insists matters: nil (no
// specification) and an empty, non-nil set (a real specification that declared
// nothing sensitive) must not collapse into one answer, or one of the two
// direction's callers gets the wrong default.
func TestSensitiveOutputNamesDistinguishesNoSpecFromNoSensitiveOutputs(t *testing.T) {
	require.Nil(t, sensitiveOutputNames(nil), "no specification must answer nil, not an empty set")

	workflow := &v1.Workflow{DeclaredOutputs: []*v1.OutputDeclaration{{Name: "url"}}}
	names := sensitiveOutputNames(workflow)
	require.NotNil(t, names, "a real specification must answer a non-nil set even when nothing is sensitive")
	require.Empty(t, names)

	workflow.DeclaredOutputs = append(workflow.DeclaredOutputs, &v1.OutputDeclaration{Name: "token", Sensitive: true})
	names = sensitiveOutputNames(workflow)
	require.True(t, names["token"])
	require.False(t, names["url"])
}

// TestRedactGetResponseFailsClosedWithNoWorkflow is the ambiguous case CLAUDE.md
// names directly: a declaration missing, a spec absent, or an older run whose
// spec predates the field. workflow == nil covers all three here, since this
// package's callers pass nil precisely when none of those is available — see
// sensitive.go's package comment. Every declared output must be withheld, and the
// real value must be absent from the rendered message, not merely covered by a
// marker printed alongside it.
func TestRedactGetResponseFailsClosedWithNoWorkflow(t *testing.T) {
	response := &v1.GetResponse{
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"token": v1.NewLiteral(secretString),
		}},
		Kind: &v1.GetResponse_Outputs{
			Outputs: &v1.Workflow_StepOutputs{
				RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
					"token": v1.NewLiteral(secretString),
				}},
			},
		},
	}

	redacted := redactGetResponse(response, nil, false)

	require.Equal(t, "[redacted: token]",
		redacted.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())
	require.Equal(t, "[redacted: token]",
		redacted.GetOutputs().GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue(),
		"both places the answer travels must be redacted, not only the top-level field")

	// Containment shapes: %v, %+v, %#v and %s on the redacted message, and on a
	// struct and a slice holding it, must never surface the real string. This is
	// the exact bug shape CLAUDE.md's "secrets never enter workflow history"
	// section describes for reflection through a struct, applied to a renderer
	// instead of a redacting String method.
	type holder struct{ Response *v1.GetResponse }
	h := holder{Response: redacted}
	slice := []*v1.GetResponse{redacted}

	// The %s verb is spelled rather than String() called, deliberately: an
	// operator's log line spells the verb, so the verb is what has to be proven
	// safe. Calling String() here would assert something adjacent to, and not
	// the same as, the path that actually leaks.
	//lint:ignore S1025 see above — %s is the path under test, not a shortcut for String()
	asVerb := fmt.Sprintf("%s", redacted)

	for _, rendered := range []string{
		fmt.Sprintf("%v", redacted),
		fmt.Sprintf("%+v", redacted),
		fmt.Sprintf("%#v", redacted),
		asVerb,
		fmt.Sprintf("%v", h),
		fmt.Sprintf("%+v", h),
		fmt.Sprintf("%v", slice),
	} {
		require.NotContains(t, rendered, secretString)
	}
}

// TestRedactGetResponseRevealBypassesRedaction checks the escape hatch defeats
// the fail-closed path exactly as it defeats the precise one.
func TestRedactGetResponseRevealBypassesRedaction(t *testing.T) {
	response := &v1.GetResponse{
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"token": v1.NewLiteral(secretString),
		}},
	}

	redacted := redactGetResponse(response, nil, true)

	require.Equal(t, secretString, redacted.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())
}

// TestRedactGetResponsePrecisionWithSpec is the non-regression direction: given a
// real specification, only the names it marked sensitive change, and everything
// else renders exactly as produced.
func TestRedactGetResponsePrecisionWithSpec(t *testing.T) {
	workflow := &v1.Workflow{
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "token", Sensitive: true},
			{Name: "url"},
		},
	}
	response := &v1.GetResponse{
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"token": v1.NewLiteral(secretString),
			"url":   v1.NewLiteral("https://example.com/build/12"),
		}},
	}

	redacted := redactGetResponse(response, workflow, false)

	require.Equal(t, "[redacted: token]",
		redacted.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())
	require.Equal(t, "https://example.com/build/12",
		redacted.GetRunOutputs().GetValues()["url"].GetLiteral().GetStringValue(),
		"a value the specification did not mark sensitive must render unchanged")
}

// TestRedactGetResponseNilAndEmptyPassThrough checks the boring paths a fail-open
// mistake likes to hide in: a nil response, and a response with no run outputs at
// all, must pass through unchanged rather than panic or fabricate a document.
func TestRedactGetResponseNilAndEmptyPassThrough(t *testing.T) {
	require.Nil(t, redactGetResponse(nil, nil, false))

	response := &v1.GetResponse{Status: v1.RunResponse_STATUS_RUNNING}
	require.Same(t, response, redactGetResponse(response, nil, false))
}

// TestRedactGetResponseRedactsSensitiveInputInTranscriptWithoutRunOutputs
// reproduces the loop-failure disclosure: a tolerated for_each failure records
// its bound item in the transcript, while a workflow with no declared outputs
// legitimately has nil RunOutputs. Redaction must not use that nil field as a
// reason to skip the transcript.
func TestRedactGetResponseRedactsSensitiveInputInTranscriptWithoutRunOutputs(t *testing.T) {
	workflow := &v1.Workflow{
		DeclaredInputs: []*v1.InputDeclaration{{Name: "customers", Sensitive: true}},
	}
	response := &v1.GetResponse{
		Kind: &v1.GetResponse_Outputs{
			Outputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"process": {NamedValues: map[string]*v1.Value{
						"results": v1.NewLiteral(secretString),
					}},
				},
			},
		},
	}

	redacted := redactGetResponse(response, workflow, false)

	require.Nil(t, redacted.GetRunOutputs(), "redaction must not fabricate run outputs")
	result := redacted.GetOutputs().GetStepValues()["process"].GetNamedValues()["results"].GetLiteral().GetStringValue()
	require.NotEqual(t, secretString, result)
	require.Contains(t, result, "redacted")
	require.Equal(t, secretString,
		response.GetOutputs().GetStepValues()["process"].GetNamedValues()["results"].GetLiteral().GetStringValue(),
		"redaction must not mutate the response shared by another renderer")
}

// TestRedactedMarkerIsHonestAndUnmistakable checks requirement 4: the marker must
// not look like a value the workload could have produced, and it must name what
// was withheld.
func TestRedactedMarkerIsHonestAndUnmistakable(t *testing.T) {
	marker := redactedMarker("api_key")

	require.Equal(t, "[redacted: api_key]", marker)
	require.Contains(t, marker, "redacted")
	require.Contains(t, marker, "api_key")
}

// codexStepComputedSensitiveResponse builds the exact shape Codex found on PR
// #212: a declared output computed from a step's own output —
// `outputs.token.value: ${steps.fetch.token}` with `sensitive: true` — so the
// raw secret sits in three places at once, exactly as a completed run answers in
// production: the top-level [v1.RunOutputs], the nested
// [v1.Workflow_StepOutputs.RunOutputs] the oneof carries beside the transcript,
// and the transcript itself, at the step that computed it.
//
// A second, unrelated step and a second, non-sensitive declared output ride
// along so a test can tell "the transcript is gone because it is untouched" from
// "the transcript is gone because nothing declared here is sensitive."
func codexStepComputedSensitiveResponse() (*v1.Workflow, *v1.GetResponse) {
	workflow := &v1.Workflow{
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "token", Sensitive: true},
			{Name: "region"},
		},
	}

	runOutputs := &v1.RunOutputs{Values: map[string]*v1.Value{
		"token":  v1.NewLiteral(secretString),
		"region": v1.NewLiteral("us-east-1"),
	}}

	response := &v1.GetResponse{
		RunOutputs: proto.Clone(runOutputs).(*v1.RunOutputs),
		Kind: &v1.GetResponse_Outputs{
			Outputs: &v1.Workflow_StepOutputs{
				RunOutputs: proto.Clone(runOutputs).(*v1.RunOutputs),
				StepValues: map[string]*v1.Node_Outputs{
					"fetch": {NamedValues: map[string]*v1.Value{
						"token": v1.NewLiteral(secretString),
					}},
					"place": {NamedValues: map[string]*v1.Value{
						"region": v1.NewLiteral("us-east-1"),
					}},
				},
			},
		},
	}

	return workflow, response
}

// TestRedactGetResponseRedactsStepTranscriptForStepComputedSensitiveOutput is the
// Codex finding on PR #212, reproduced directly: `redactRunOutputsValues`
// withheld `token` at the name it surfaced under and left the same raw value in
// the step transcript one line down, in the clear. The real value must be
// absent from the whole message — checked here by marshaling it exactly as the
// MCP surface does (`protojson`) and asserting the bytes never contain it,
// rather than asserting a marker is merely present somewhere, which a value
// printed twice could still satisfy.
func TestRedactGetResponseRedactsStepTranscriptForStepComputedSensitiveOutput(t *testing.T) {
	workflow, response := codexStepComputedSensitiveResponse()

	redacted := redactGetResponse(response, workflow, false)

	require.Equal(t, "[redacted: token]",
		redacted.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())
	require.Equal(t, "[redacted: token]",
		redacted.GetOutputs().GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())

	fetched := redacted.GetOutputs().GetStepValues()["fetch"].GetNamedValues()["token"].GetLiteral().GetStringValue()
	require.NotEqual(t, secretString, fetched,
		"the raw value must not survive in the step transcript that fed the sensitive output")
	require.Contains(t, fetched, "redacted",
		"withheld silently is worse than withheld with a marker — CLAUDE.md's fail-closed section")

	// This file's chosen design (Option A, see redactStepValues's own comment)
	// is blunt on purpose: the whole transcript is withheld once any declared
	// output is sensitive, not only the step that happens to feed it. Proving
	// that "place" — which fed only the non-sensitive "region" — is withheld
	// too is what tells this test apart from one that merely checked the one
	// entry the bug report named.
	placed := redacted.GetOutputs().GetStepValues()["place"].GetNamedValues()["region"].GetLiteral().GetStringValue()
	require.NotEqual(t, "us-east-1", placed,
		"Option A withholds the whole transcript, including steps unrelated to the sensitive output")

	// The declared answer for the name nothing marked sensitive is untouched —
	// this design is blunt about the transcript and precise about the answer.
	require.Equal(t, "us-east-1",
		redacted.GetRunOutputs().GetValues()["region"].GetLiteral().GetStringValue())

	encoded, err := protojson.Marshal(redacted)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), secretString,
		"the secret must be absent from the exact bytes the MCP surface serializes")
}

// TestRedactGetResponseRevealShowsStepTranscript checks the escape hatch covers
// the transcript exactly as it covers the named answer: --reveal-sensitive
// shows both, because a caller who asked for it by name gets what they asked
// for.
func TestRedactGetResponseRevealShowsStepTranscript(t *testing.T) {
	workflow, response := codexStepComputedSensitiveResponse()

	redacted := redactGetResponse(response, workflow, true)

	require.Equal(t, secretString,
		redacted.GetOutputs().GetStepValues()["fetch"].GetNamedValues()["token"].GetLiteral().GetStringValue())
}

// TestRedactStepValuesLeavesTranscriptUntouchedWhenNothingIsSensitive is the
// over-redaction direction CLAUDE.md warns a fix can hide behind: a rewrite that
// wipes the whole transcript unconditionally would still pass the Codex
// reproduction above. This is the workflow with a real specification that
// declares no sensitive output at all — the same "empty, non-nil set" case
// [sensitiveOutputNames] documents — and the transcript a step computed a
// non-sensitive output from must render exactly as produced.
func TestRedactStepValuesLeavesTranscriptUntouchedWhenNothingIsSensitive(t *testing.T) {
	workflow := &v1.Workflow{
		DeclaredOutputs: []*v1.OutputDeclaration{{Name: "region"}},
	}
	response := &v1.GetResponse{
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"region": v1.NewLiteral("us-east-1"),
		}},
		Kind: &v1.GetResponse_Outputs{
			Outputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"place": {NamedValues: map[string]*v1.Value{
						"region": v1.NewLiteral("us-east-1"),
					}},
				},
			},
		},
	}

	redacted := redactGetResponse(response, workflow, false)

	require.Equal(t, "us-east-1",
		redacted.GetOutputs().GetStepValues()["place"].GetNamedValues()["region"].GetLiteral().GetStringValue(),
		"a specification that declared nothing sensitive must leave the transcript exactly as produced")
}
