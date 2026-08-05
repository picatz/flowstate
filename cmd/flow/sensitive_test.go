package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

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

// TestRedactedMarkerIsHonestAndUnmistakable checks requirement 4: the marker must
// not look like a value the workload could have produced, and it must name what
// was withheld.
func TestRedactedMarkerIsHonestAndUnmistakable(t *testing.T) {
	marker := redactedMarker("api_key")

	require.Equal(t, "[redacted: api_key]", marker)
	require.Contains(t, marker, "redacted")
	require.Contains(t, marker, "api_key")
}
