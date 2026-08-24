package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// #734, from the client's side: `flow run` redacted a run's outputs against the
// specification it *submitted*, and a deployment may have run a different one.
//
// The test that matters is the negative direction CLAUDE.md's "Test that A cannot
// reach B" section describes. Asserting that a caller holding the executed
// specification redacts precisely is a functionality test — it was already passing
// while the bug was live. What has to be asserted is that a value only the
// *executed* specification marks sensitive never reaches the screen, given a
// submitted copy that says it is ordinary. Since a client is never told which names
// the executed copy marks, the only safe answer is to withhold every declared
// output, and that is what these check.

// submittedSpecSayingNothingIsSensitive is the caller's copy in the case the bug
// lived in: it names the same output the deployment's copy does, and declares it
// perfectly ordinary.
func submittedSpecSayingNothingIsSensitive() *v1.Workflow {
	return &v1.Workflow{
		Name: "attested",
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "token"},
			{Name: "url"},
		},
	}
}

// startedWith is what `Run` answered, carrying one of the three attestations
// [v1.RunResponse.RanSubmittedSpecification] distinguishes: proto.Bool(true),
// proto.Bool(false), or nil for a server that said nothing.
func startedWith(attestation *bool) *v1.RunResponse {
	return &v1.RunResponse{
		WorkflowId:               "flowstate-workflow-3f7c",
		RunId:                    "0198f1e2-0000-7000-8000-000000000000",
		Status:                   v1.RunResponse_STATUS_RUNNING,
		SpecificationAsSubmitted: attestation,
	}
}

// completedRunReporting is a finished run's answer, carrying the value the executed
// specification declared sensitive and the submitted copy did not.
func completedRunReporting(secret string) *v1.GetResponse {
	return &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		Status:     v1.RunResponse_STATUS_COMPLETED,
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"token": v1.NewLiteral(secret),
			"url":   v1.NewLiteral("https://example.com/build/12"),
		}},
		Kind: &v1.GetResponse_Outputs{
			Outputs: &v1.Workflow_StepOutputs{
				RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
					"token": v1.NewLiteral(secret),
				}},
				StepValues: map[string]*v1.Node_Outputs{
					"mint": {NamedValues: map[string]*v1.Value{"value": v1.NewLiteral(secret)}},
				},
			},
		},
	}
}

// TestASubstitutedSpecificationWithholdsWhatTheSubmittedCopyCalledOrdinary is the
// leak itself.
//
// The submitted copy declares `token` ordinary; the deployment's copy — which is
// what ran — declares it sensitive. Before this, the value printed in the clear
// with no `--reveal-sensitive` typed, because the local file said it was fine.
// Nothing here can consult the executed copy, so every declared output is withheld
// and the secret must be absent from every field the answer travels in.
func TestASubstitutedSpecificationWithholdsWhatTheSubmittedCopyCalledOrdinary(t *testing.T) {
	for _, test := range []struct {
		name        string
		attestation *bool
	}{
		{name: "the server said its own copy ran", attestation: proto.Bool(false)},
		{name: "the server said nothing at all", attestation: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			submitted := submittedSpecSayingNothingIsSensitive()

			executed := executedSpecification(submitted, startedWith(test.attestation))
			require.Nil(t, executed,
				"a specification the server did not attest was trusted to describe the run")

			redacted := redactGetResponse(completedRunReporting(secretString), executed, false)

			require.Equal(t, "[redacted: token]",
				redacted.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue(),
				"a value only the executed specification marks sensitive was printed")
			require.Equal(t, "[redacted: token]",
				redacted.GetOutputs().GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())

			// Containment shapes, per CLAUDE.md: the value has to be gone from
			// the message, not merely covered by a marker printed beside it, and
			// reflection through a struct that holds the message is where a
			// redaction that only fixes the direct rendering leaks anyway.
			type holder struct{ Response *v1.GetResponse }
			h := holder{Response: redacted}

			//lint:ignore S1025 %s is the path an operator's log line takes, so it is the path under test
			asVerb := fmt.Sprintf("%s", redacted)

			for _, rendered := range []string{
				fmt.Sprintf("%v", redacted),
				fmt.Sprintf("%+v", redacted),
				fmt.Sprintf("%#v", redacted),
				asVerb,
				fmt.Sprintf("%v", h),
				fmt.Sprintf("%+v", h),
				fmt.Sprintf("%v", []*v1.GetResponse{redacted}),
			} {
				require.NotContains(t, rendered, secretString)
			}
		})
	}
}

// TestAnAttestedSpecificationKeepsThePreciseView is the non-regression direction,
// and the reason this is an attestation rather than a blanket fail-closed rule for
// every remote run: the ordinary case — no substitution — must still redact exactly
// what the author's own file marked and nothing else.
func TestAnAttestedSpecificationKeepsThePreciseView(t *testing.T) {
	submitted := submittedSpecSayingNothingIsSensitive()
	submitted.DeclaredOutputs[0].Sensitive = true

	executed := executedSpecification(submitted, startedWith(proto.Bool(true)))
	require.Same(t, submitted, executed,
		"a specification the server attested must be the one redaction consults")

	redacted := redactGetResponse(completedRunReporting(secretString), executed, false)

	require.Equal(t, "[redacted: token]",
		redacted.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())
	require.Equal(t, "https://example.com/build/12",
		redacted.GetRunOutputs().GetValues()["url"].GetLiteral().GetStringValue(),
		"a value the executed specification did not mark sensitive must render unchanged")
}

// TestTheFollowSaysWhyItIsWithholding covers the diagnostic, which is what keeps
// the degraded view from reading as a bug.
//
// It has to name both readings — a substituted copy, or a server too old to say —
// because the client cannot tell them apart and must not assert either.
func TestTheFollowSaysWhyItIsWithholding(t *testing.T) {
	surface, out, errOut := plainSurface()

	noteUnattestedSpecification(surface)

	require.Empty(t, out.String(), "an account of how the answer was produced went to stdout")
	account := errOut.String()
	require.Contains(t, account, "did not confirm")
	require.Contains(t, account, "deployment-owned copy")
	require.Contains(t, account, "predates the attestation")
}
