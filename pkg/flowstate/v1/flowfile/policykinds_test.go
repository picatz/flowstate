package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const policyKindsSource = `edition: v2026.4
name: policy-kinds
errors:
  Refused: {}
steps:
  - id: fetch
    continue_on_error: [PolicyDenied, Refused]
    retry:
      attempts: 3
      except: [RateLimited]
    http:
      url: https://example.com/
  - id: sync
    continue_on_error: true
    retry:
      only: [Upstream, Timeout]
    http:
      url: https://example.com/sync
`

// TestPolicyKindListsRoundTrip: the lists compile to the wire fields, `true`
// keeps meaning every kind, and writing the workflow back reads the same.
func TestPolicyKindListsRoundTrip(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse([]byte(policyKindsSource))
	require.NoError(t, err)

	fetch := workflow.GetSteps()[0].GetPolicy()
	assert.True(t, fetch.GetContinueOnError(), "a list is tolerance, narrowed")
	assert.Equal(t, []string{"PolicyDenied", "Refused"}, fetch.GetToleratedKinds())
	assert.Equal(t, []string{"RateLimited"}, fetch.GetRetry().GetExcept())

	sync := workflow.GetSteps()[1].GetPolicy()
	assert.True(t, sync.GetContinueOnError())
	assert.Empty(t, sync.GetToleratedKinds(), "`true` names no kinds: every kind")
	assert.Equal(t, []string{"Upstream", "Timeout"}, sync.GetRetry().GetOnly())

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	round, err := flowfile.Marshal(again)
	require.NoError(t, err)
	require.Equal(t, string(written), string(round))
	require.Equal(t, fetch.GetToleratedKinds(), again.GetSteps()[0].GetPolicy().GetToleratedKinds())
	require.Equal(t, sync.GetRetry().GetOnly(), again.GetSteps()[1].GetPolicy().GetRetry().GetOnly())
}

func TestPolicyKindDiagnostics(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		edit string // replaces `except: [RateLimited]` in the source
		from string
		to   string
		want string
	}{
		"valid": {from: "", to: ""},
		"a misspelled tolerated kind suggests the nearest": {
			from: "PolicyDenied, Refused", to: "PolicyDenid, Refused", want: `did you mean "PolicyDenied"`,
		},
		"a misspelled retry kind suggests the nearest": {
			from: "except: [RateLimited]", to: "except: [RateLimted]", want: `did you mean "RateLimited"`,
		},
		"a permanent kind cannot be made retryable": {
			from: "only: [Upstream, Timeout]", to: "only: [Upstream, InvalidInput]", want: "never retried",
		},
		"a declared error is never retried": {
			from: "only: [Upstream, Timeout]", to: "only: [Upstream, Refused]", want: "never retried",
		},
		"stopping a kind that is never retried changes nothing": {
			from: "except: [RateLimited]", to: "except: [InvalidInput]", want: "never retried",
		},
		"a kind in both lists is a contradiction": {
			from: "only: [Upstream, Timeout]", to: "only: [Upstream, Timeout]\n      except: [Upstream]", want: "in both",
		},
		"a timeout cannot be excepted, attempts bounds it": {
			from: "except: [RateLimited]", to: "except: [Timeout]", want: "attempts: 1",
		},
		"an empty tolerated list is refused": {
			from: "[PolicyDenied, Refused]", to: "[]", want: "at least one failure kind",
		},
		"a repeated kind is refused": {
			from: "[PolicyDenied, Refused]", to: "[PolicyDenied, PolicyDenied]", want: "twice",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			src := policyKindsSource
			if tc.from != "" {
				require.Contains(t, src, tc.from)
				src = strings.Replace(src, tc.from, tc.to, 1)
			}

			diagnostics := validateTriggerSource(t, src)
			if tc.want == "" {
				require.Empty(t, diagnostics)

				return
			}
			var found bool
			for _, d := range diagnostics {
				found = found || strings.Contains(d.Message, tc.want)
			}
			require.True(t, found, "want %q in %v", tc.want, diagnostics)
		})
	}
}

// TestPolicyKindRulesApplyAtSubmit: a specification that never was a Flowfile
// is held to the same rule, through [v1.CheckPolicyKinds].
func TestPolicyKindRulesApplyAtSubmit(t *testing.T) {
	t.Parallel()

	policy := func(p *v1.StepPolicy) *v1.Workflow {
		return &v1.Workflow{Name: "w", Steps: []*v1.Node{{
			Id:     "s",
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: "log"}},
			Policy: p,
		}}}
	}

	require.NoError(t, v1.CheckPolicyKinds(policy(&v1.StepPolicy{ContinueOnError: true, ToleratedKinds: []string{"Upstream"}})))
	require.Error(t, v1.CheckPolicyKinds(policy(&v1.StepPolicy{ToleratedKinds: []string{"Upstream"}})),
		"kinds without continue_on_error would tolerate nothing")
	require.Error(t, v1.CheckPolicyKinds(policy(&v1.StepPolicy{ContinueOnError: true, ToleratedKinds: []string{"Nope"}})))
	require.Error(t, v1.CheckPolicyKinds(policy(&v1.StepPolicy{Retry: &v1.RetryPolicy{Only: []string{"InvalidInput"}}})))
	require.NoError(t, v1.CheckPolicyKinds(policy(&v1.StepPolicy{Retry: &v1.RetryPolicy{Except: []string{"RateLimited"}}})))
}
