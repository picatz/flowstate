package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A step id is unique within a *visibility domain*, not within a file: two
// sibling `loop:` blocks may each declare a body step called `page`, because
// body outputs do not escape a loop, and the engine evaluates each loop's
// `until:`/`update:` against the scope its own body finished in.
//
// The scope walk here has always got the *names* right — the second loop's
// `until:` is checked against a scope holding that loop's body ids. What it got
// wrong was the step behind the name: the check on `steps.<id>.<output>` found
// its node by searching the whole workflow for that id, so from the second loop
// it read the *first* loop's `page` and reported its outputs as the ones that
// exist. A legal file was told it referenced an output nothing produces, which
// is the failure this package treats as worse than saying nothing at all.
//
// These are the validator's half of #323. The editor's half — hover and
// definition resolving the same reference to the same wrong step — is in
// pkg/flowstate/v1/flowfile/lsp.

// loopIDReuseSource reuses the body-step id `page` across two sequential loops
// whose bodies produce different output sets: one runs a task, one is a
// `value:`. Identical bodies would pass under a first-match lookup by accident.
const loopIDReuseSource = `edition: v2026.3
name: loop-id-reuse
steps:
  - id: crawl
    loop:
      as: cursor
      init: ${'start'}
      until: ${steps.page.body == 'done'}
      update: ${steps.page.body}
      max_iterations: 5
      steps:
        - id: page
          http:
            method: GET
            url: ${'https://example.invalid/' + cursor}
  - id: tally
    loop:
      as: total
      init: ${0}
      until: ${steps.page.value >= 3}
      update: ${steps.page.value + 1}
      max_iterations: 5
      steps:
        - id: page
          value: ${total + 1}
`

// TestSiblingLoopsMayReuseABodyStepID is the positive direction: the file is
// accepted, and each loop's own expressions are checked against its own body.
func TestSiblingLoopsMayReuseABodyStepID(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(loopIDReuseSource))
	require.NoError(t, err)
	assert.Empty(t, diags,
		"reusing a body-step id across sibling loops is legal: body outputs do not escape, so the two names never meet")
}

// TestAReusedBodyStepIDIsCheckedAgainstTheStepInScope is the negative
// direction, and the one that would pass by accident on a fix that merely
// silenced the check. Each loop's reference is made wrong in turn, and the
// output set the diagnostic names has to be that loop's own step's.
func TestAReusedBodyStepIDIsCheckedAgainstTheStepInScope(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		from    string
		to      string
		names   string
		reports string
	}{
		{
			name: "the first loop's page is an http step",
			from: "until: ${steps.page.body == 'done'}",
			to:   "until: ${steps.page.nope == 'done'}",
			// The http task's declared outputs, not the `value:` step's.
			names:   "status_code",
			reports: `step "page" has no output "nope"`,
		},
		{
			name: "the second loop's page is a value step",
			from: "until: ${steps.page.value >= 3}",
			to:   "until: ${steps.page.nope >= 3}",
			// The `value:` step's own sentence, not the http task's list.
			names:   "a `value:` step produces exactly one output",
			reports: `step "page" has no output "nope"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := strings.Replace(loopIDReuseSource, tc.from, tc.to, 1)
			require.NotEqual(t, loopIDReuseSource, src, "the fixture no longer holds the line this case rewrites")

			diags, err := flowfile.ValidateSource([]byte(src))
			require.NoError(t, err)
			require.NotEmpty(t, diags, "a name the step in scope does not produce is still a mistake")

			var reported strings.Builder
			for _, d := range diags {
				reported.WriteString(d.Message)
				reported.WriteString("\n")
			}
			assert.Contains(t, reported.String(), tc.reports)
			assert.Contains(t, reported.String(), tc.names,
				"the diagnostic describes a step in another block, whose outputs this expression cannot reach")
		})
	}
}
