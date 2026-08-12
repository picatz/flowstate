package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow validate` said ok on a workflow whose only step this build cannot run —
// `url: ftp://example.com/x` compiled, validated, and was refused on the first
// request.
//
// The interesting half of this file is not that the mistakes are reported. It is
// the line drawn under them: a great deal about a URL is decided by the deployment
// that runs it, and reporting any of that here would tell an author their file is
// wrong on the strength of configuration the machine they are typing on may not
// share. What is reported is what the http task itself cannot do, which is the
// same answer everywhere.

// fetching is a one-step workflow that requests a URL.
func fetching(url string) string {
	return fmt.Sprintf(`edition: v2026.3
name: fetches
steps:
  - id: web
    http:
      url: %s
`, url)
}

// TestAURLTheTaskCannotRequestIsReported covers what is decidable from the file
// alone.
//
// Each of these is a plain authoring mistake with no reading under which the author
// meant it, each was previously found by running the workflow, and each is a
// mistake in every deployment — the http task speaks HTTP wherever it runs.
func TestAURLTheTaskCannotRequestIsReported(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		url     string
		wantsay string
	}{
		{
			name:    "a protocol this task does not speak",
			url:     "ftp://example.com/secret.txt",
			wantsay: "http:// or https://",
		},
		{
			name:    "a scheme with a typo in it",
			url:     "htttp://example.com/",
			wantsay: "http:// or https://",
		},
		{
			name:    "a websocket URL in a task that speaks HTTP",
			url:     "ws://example.com/socket",
			wantsay: "http:// or https://",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds := diagnosticsFor(t, fetching(test.url))
			require.Len(t, ds, 1,
				"a URL the http task cannot request produced %d diagnostics, and one mistake "+
					"is one diagnostic", len(ds))

			assert.Contains(t, ds[0], test.wantsay,
				"the diagnostic does not say what to write instead, which is the half an author acts on")
			assert.Contains(t, ds[0], `input "url"`,
				"the diagnostic does not name the input it is about")
			assert.Regexp(t, `^\d+:\d+:`, ds[0],
				"the diagnostic has no position, so an editor cannot place it")
		})
	}
}

// TestWhatTheDeploymentDecidesIsNotReportedHere is the line, and the reason this
// asks about the task rather than about the egress policy.
//
// The policy refuses several of these, and every one of them is a property of where
// a run happens rather than of the file. A validator does not run where the worker
// runs — and `examples/conditional-and-retry` deliberately targets
// `http://localhost:1/hook`, says so in a comment, and tolerates the failure with
// `continue_on_error:`, because a step that always fails is what makes the
// tolerance demonstrate anything. A step whose request will be refused is not a
// broken step.
//
// The last two rows are the ones an implementation built on the policy gets wrong,
// and both are configuration a deployment supplies: a narrower scheme allowlist, a
// port restriction. Asking the policy would also have put a DNS lookup on the
// editor's keystroke path, since a policy with a proxy configured resolves the host
// to check it.
//
// A false diagnostic is worse than a missing one: an author who cannot tell which
// kind they are looking at stops reading them.
func TestWhatTheDeploymentDecidesIsNotReportedHere(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		url  string
	}{
		{
			name: "a literal address the policy denies by category",
			url:  "http://169.254.169.254/latest/meta-data/",
		},
		{
			name: "loopback by address, which one environment variable permits",
			url:  "http://127.0.0.1:8080/hook",
		},
		{
			name: "loopback by name, which is what a shipped example writes",
			url:  "http://localhost:1/hook",
		},
		{
			name: "a host that resolves to whatever it resolves to today",
			url:  "https://internal.example.com/health",
		},
		{
			// A deployment may be https-only, and this one is not. Reporting it
			// would also mean suggesting a correction — "write an http:// URL" —
			// that the same deployment refuses.
			name: "plain http, which a narrower policy may refuse and this build does not",
			url:  "http://example.com/health",
		},
		{
			// Ports are the same shape: any port is permitted unless a deployment
			// says otherwise, and what it says is not in this file.
			name: "an unusual port, which a policy may or may not permit",
			url:  "https://example.com:8443/health",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Empty(t, diagnosticsFor(t, fetching(test.url)),
				"a URL whose refusal depends on the network the run happens on was reported "+
					"as a mistake in the file")
		})
	}
}

// TestAURLTheAuthorHasNotWrittenYetIsNotChecked is the [v1.ResolvableInputs] rule
// applied to this check.
//
// An expression is not a URL until something evaluates it, and the validator has no
// scope to do that in. Guessing at what it will produce is how a correct workflow
// gets reported.
func TestAURLTheAuthorHasNotWrittenYetIsNotChecked(t *testing.T) {
	t.Parallel()

	for _, url := range []string{
		`"${vars.endpoint}"`,
		`"${'ftp://' + vars.host}"`,
		`"${steps.discover.namedValues.url}"`,
	} {
		t.Run(url, func(t *testing.T) {
			t.Parallel()

			source := `edition: v2026.3
name: fetches
vars:
  endpoint: https://example.com
  host: example.com
steps:
  - id: discover
    http:
      url: https://example.com/discover
  - id: web
    http:
      url: ` + url + "\n"

			for _, d := range diagnosticsFor(t, source) {
				assert.NotContains(t, d, "cannot request",
					"an expression was checked as though it were a URL")
			}
		})
	}
}

// TestTheSchemasOwnRuleStillAnswersFirst is the ordering, and it is the one thing
// here that is easy to get backwards.
//
// Three questions are asked about a literal input, narrowing: can the field hold
// this shape, does the schema's rule accept this value, and can the task do
// anything with it. Asking the last one first is what an earlier version of this
// did, and `url: not a uri at all` then produced *this* check's message about a
// value the schema had already refused, and refused better.
func TestTheSchemasOwnRuleStillAnswersFirst(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		url     string
		wantsay string
	}{
		{
			name: "a value that is not a URL at all",
			url:  "not a uri at all",
			// The schema's, because the value fails its rule.
			wantsay: "must be a valid URI",
		},
		{
			name: "a URL written from memory, with no scheme",
			url:  "example.com/health",
			// Also the schema's: a relative reference is not a valid URI, so the
			// policy is never asked. That is why this check has no message for a
			// missing scheme, and the reason is worth pinning — the obvious
			// improvement is to add one, and it would be unreachable.
			wantsay: "must be a valid URI",
		},
		{
			name: "a well-formed URL the task cannot request",
			url:  "ftp://example.com/x",
			// The task's, because the schema has no complaint: `ftp://` is a
			// perfectly good URI. This is the whole gap the check exists for.
			wantsay: "cannot request a ftp:// URL",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds := diagnosticsFor(t, fetching(test.url))
			require.Len(t, ds, 1,
				"one mistake produced %d diagnostics, so an author is being told the same "+
					"thing twice in two vocabularies", len(ds))

			assert.Contains(t, strings.Join(ds, "\n"), test.wantsay)
		})
	}
}
