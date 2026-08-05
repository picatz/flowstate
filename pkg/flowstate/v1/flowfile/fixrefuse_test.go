package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// These cover issue #203: `flow fix` used to stamp `edition:` into any YAML
// document it was handed, including a fail-closed policy file parsed
// `yaml.Strict()` elsewhere in this repo. The fix is a positive allowlist —
// [flowfile.Fix] only acts on a document that declares `steps:` or `tests:` —
// and everything below is checked on bytes, per CLAUDE.md's own lesson about
// this exact class of bug: "a corrupted file still validates, it simply
// computes something else." A test asserting only that a refusal was reported
// would not have caught the original defect, because the original defect never
// failed to report success; it failed by acting at all.

// realPolicyFile locates one of the three files this repo parses
// `yaml.Strict()` for exactly the reason `flow fix` must never touch them:
// each one denies by default and denies on error, so an unrecognized key added
// by a well-meaning migration is not cosmetic, it is the control going dark.
func realPolicyFile(t *testing.T, rel string) string {
	t.Helper()

	path := filepath.Join("..", "..", "..", "..", "examples", rel)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("locating %s relative to this test: %v", rel, err)
	}
	return path
}

// TestFixRefusesEveryRealPolicyFile is the reproduction from issue #203, run
// against all three files it names, asserting the file `flow fix` touches is
// byte-identical afterward — not merely "still parses", which is what let the
// earlier corruption bugs CLAUDE.md records through.
func TestFixRefusesEveryRealPolicyFile(t *testing.T) {
	t.Parallel()

	for _, rel := range []string{
		"egress-policy.yaml",
		filepath.Join("http-secret", "auth-policy.yaml"),
		filepath.Join("http-federated", "auth-policy.yaml"),
	} {
		t.Run(rel, func(t *testing.T) {
			t.Parallel()

			path := realPolicyFile(t, rel)
			before, err := os.ReadFile(path)
			require.NoError(t, err)

			result, err := flowfile.Fix(before)
			require.NoError(t, err, "a policy file is valid YAML; Fix must not error on it")

			assert.False(t, result.Changed(), "flow fix must not touch %s at all", rel)
			require.NotEmpty(t, result.Refusals, "flow fix silently accepted a document it does not recognize: %s", rel)

			// Bytes, not "reads the same" — a rewriter that reformats a file it had
			// nothing to do with, or drops a byte doing so, is exactly the failure
			// this repo has already shipped twice under a validation-only test.
			assert.Equal(t, before, result.Source,
				"%s came back different from a refusal that should have left it untouched", rel)

			for _, refusal := range result.Refusals {
				assert.Contains(t, refusal.Message, "does not look like a Flowfile",
					"the refusal should say what the file looks like instead, not just that something is wrong")
			}
		})
	}
}

// TestFixRefusalLeavesTheEgressPolicyLoadable is the actual defect, proven
// end to end rather than inferred from a message. Issue #203's failure mode
// was never "the diagnostic looks wrong" — it was "flow fix exits 0 and
// `netpolicy.ParseConfig` refuses to load the file it just edited". This test
// runs both halves for real: `flow fix` against the shipped egress policy, and
// then the exact strict parser the worker uses against what it produced.
func TestFixRefusalLeavesTheEgressPolicyLoadable(t *testing.T) {
	t.Parallel()

	path := realPolicyFile(t, "egress-policy.yaml")
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	// Sanity check that this fixture is the fail-closed file the issue is
	// about, so a future edit to the example that removes its strictness does
	// not leave this test silently proving nothing.
	_, err = netpolicy.ParseConfig(before)
	require.NoError(t, err, "the shipped egress policy must parse before flow fix ever touches it")

	result, err := flowfile.Fix(before)
	require.NoError(t, err)
	require.NotEmpty(t, result.Refusals, "flow fix must refuse an egress policy rather than silently edit it")

	// The proof: the strict, fail-closed parser the worker actually uses still
	// accepts what flow fix produced. Before the fix in #203, this failed with
	// `unknown field "edition"` — the security control going dark, silently,
	// on a command that exited 0.
	_, err = netpolicy.ParseConfig(result.Source)
	require.NoError(t, err, "flow fix left the egress policy unparseable, disabling a fail-closed control")
}

// TestFixLooksLikeFlowfileAgreesWithFix pins [flowfile.LooksLikeFlowfile] —
// which a directory walk uses to decide what to even hand to [flowfile.Fix] —
// against the same three policy files and a genuine Flowfile and Flowfile
// test, so the two never drift into recognizing different documents.
func TestFixLooksLikeFlowfileAgreesWithFix(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		data []byte
		want bool
	}{
		{
			name: "a Flowfile",
			data: []byte("edition: v2026.2\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n"),
			want: true,
		},
		{
			name: "a Flowfile test",
			data: []byte("edition: v2026.2\ntests:\n  - name: a case\n    workflow: ./workflow.yaml\n    expect: {}\n"),
			want: true,
		},
		{
			name: "a Flowfile declaring no steps at all",
			data: []byte("edition: v2026.2\nname: t\n"),
			want: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, flowfile.LooksLikeFlowfile(tt.data))

			result, err := flowfile.Fix(tt.data)
			require.NoError(t, err)
			assert.Empty(t, result.Refusals, "Fix refused a document LooksLikeFlowfile accepted")
		})
	}

	for _, rel := range []string{
		"egress-policy.yaml",
		filepath.Join("http-secret", "auth-policy.yaml"),
		filepath.Join("http-federated", "auth-policy.yaml"),
	} {
		t.Run(rel, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(realPolicyFile(t, rel))
			require.NoError(t, err)

			assert.False(t, flowfile.LooksLikeFlowfile(data), "%s should not look like a Flowfile", rel)

			result, err := flowfile.Fix(data)
			require.NoError(t, err)
			assert.NotEmpty(t, result.Refusals, "Fix accepted a document LooksLikeFlowfile rejected")
		})
	}
}

// TestFixStampsAFlowfileTestNormally is the non-regression direction for a
// Flowfile test: recognizing `tests:` must not become recognizing-but-refusing
// it some other way, and the edition stamp — the entire drift issue #203
// reports — must still land.
func TestFixStampsAFlowfileTestNormally(t *testing.T) {
	t.Parallel()

	src := `tests:
  - name: a case
    workflow: ./workflow.yaml
    expect: {}
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)
	require.True(t, result.Changed(), "a Flowfile test with no edition must still be stamped")
	assert.True(t, strings.HasPrefix(string(result.Source), "edition: "+flowfile.CurrentEdition+"\n"))
}

// TestFixStillRepairsAMalformedButRecognizableFlowfile is the converse of the
// refusal: a document that declares `steps:` is a Flowfile even when what is
// under `steps:` is written in a spelling this build no longer accepts, and
// the whole point of `flow fix` is to repair exactly that file rather than
// refuse it alongside a policy file.
func TestFixStillRepairsAMalformedButRecognizableFlowfile(t *testing.T) {
	t.Parallel()

	src := `name: greeter
steps:
  - id: greet
    task:
      name: log
      inputs:
        message: hi
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "a malformed Flowfile is not the same as an unrecognized document")
	require.True(t, result.Changed())
	assert.Contains(t, string(result.Source), "log:\n      message: hi")
	assert.Contains(t, string(result.Source), "edition: "+flowfile.CurrentEdition)
}

// TestFixRefusesADocumentWhoseOnlyKeysAreNameAndDescription covers the gap the
// "every key is a workflow key" branch of the allowlist would otherwise leave.
//
// `name:` and `description:` are spelled by nearly every configuration format
// there is, so a document declaring only those two is not a Flowfile that has
// yet to grow steps — it is a stranger whose keys happen to collide with ours.
// Accepting it would stamp an edition into somebody else's file, which is the
// whole defect #203 exists to fix, in a smaller and later-arriving form.
//
// The companion direction is TestFixStillFixesAZeroStepFlowfile below: the
// narrowing must not cost the real edge case the branch was added for.
func TestFixRefusesADocumentWhoseOnlyKeysAreNameAndDescription(t *testing.T) {
	t.Parallel()

	src := "name: something\ndescription: a config that is not a workflow\n"

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.NotEmpty(t, result.Refusals, "a document of only name: and description: must be refused")
	assert.False(t, result.Changed(), "a refused document must not be rewritten")
	assert.Equal(t, src, string(result.Source), "a refused document must come back byte-for-byte")
}

// TestFixStillFixesAZeroStepFlowfile is the direction the narrowing above must
// not break: `edition:` is distinctive, so a legal Flowfile that declares no
// steps is still recognized and still fixed.
func TestFixStillFixesAZeroStepFlowfile(t *testing.T) {
	t.Parallel()

	result, err := flowfile.Fix([]byte("name: t\n"))
	require.NoError(t, err)
	assert.NotEmpty(t, result.Refusals, "name: alone is not distinctive enough to qualify")

	current, err := flowfile.Fix([]byte("edition: v2026.2\nname: t\n"))
	require.NoError(t, err)
	assert.Empty(t, current.Refusals, "edition: is distinctive, so a zero-step Flowfile is still recognized")
	assert.False(t, current.Changed())
}
