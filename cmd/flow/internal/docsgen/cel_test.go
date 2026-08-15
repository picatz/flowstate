package docsgen

import (
	"regexp"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// celFence matches a fenced ```cel code block the way docs/reference/cel.md's
// idiom section writes one — the same shape #571's TestEveryDocumentedRuleCompiles
// looks for in auth's doc comments, applied here to a generated document instead
// of a hand-written one.
//
// (?s) so `.` crosses the newlines a multi-line idiom's expression spans.
var celFence = regexp.MustCompile("(?s)```cel\n(.*?)\n```")

// idiomEnv is [v1.CurrentProfile]'s environment extended with every Flowfile root
// an idiom's expression may reference, declared `dyn` — the identical technique
// pkg/flowstate/v1/flowfile/celcheck.go uses to type-check a workflow's own
// expressions without needing to know their scope: an identifier that is not
// actually reachable where the idiom is meant to sit is not this test's question,
// only whether the expression itself is well-formed CEL against the profile it
// claims.
func idiomEnv(t *testing.T) *cel.Env {
	t.Helper()

	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	require.NoError(t, err)

	base, err := v1.DefaultEvaluator().Env(libs...)
	require.NoError(t, err)

	env, err := base.Extend(
		cel.Variable(v1.PayloadOutput, cel.DynType),
		cel.Variable(v1.StepsRoot, cel.DynType),
		cel.Variable(v1.VarsRoot, cel.DynType),
		cel.Variable(v1.InputsRoot, cel.DynType),
		cel.Variable(v1.ResponseRoot, cel.DynType),
	)
	require.NoError(t, err)

	return env
}

// TestEveryDocumentedCELIdiomCompiles is #571's guarantee applied to this
// document: an idiom in docs/reference/cel.md that does not compile against the
// profile it claims to describe is worse advice than no idiom at all, since it is
// the first thing an author copies.
//
// It renders the document rather than reading the committed file, so a change to
// [celIdioms] is checked before anyone runs `flow docs generate` and commits the
// result — the fastest place this can fail is in the same `go test` a doc edit
// already needs to pass. TestGeneratedDocsAreCommitted (in cmd/flow) is what pins
// the rendered bytes to the file this test's failures are actually about.
//
// The anti-vacuity guard is doubled, for the two ways a regex silently reading
// nothing turns this into a test that proves nothing: the fence pattern could stop
// matching the document's own formatting, and [celIdioms] itself could be emptied
// by an edit that forgot to add its replacement back. Comparing the match count
// against len(celIdioms) catches both — a document with fewer fenced examples than
// idioms declared is exactly as wrong as one with none.
func TestEveryDocumentedCELIdiomCompiles(t *testing.T) {
	require.NotEmpty(t, celIdioms, "no idioms are declared, so this test would check nothing")

	rendered := (&Generator{}).renderCELReference()

	matches := celFence.FindAllStringSubmatch(rendered, -1)
	require.Len(t, matches, len(celIdioms),
		"the rendered document has %d fenced ```cel examples but celIdioms declares %d — "+
			"either the fence regex stopped matching the document's formatting, or an idiom "+
			"was added or removed without the other changing to match", len(matches), len(celIdioms))

	env := idiomEnv(t)

	for i, idiom := range celIdioms {
		t.Run(idiom.title, func(t *testing.T) {
			expr := matches[i][1]
			assert.Equal(t, idiom.expr, expr,
				"the fenced block in document order does not match celIdioms in order; "+
					"the two lists have drifted apart")

			_, issues := env.Compile(expr)
			assert.Nil(t, issues.Err(), "idiom %q does not compile against profile %s: %v",
				idiom.title, v1.CurrentProfile, issues.Err())
		})
	}
}
