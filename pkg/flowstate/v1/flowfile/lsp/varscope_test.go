package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The editor could not see a `vars:` block, in either of its namespaces.
//
// A step's `vars:` are bare and the workflow's are rooted under `vars.` — the
// crossing [v1.Scope.Activation] describes, and one the grammar has had since
// `vars:` landed at several positions. Completion knew about neither: its locals
// came from loop iterators alone, and `vars.` fell through the qualifier switch
// into the arm that treats an unknown qualifier as a binding and offers nothing.
// So an author who declared a variable two lines above got a menu that did not
// contain it, in the one position where they had just proved they wanted it.
//
// Hover had the sharper version. The rule it states — "a binding is what the
// author wrote and a function of the same spelling is a coincidence" — was right
// and was being decided against a binding set that did not include `vars:`. So a
// step var named `join` fell through to the function fallback and hover described
// the `strings` library, confidently, about a name the engine resolves to the
// author's value. The profile declares thirty-six bare function names and they are
// words people name variables: `value`, `first`, `last`, `or`, `format`, `sort`,
// `replace`, `split`, `trim`.

// varScopeFile is one legal Flowfile exercising every position a `vars:` block has.
//
// `join` deliberately: it is also a function the `strings` library declares, which
// is the collision hover was answering wrongly. PLACEHOLDER is where a cursor goes.
const varScopeFile = `edition: v2026.2
name: var-scope
vars:
  region: eu-west-1
steps:
  - id: each
    vars:
      factor: ${2}
    for_each:
      items: ${[1]}
      as: n
      steps:
        - id: inner
          vars:
            join: ${'a-b-c'}
            tag: ${'v2'}
          log:
            message: PLACEHOLDER
`

// TestTheFileIsLegal is the premise everything below rests on.
//
// Without it, a test asserting the editor offers `join` could be asserting that it
// offers something the engine rejects — which is the defect, not the fix.
func TestTheFileIsLegal(t *testing.T) {
	t.Parallel()

	src := strings.Replace(varScopeFile, "PLACEHOLDER",
		`${join + tag + string(factor) + vars.region + string(n)}`, 1)

	diags, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, diags,
		"the fixture below is meant to be a file the engine accepts, and the validator "+
			"disagrees; every assertion about what the editor should offer depends on this")
}

// TestEveryBindingInScopeIsOffered is the completion half.
func TestEveryBindingInScopeIsOffered(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	got := completeAtPlaceholder(t, c, "file:///var-scope-bare.yaml", "${|}")

	for name, what := range map[string]string{
		// `tag` rather than `join` for the step's own var: `join` is also a function
		// the profile declares, so finding it in this list would prove nothing about
		// whether the binding was offered. Its collision is what the hover case is
		// for, and it is exactly why a completion case cannot use it.
		"tag":    "the step's own var, which is the nearest binding there is",
		"factor": "a var on the enclosing loop, which binds for its whole body",
		"n":      "the loop's iterator",
		"vars":   "the root the workflow's own vars hang from",
		"steps":  "the root every step's outputs hang from",
	} {
		assert.Contains(t, got, name, "completion does not offer %q: %s", name, what)
	}
}

// TestTheWorkflowsVarsAreOfferedAfterTheRoot is the rooted namespace, which was
// reachable by typing it and by nothing else.
func TestTheWorkflowsVarsAreOfferedAfterTheRoot(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	got := completeAtPlaceholder(t, c, "file:///var-scope-rooted.yaml", "${"+v1.VarsRoot+".|}")

	assert.Contains(t, got, "region",
		"`%s.` offers nothing; the file declares one and the validator resolves it", v1.VarsRoot)

	// The two namespaces stay apart, which is the property that makes them two.
	// A step's var is bare, so it must not appear here.
	for _, bare := range []string{"tag", "factor", "n"} {
		assert.NotContains(t, got, bare,
			"%q is bound bare and was offered under `%s.`, where the engine will not find it",
			bare, v1.VarsRoot)
	}
}

// TestTheRootDoesNotOfferAVarsBlockThatIsNotThere keeps the root honest.
//
// `steps` is offered before any step exists, because the name is what an author has
// to learn and the first step is written before there is anything to reference. A
// `vars:` block is different: offering the root for a file that has none teaches a
// name that resolves to an empty map.
func TestTheRootDoesNotOfferAVarsBlockThatIsNotThere(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `edition: v2026.2
name: no-vars
steps:
  - id: only
    log:
      message: ${|}
`

	clean, pos := splitCursor(t, src)
	c.open("file:///no-vars.yaml", clean)
	got := labels(c.complete("file:///no-vars.yaml", pos.Line, pos.Character).Items)

	assert.NotContains(t, got, v1.VarsRoot, "the file declares no vars and the root was offered")
	assert.Contains(t, got, v1.StepsRoot, "the steps root is offered whether or not a step has run")
}

// TestHoverPrefersABindingOverAFunctionOfTheSameName is the hover half, on the
// collision that produced a confidently wrong answer.
func TestHoverPrefersABindingOverAFunctionOfTheSameName(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	// The collision is real rather than contrived: `join` has to be a function the
	// profile declares for this to be testing anything.
	var declared bool
	for _, fn := range v1.ProfileFunctions(v1.CurrentProfile) {
		if fn.Name == "join" {
			declared = true

			break
		}
	}
	require.True(t, declared,
		"the profile no longer declares `join`, so this fixture has no collision in it; "+
			"pick another name the profile has")

	src := strings.Replace(varScopeFile, "PLACEHOLDER", "${join}", 1)
	uri := "file:///var-scope-hover.yaml"
	c.open(uri, src)

	// Two units past the fence, so the cursor rests inside the name.
	at := positionOf(t, src, "${join}", 2)
	h := c.hover(uri, at.Line, at.Character)

	require.NotNil(t, h, "hover says nothing about a name the author bound two lines above")

	var text strings.Builder
	for _, part := range h.Contents {
		text.WriteString(part.Value)
	}

	assert.Contains(t, text.String(), "a variable this step declares",
		"hover describes something other than the binding the engine resolves")
	assert.NotContains(t, text.String(), "strings",
		"hover answered with the `strings` library's function of the same spelling, which is "+
			"not what this expression evaluates to")
}

// completeAtPlaceholder opens the fixture with the placeholder replaced and returns
// the labels offered there.
func completeAtPlaceholder(t *testing.T, c *client, uri, at string) []string {
	t.Helper()

	clean, pos := splitCursor(t, strings.Replace(varScopeFile, "PLACEHOLDER", at, 1))
	c.open(uri, clean)

	return labels(c.complete(uri, pos.Line, pos.Character).Items)
}
