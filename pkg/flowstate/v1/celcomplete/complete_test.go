package celcomplete_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
)

// The rules these pin are the ones two surfaces now share, so a test here is a
// claim about the *language* rather than about an editor: where a name may be
// written, what a dot reaches, and what is offered before what.

// scope is a small scope with all three roots and one binding.
func scope() celcomplete.Scope {
	return celcomplete.Scope{
		Profile: v1.CurrentProfile,
		Locals: []celcomplete.Candidate{
			{Name: "item", Kind: celcomplete.KindValue, Detail: "loop item"},
		},
		Roots: []celcomplete.Candidate{
			celcomplete.StepsRoot([]celcomplete.Candidate{
				{Name: "build", Kind: celcomplete.KindValue, Members: []celcomplete.Candidate{
					{Name: "artifact", Kind: celcomplete.KindField},
					{Name: "digest", Kind: celcomplete.KindField},
				}},
				{Name: "deploy", Kind: celcomplete.KindValue},
			}),
			celcomplete.VarsRoot([]celcomplete.Candidate{{Name: "region", Kind: celcomplete.KindValue}}),
			celcomplete.InputsRoot([]celcomplete.Candidate{{Name: "version", Kind: celcomplete.KindValue}}),
		},
	}
}

// names is what a result offers, in order.
func names(result celcomplete.Result) []string {
	out := make([]string, 0, len(result.Candidates))
	for _, c := range result.Candidates {
		out = append(out, c.Name)
	}

	return out
}

// TestTheBareListIsBindingsThenRootsThenFunctions pins the ordering decision,
// which is the whole of what a bare list is for: somebody who does not know the
// name they want must not have to scroll past sixty functions to find the loop
// variable they bound two lines up.
func TestTheBareListIsBindingsThenRootsThenFunctions(t *testing.T) {
	t.Parallel()

	offered := names(celcomplete.Complete("", scope()))
	require.Greater(t, len(offered), 4, "the profile's functions should be in here too")

	assert.Equal(t, []string{"item", v1.StepsRoot, v1.VarsRoot, v1.InputsRoot}, offered[:4],
		"the binding is nearest, then the roots in the order the scope named them")

	assert.Contains(t, offered[4:], "join", "and then the profile's functions")
}

// TestARootIsWrittenWithTheDotThatContinuesIt: a root is never the whole of a
// reference, so accepting one has to leave the cursor after the dot rather than
// leaving one character for somebody to guess at.
func TestARootIsWrittenWithTheDotThatContinuesIt(t *testing.T) {
	t.Parallel()

	for _, want := range []string{v1.StepsRoot, v1.VarsRoot, v1.InputsRoot} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()

			result := celcomplete.Complete(want, scope())
			require.Len(t, result.Candidates, 1)

			assert.Equal(t, want+".", result.Candidates[0].Text())
			assert.True(t, result.Candidates[0].Continues(),
				"a surface must know not to write a space after it")
		})
	}
}

// TestADotReachesThreeLevelsAndNoFourth walks the depth a rooted reference has.
//
// The fourth level is the one worth a test: selecting into a value whose shape
// nothing here describes has no honest answer, and guessing at one is how an
// editor starts offering references the engine rejects.
func TestADotReachesThreeLevelsAndNoFourth(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"build", "deploy"}, names(celcomplete.Complete("steps.", scope())),
		"the root reaches the steps")
	assert.Equal(t, []string{"artifact", "digest"}, names(celcomplete.Complete("steps.build.", scope())),
		"a step reaches its outputs")
	assert.Empty(t, names(celcomplete.Complete("steps.build.artifact.", scope())),
		"and an output reaches nothing this knows about")
	assert.Empty(t, names(celcomplete.Complete("steps.nosuch.", scope())),
		"nor does a step that is not in scope")
}

// TestEachRootReachesOnlyItsOwnNames is the negative direction of the two
// namespaces the grammar keeps apart: a step called `region` and a var called
// `region` are different things, and the mistake worth testing is not that each
// root answers but that neither answers for the other.
func TestEachRootReachesOnlyItsOwnNames(t *testing.T) {
	t.Parallel()

	steps := names(celcomplete.Complete("steps.", scope()))
	vars := names(celcomplete.Complete("vars.", scope()))
	inputs := names(celcomplete.Complete("inputs.", scope()))

	assert.Equal(t, []string{"region"}, vars)
	assert.Equal(t, []string{"version"}, inputs)

	for _, name := range append(append([]string{}, vars...), inputs...) {
		assert.NotContains(t, steps, name, "`steps.` must not reach another root's names")
	}
	for _, name := range steps {
		assert.NotContains(t, vars, name, "`vars.` must not reach a step")
		assert.NotContains(t, inputs, name, "`inputs.` must not reach a step")
	}

	assert.Empty(t, names(celcomplete.Complete("item.", scope())),
		"and a bare binding's element type is not known, so it reaches nothing")
}

// TestAProfileNamespaceIsReachedAfterItsDot covers the other kind of qualifier.
func TestAProfileNamespaceIsReachedAfterItsDot(t *testing.T) {
	t.Parallel()

	offered := names(celcomplete.Complete("math.", scope()))
	require.NotEmpty(t, offered, "`math` is a namespace this profile declares")

	assert.Contains(t, offered, "greatest",
		"the spelling the validator accepts, which is the one to offer")
}

// TestARootWinsAQualifierAFunctionNamespaceWouldClaim states an ordering that
// is invisible until a file happens to name a var after a namespace.
//
// The scope's roots are the language's own and are answered first; a function
// qualifier is only reached where no root claims the name. Reversed, a workflow
// with a `vars:` block would find `vars.` answering with functions.
func TestARootWinsAQualifierAFunctionNamespaceWouldClaim(t *testing.T) {
	t.Parallel()

	shadowing := scope()
	shadowing.Roots = append(shadowing.Roots, celcomplete.Candidate{
		Name:    "math",
		Kind:    celcomplete.KindRoot,
		Insert:  "math.",
		Members: []celcomplete.Candidate{{Name: "mine", Kind: celcomplete.KindValue}},
	})

	assert.Equal(t, []string{"mine"}, names(celcomplete.Complete("math.", shadowing)))
}

// TestThePrefixIsWhatACandidateReplaces: a surface writes the candidate over
// the prefix, so a prefix that is wrong by one character eats the dot before it.
func TestThePrefixIsWhatACandidateReplaces(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		typed, prefix string
		offers        []string
	}{
		{typed: "it", prefix: "it", offers: []string{"item"}},
		{typed: "steps.", prefix: "", offers: []string{"build", "deploy"}},
		{typed: "steps.bu", prefix: "bu", offers: []string{"build"}},
		{typed: "steps.build.art", prefix: "art", offers: []string{"artifact"}},
		// Everything that is not a name character ends the word, which is what
		// makes a reference *inside* an expression complete against the
		// reference. The offers are asserted as well as the prefix, because a
		// word that ran back too far keeps the same prefix and silently stops
		// finding the root: `size(steps` is not a qualifier anything answers.
		{typed: "size(steps.bu", prefix: "bu", offers: []string{"build"}},
		{typed: "a + steps.build.dig", prefix: "dig", offers: []string{"digest"}},
		{typed: "[1, 2].map(x, steps.", prefix: "", offers: []string{"build", "deploy"}},
		{typed: "!vars.", prefix: "", offers: []string{"region"}},
	} {
		t.Run(fmt.Sprintf("%q", tc.typed), func(t *testing.T) {
			t.Parallel()

			result := celcomplete.Complete(tc.typed, scope())

			assert.Equal(t, tc.prefix, result.Prefix)
			assert.Equal(t, tc.offers, names(result))
		})
	}
}

// TestOnlyMatchingNamesAreOffered pins the filter.
func TestOnlyMatchingNamesAreOffered(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"build"}, names(celcomplete.Complete("steps.bu", scope())))
	assert.Empty(t, names(celcomplete.Complete("steps.zz", scope())))
}

// TestTheAnswerIsBoundedAndSaysSo.
//
// The count grows with the document or the run rather than with what was typed,
// and both are somebody else's choice. Asserting the bound was *reached* as well
// as not exceeded, because `<= MaxCandidates` is also satisfied by an answer
// that gave up after one (CLAUDE.md).
func TestTheAnswerIsBoundedAndSaysSo(t *testing.T) {
	t.Parallel()

	many := make([]celcomplete.Candidate, 0, celcomplete.MaxCandidates*2)
	for i := range celcomplete.MaxCandidates * 2 {
		many = append(many, celcomplete.Candidate{Name: fmt.Sprintf("step%04d", i)})
	}

	huge := celcomplete.Scope{Roots: []celcomplete.Candidate{celcomplete.StepsRoot(many)}}
	result := celcomplete.Complete("steps.", huge)

	assert.Len(t, result.Candidates, celcomplete.MaxCandidates, "the bound is reached")
	assert.True(t, result.Truncated, "and admitted, so a short list is not mistaken for a whole one")
}

// TestAnUnknownProfileOffersNoFunctions is the fail-closed direction: a
// specification compiled by a build this one does not know has a vocabulary
// this one cannot enumerate, and inventing one would offer names that run
// cannot evaluate.
func TestAnUnknownProfileOffersNoFunctions(t *testing.T) {
	t.Parallel()

	unknown := scope()
	unknown.Profile = "a-profile-from-the-future"

	offered := names(celcomplete.Complete("", unknown))

	assert.Equal(t, []string{"item", v1.StepsRoot, v1.VarsRoot, v1.InputsRoot}, offered,
		"the scope's own names, and nothing claimed about a vocabulary this build has never seen")
	assert.Empty(t, celcomplete.FunctionCandidates("a-profile-from-the-future"))
}

// TestEveryFunctionOfferedIsSpelledTheWayItIsWritten is the claim `lsp`'s
// macrospelling tests make about the editor, restated where the list is built:
// a macro's name is not its call form, so a bare list holding one would be
// completing a name straight into a diagnostic.
func TestEveryFunctionOfferedIsSpelledTheWayItIsWritten(t *testing.T) {
	t.Parallel()

	for _, candidate := range celcomplete.FunctionCandidates(v1.CurrentProfile) {
		if candidate.Kind == celcomplete.KindNamespace {
			continue
		}
		t.Run(candidate.Name, func(t *testing.T) {
			t.Parallel()

			assert.False(t, strings.Contains(candidate.Detail, "macro"),
				"a macro is written on its namespace, so it belongs after a dot and not in the bare list")
		})
	}
}
