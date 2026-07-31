package lsp

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A scoping rule can be right at one nesting depth and wrong at the next.
//
// `TestReferenceScoping` asserts the sibling-branch rule at depth 1 and holds. The
// same file at depth 4 offered a branch its sibling's step ids, because the scope
// slice each step keeps was appended to in place: `append(scope, frame)` on a slice
// with spare capacity gives two siblings one backing array, and the second frame
// overwrites the first *after* the first has been stored.
//
// So it depends on spare capacity, which follows append's growth — clean at 2, 3, 5
// and 9, broken at 4, 6, 7, 8 and everything from 10 up. Any single depth is a coin
// toss; the rule is a claim about all of them.
//
// These walk the depths for that reason, and check the answer against the validator
// on the same file rather than against a list written here: the point of the whole
// package is that the editor offers what the engine accepts, so the two must be
// asked the same question.

// maxScopeDepth is how deep these nest.
//
// Past the first reallocation the pattern repeats, and twelve covers four full
// growth steps — enough that a fix which only moves the boundary fails rather than
// passing by landing on safe depths.
const maxScopeDepth = 12

// TestSiblingBranchesStayInvisibleAtEveryDepth is the sibling-branch rule, asked at
// each depth rather than at one.
func TestSiblingBranchesStayInvisibleAtEveryDepth(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	for depth := 1; depth <= maxScopeDepth; depth++ {
		t.Run("depth "+strconv.Itoa(depth), func(t *testing.T) {
			text := nestedBranches(depth, "${steps.|")
			clean, pos := splitCursor(t, text)

			uri := "file:///branch-depth-" + strconv.Itoa(depth) + ".yaml"
			c.open(uri, clean)
			got := labels(c.complete(uri, pos.Line, pos.Character).Items)

			assert.NotContains(t, got, "sibling_a",
				"the editor offered a step in the sibling branch; branches are unordered, so "+
					"referencing one from the other is what the validator refuses")
		})
	}
}

// TestTheValidatorRefusesWhatCompletionMustNotOffer pins the other side of it.
//
// Without this, a fix that made completion offer nothing anywhere would pass the
// test above. What makes `sibling_a` wrong is that the engine rejects it, so that
// is asserted directly — and at every depth, since the aliasing changed which
// frames a step carried and could in principle have moved the validator too.
func TestTheValidatorRefusesWhatCompletionMustNotOffer(t *testing.T) {
	t.Parallel()

	for depth := 1; depth <= maxScopeDepth; depth++ {
		t.Run("depth "+strconv.Itoa(depth), func(t *testing.T) {
			t.Parallel()

			refers := nestedBranches(depth, "${steps.sibling_a.result}")

			diags, err := flowfile.ValidateSource([]byte(refers))
			require.NoError(t, err, "the generated fixture does not parse")
			require.NotEmpty(t, diags,
				"the validator accepted a reference across sibling branches, so there is "+
					"nothing for completion to be wrong about")

			var messages []string
			for _, d := range diags {
				messages = append(messages, d.Message)
			}
			assert.Contains(t, strings.Join(messages, "\n"), `unknown step "sibling_a"`)
		})
	}
}

// TestALoopBodyStaysInsideItsLoopAtEveryDepth is the same defect through the other
// call site.
//
// Two sibling loops share the slot exactly as two branches do, so at depth 4 one
// loop body was offered a *different* loop body's step ids. Worth its own case
// because the two sites are two lines and a fix applied to one of them would leave
// this passing at depth 1 and failing at depth 4.
func TestALoopBodyStaysInsideItsLoopAtEveryDepth(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	for depth := 1; depth <= maxScopeDepth; depth++ {
		t.Run("depth "+strconv.Itoa(depth), func(t *testing.T) {
			text := nestedLoops(depth, "${steps.|")
			clean, pos := splitCursor(t, text)

			uri := "file:///loop-depth-" + strconv.Itoa(depth) + ".yaml"
			c.open(uri, clean)
			got := labels(c.complete(uri, pos.Line, pos.Character).Items)

			assert.NotContains(t, got, "body_a",
				"the editor offered a step from another loop's body; a loop body's outputs do "+
					"not escape the loop")
		})
	}
}

// nestedBranches returns a Flowfile whose innermost `parallel:` has two branches:
// one holding `sibling_a`, one holding `sibling_b` with the given message.
//
// The nesting above it is single-branch, so the only sibling relationship in the
// file is the one under test — anything offered from the other branch got there
// through the bug rather than through a rule this does not model.
func nestedBranches(depth int, message string) string {
	var b strings.Builder
	b.WriteString("edition: v2026.2\nname: branch-depth\nsteps:\n")

	// Two spaces per level for the `- id:` and its `parallel:`, plus the branch's
	// own `- steps:` — the shape below repeats exactly, so the indent is arithmetic
	// rather than a table.
	indent := "  "
	for level := 1; level <= depth; level++ {
		fmt.Fprintf(&b, "%s- id: block_%d\n", indent, level)
		fmt.Fprintf(&b, "%s  parallel:\n", indent)
		fmt.Fprintf(&b, "%s    - steps:\n", indent)
		indent += "        "
	}

	fmt.Fprintf(&b, "%s- id: sibling_a\n", indent)
	fmt.Fprintf(&b, "%s  log:\n", indent)
	fmt.Fprintf(&b, "%s    message: hi\n", indent)

	// The second branch of the innermost block, which is where the cursor goes.
	// Its `- steps:` aligns with the first branch's, four spaces back from where
	// the loop above left the indent.
	branchIndent := indent[:len(indent)-4]
	fmt.Fprintf(&b, "%s- steps:\n", branchIndent)
	fmt.Fprintf(&b, "%s- id: sibling_b\n", indent)
	fmt.Fprintf(&b, "%s  log:\n", indent)
	fmt.Fprintf(&b, "%s    message: %s\n", indent, message)

	return b.String()
}

// nestedLoops returns a Flowfile with two sibling `for_each:` blocks at the given
// nesting depth, the second one's body holding the message.
//
// Sibling *blocks* rather than sibling branches: the frame a loop pushes for its
// body is written to the same slot its neighbour used, so this reaches the defect
// through the for_each call site instead of the parallel one.
func nestedLoops(depth int, message string) string {
	var b strings.Builder
	b.WriteString("edition: v2026.2\nname: loop-depth\nsteps:\n")

	indent := "  "
	for level := 1; level < depth; level++ {
		fmt.Fprintf(&b, "%s- id: outer_%d\n", indent, level)
		fmt.Fprintf(&b, "%s  for_each:\n", indent)
		fmt.Fprintf(&b, "%s    items: ${['x']}\n", indent)
		fmt.Fprintf(&b, "%s    as: n%d\n", indent, level)
		fmt.Fprintf(&b, "%s    steps:\n", indent)
		indent += "      "
	}

	for _, loop := range []struct{ id, body, message string }{
		{id: "loop_a", body: "body_a", message: "hi"},
		{id: "loop_b", body: "body_b", message: message},
	} {
		fmt.Fprintf(&b, "%s- id: %s\n", indent, loop.id)
		fmt.Fprintf(&b, "%s  for_each:\n", indent)
		fmt.Fprintf(&b, "%s    items: ${['x']}\n", indent)
		fmt.Fprintf(&b, "%s    as: item_%s\n", indent, loop.id)
		fmt.Fprintf(&b, "%s    steps:\n", indent)
		fmt.Fprintf(&b, "%s      - id: %s\n", indent, loop.body)
		fmt.Fprintf(&b, "%s        log:\n", indent)
		fmt.Fprintf(&b, "%s          message: %s\n", indent, loop.message)
	}

	return b.String()
}
