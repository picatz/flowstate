package flowfile

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	yaml "github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The grammar refuses an anchor, an alias and a merge key in a *document*. This
// file is about the same constructs one level down, inside a string a document
// legitimately holds — where that refusal does not reach, because at the moment
// it runs the construct is a quoted scalar and nothing more.
//
// It matters because the scalar-style chooser (marshal.go) verifies a candidate
// by writing it and reading it back, and the plain candidate is the string's own
// bytes handed to a YAML parser as a document of its own. Read with a decoder,
// those bytes expand. #889 measured it through the command: a valid 523-byte
// Flowfile whose `message:` held twenty-four nested merge keys took 23.8s in
// `flow fmt`, four times the cost of twenty-two levels and sixteen times the
// cost of twenty — the exponential this repository already knows by name, on a
// path nothing had thought to bound.
//
// Merge keys are what does it. A plain alias bomb of the same depth stays linear
// here because the decoder shares one decoded value between the aliases naming
// it; a merge *splices* a mapping's entries into a new mapping at every level,
// so each level copies everything below it.

// mergeKeyBomb is a CEL-free string whose text is a nested merge-key document:
// `&l2 {p: &l1 {p: &l0 {a: x}, <<: *l0}, <<: *l1}` at depth 3.
//
// Written as a helper rather than a constant so a case can name the depth it
// wants, since the whole property under test is what happens as depth grows.
func mergeKeyBomb(depth int) string {
	text := "&l0 {a: x}"
	for i := 1; i < depth; i++ {
		text = fmt.Sprintf("&l%d {p: %s, <<: *l%d}", i, text, i-1)
	}
	return text
}

// TestTheScalarChooserNeverExpandsWhatItVerifies is the bound, stated where it
// is enforced.
//
// It asserts the refusal rather than a running time, because a time is a
// property of the machine: [expandsWhenRead] declining is what makes the
// exponential unreachable, and a change that removes the gate fails here
// deterministically instead of failing on a slow afternoon.
func TestTheScalarChooserNeverExpandsWhatItVerifies(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		text    string
		expands bool
	}{
		{
			name:    "a merge key, the shape that multiplies",
			text:    mergeKeyBomb(24),
			expands: true,
		},
		{
			// Linear rather than exponential when read, since the decoder
			// shares one value between the aliases — declined all the same,
			// because the gate refuses on the construct's presence rather than
			// on a guess about what this particular shape would cost.
			name:    "an anchor and the aliases naming it",
			text:    "[&a x,*a,*a]",
			expands: true,
		},
		{
			name: "an ordinary expression, which must still be read",
			text: `${"hello, " + inputs.name}`,
		},
		{
			name: "text holding an ampersand that is not an anchor",
			text: "tea & biscuits",
		},
		{
			name: "text holding an asterisk that is not an alias",
			text: "2 * 3 = 6",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Through the same candidate the chooser would try first, in the
			// same one-entry mapping it verifies in, so this is the value the
			// gate actually sees rather than a hand-built one.
			encoded, err := yaml.Marshal(yaml.MapSlice{{Key: "v", Value: plainScalar(test.text)}})
			require.NoError(t, err)

			assert.Equal(t, test.expands, expandsWhenRead(encoded),
				"expandsWhenRead disagrees about whether reading %q back would expand something", test.text)
		})
	}
}

// TestFormatOfAStringHoldingAMergeKeyBombStaysBounded is the end-to-end claim,
// over the path an attacker actually has: a Flowfile.
//
// The file is valid and `flow validate` accepts it — the constructs are inside a
// double-quoted string, so the strict refusal has nothing to report. What is
// asserted is that formatting it costs an amount that does not grow with the
// bomb's depth, and that the document still comes back correct: the string is
// written double-quoted, unchanged, and the workflow still says what it said.
//
// Allocation rather than wall time, because allocation is what the attack
// actually controls and what a shared machine's load does not. The ceiling is
// deliberately generous — the measured cost is under a megabyte and the
// unbounded version allocated 464 MiB at depth 20 alone — so this fails on the
// defect and not on a GC that ran at an awkward moment.
func TestFormatOfAStringHoldingAMergeKeyBombStaysBounded(t *testing.T) {
	// Not parallel: it reads process-wide allocation counters, and a sibling
	// test allocating alongside it would be charged to this one.

	const depth = 24
	bomb := mergeKeyBomb(depth)
	source := fmt.Sprintf(
		"edition: %s\nname: bomb\nsteps:\n  - id: a\n    log:\n      message: %q\n",
		CurrentEdition, bomb)
	require.Less(t, len(source), 1024, "the whole point is that the input is small")

	workflow, _, err := Parse([]byte(source))
	require.NoError(t, err, "the fixture is not a valid Flowfile, so it says nothing about the attack")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()

	formatted, err := Format([]byte(source), workflow)

	runtime.ReadMemStats(&after)
	elapsed := time.Since(start)
	allocated := after.TotalAlloc - before.TotalAlloc

	require.NoError(t, err)

	const ceiling = 32 << 20
	assert.Less(t, allocated, uint64(ceiling),
		"formatting a %d-byte Flowfile allocated %.1f MiB in %s: the merge keys inside its `message:` string "+
			"are being expanded by the scalar chooser's own verification, which is the billion-laughs shape "+
			"one level below the grammar's refusal (see expandsWhenRead)",
		len(source), float64(allocated)/(1<<20), elapsed)

	// The bomb is not refused, only never expanded: it is a legitimate string.
	assert.Equal(t, source, string(formatted),
		"the file is already canonical — the string is double-quoted, which is where the chooser lands once "+
			"the plain candidate is declined")

	again, _, err := Parse(formatted)
	require.NoError(t, err)
	assert.Equal(t, bomb, again.GetSteps()[0].GetTask().GetInputs()["message"].GetLiteral().GetStringValue(),
		"the string came back changed, so declining the plain candidate cost the file its content")
}

// TestEveryDepthOfTheBombCostsTheSame is the bound asserted as *reached* rather
// than merely not exceeded.
//
// A ceiling alone is also satisfied by a chooser that declines everything, or by
// one whose expansion happens to fit at the depth a test picked. Formatting the
// same file at four depths and comparing the costs is what says the cost does
// not grow with the attacker's input: sixteen levels of doubling separate the
// first and last, and the unbounded version differed by 16x in wall time between
// the outer two.
func TestEveryDepthOfTheBombCostsTheSame(t *testing.T) {
	costs := map[int]uint64{}
	for _, depth := range []int{4, 12, 20, 24} {
		bomb := mergeKeyBomb(depth)
		source := fmt.Sprintf(
			"edition: %s\nname: bomb\nsteps:\n  - id: a\n    log:\n      message: %q\n",
			CurrentEdition, bomb)

		workflow, _, err := Parse([]byte(source))
		require.NoError(t, err)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err = Format([]byte(source), workflow)
		runtime.ReadMemStats(&after)
		require.NoError(t, err)

		costs[depth] = after.TotalAlloc - before.TotalAlloc
	}

	// Linear in the text's own length would be a small multiple; exponential is
	// four orders of magnitude by depth 24. Ten is far outside the first and far
	// inside the second.
	assert.Less(t, costs[24], costs[4]*10,
		"formatting cost grows with the bomb's depth (%d bytes at depth 4, %d at depth 24), so something on "+
			"the path is still expanding it", costs[4], costs[24])
}

// TestAStringHoldingAMergeKeyIsStillAValidFlowfile is the premise the two tests
// above rest on, checked rather than assumed.
//
// If the grammar refused this file, there would be no attack and no need for the
// gate — and a future change that *did* refuse it should announce itself here
// rather than by quietly making the tests above vacuous.
func TestAStringHoldingAMergeKeyIsStillAValidFlowfile(t *testing.T) {
	t.Parallel()

	source := fmt.Sprintf(
		"edition: %s\nname: bomb\nsteps:\n  - id: a\n    log:\n      message: %q\n",
		CurrentEdition, mergeKeyBomb(6))

	workflow, _, err := Parse([]byte(source))
	require.NoError(t, err, "the strict profile refuses anchors in a document; this one has none, "+
		"they are inside a string")

	assert.Empty(t, Validate(workflow))

	assert.True(t, strings.Contains(
		workflow.GetSteps()[0].GetTask().GetInputs()["message"].GetLiteral().GetStringValue(), "<<:"),
		"the merge key did not survive parsing, so the fixture is not the one this file is about")
}
