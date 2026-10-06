package policycheck

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The scan is called directly, never the parser: a regression that stopped
// counting must cost an assertion, not the machine's memory.
func scanAllocated(t *testing.T, doc string) (uint64, error) {
	t.Helper()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	err := refuseDeepNesting([]byte(doc))

	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc, err
}

// Stray quotes in plain text were once enough to put a quote-aware scan in a mode
// the parser was not in. The scan models no quote, so every one is refused.
func TestScanCountsOpenersWhateverQuotesPrecedeThem(t *testing.T) {
	// Serial: reads process-wide allocation counters.
	openers := strings.Repeat("[", 60000)

	for name, prefix := range map[string]string{
		"an apostrophe in a word": "y: don 't",
		"an unclosed single":      "name: it 'a",
		"an unclosed double":      `say "a`,
		"after a dash":            "x: -'a",
		"a trailing single":       "x: a'",
		"a trailing double":       `x: a"`,
	} {
		t.Run(name, func(t *testing.T) {
			alloc, err := scanAllocated(t, "a: 1\n"+prefix+" "+openers+"\n")
			require.ErrorContains(t, err, "too many flow collections")
			require.Regexp(t, `line 2, column \d+`, err.Error())
			require.Less(t, alloc, uint64(1<<20))
		})
	}
}

func TestScanCountsBracesAndBracketsAlike(t *testing.T) {
	t.Parallel()

	require.Error(t, refuseDeepNesting([]byte(strings.Repeat("{", MaxFlowOpeners+1))))
	require.Error(t, refuseDeepNesting([]byte(strings.Repeat("[{", MaxFlowOpeners/2+1))))
	require.NoError(t, refuseDeepNesting([]byte(strings.Repeat("{", MaxFlowOpeners))))
}

func TestScanCountsOpenersBeforeUnbalancedClosers(t *testing.T) {
	t.Parallel()

	// Closers do not give back budget: depth cannot be recovered by guessing.
	doc := strings.Repeat("]", 100000) + strings.Repeat("[", MaxFlowOpeners+1)
	require.ErrorContains(t, refuseDeepNesting([]byte(doc)), "too many flow collections")

	doc = strings.Repeat("[]", MaxFlowOpeners+1)
	require.ErrorContains(t, refuseDeepNesting([]byte(doc)), "too many flow collections")
}

func TestScanCountsInCommentsQuotesAndBlockScalars(t *testing.T) {
	t.Parallel()

	many := strings.Repeat("[", MaxFlowOpeners+1)
	few := strings.Repeat("[", 100)

	for name, doc := range map[string]string{
		"comment":       "a: 1 # %s\n",
		"whole comment": "# %s\n",
		"double":        "a: \"%s\"\n",
		"single":        "a: '%s'\n",
		"block scalar":  "a: |\n  %s\n",
		"folded":        "a: >\n  %s\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.ErrorContains(t, refuseDeepNesting([]byte(strings.ReplaceAll(doc, "%s", many))), "flow collections")
			require.NoError(t, refuseDeepNesting([]byte(strings.ReplaceAll(doc, "%s", few))))
		})
	}
}

// A document's line breaks are \n, \r\n, or a bare \r, and a BOM may precede it:
// the position and the per-line caps must agree with all of them.
func TestScanLineBreaksAndBOM(t *testing.T) {
	t.Parallel()

	chain := strings.Repeat("- ", MaxBlockTokens+1) + "x"
	deep := strings.Repeat(" ", MaxBlockIndent+1) + "x"

	for name, br := range map[string]string{"LF": "\n", "CRLF": "\r\n", "CR": "\r"} {
		for _, bom := range []string{"", "\xef\xbb\xbf"} {
			doc := bom + "a: 1" + br + "b: 2" + br + chain + br
			err := refuseDeepNesting([]byte(doc))
			require.ErrorContains(t, err, "block indicators", name)
			require.Contains(t, err.Error(), "line 3,", name)

			doc = bom + "a: 1" + br + deep + br
			err = refuseDeepNesting([]byte(doc))
			require.ErrorContains(t, err, "indented too deeply", name)
			require.Contains(t, err.Error(), "line 2,", name)

			// The cap is per line: many short lines are fine.
			doc = strings.Repeat("- a"+br, 10000)
			require.NoError(t, refuseDeepNesting([]byte(doc)), name)
		}
	}

	// \r\n is one break: a CRLF file reports the same lines as an LF one.
	err := refuseDeepNesting([]byte("a\r\nb\r\n" + strings.Repeat(" ", 200)))
	require.Contains(t, err.Error(), "line 3,")
}

func TestScanBlockIndicators(t *testing.T) {
	t.Parallel()

	n := MaxBlockTokens + 1

	for name, doc := range map[string]string{
		"sequence chain":     strings.Repeat("- ", n) + "x\n",
		"explicit key chain": strings.Repeat("? ", n) + "x\n",
		"key chain":          "k: " + strings.Repeat("a: ", n) + "\n",
		"mixed":              strings.Repeat("- ? : ", n/3+1) + "x\n",
		"trailing dash":      strings.Repeat("- ", n-1) + "-\n",
		"trailing colon":     strings.Repeat("a: ", n-1) + "b:\n",
		"tab separated":      strings.Repeat("-\t", n) + "x\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.ErrorContains(t, refuseDeepNesting([]byte(doc)), "block indicators")
		})
	}

	for name, doc := range map[string]string{
		"at the cap":        strings.Repeat("- ", MaxBlockTokens) + "x\n",
		"a plain dash":      "a: -1\nb: a-b\nc: x:y\nd: ?\n",
		"an ordinary entry": "identities:\n  - name: a\n    claims: {k: v}\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.NoError(t, refuseDeepNesting([]byte(doc)))
		})
	}
}

func TestScanIndentation(t *testing.T) {
	t.Parallel()

	require.NoError(t, refuseDeepNesting([]byte(strings.Repeat(" ", MaxBlockIndent)+"x\n")))
	require.ErrorContains(t, refuseDeepNesting([]byte(strings.Repeat(" ", MaxBlockIndent+1)+"x\n")), "indented too deeply")

	for name, doc := range map[string]string{
		"leading tab":     "a:\n\tb: 1\n",
		"tab after space": "a:\n  \tb: 1\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := refuseDeepNesting([]byte(doc))
			require.ErrorContains(t, err, "tab")
			require.Contains(t, err.Error(), "line 2,")
		})
	}

	// A tab after the first token is not indentation.
	require.NoError(t, refuseDeepNesting([]byte("a:\tb\n")))
}

// The last line of a file often has no break after it, and is counted like any
// other: by its in-line indicators and by one it ends on.
func TestScanCountsALastLineWithoutABreak(t *testing.T) {
	t.Parallel()

	n := MaxBlockTokens + 1

	for name, doc := range map[string]string{
		"in line":         "a: 1\n" + strings.Repeat("- ", n) + "x",
		"ends on a colon": "a: 1\n" + strings.Repeat("a: ", n-1) + "b:",
		"ends on a dash":  strings.Repeat("- ", n-1) + "-",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.ErrorContains(t, refuseDeepNesting([]byte(doc)), "block indicators")
		})
	}

	require.NoError(t, refuseDeepNesting([]byte(strings.Repeat("- ", MaxBlockTokens)+"x")))
	require.NoError(t, refuseDeepNesting([]byte("a: 1\nb:")))
}

func TestScanRefusalQuotesNothing(t *testing.T) {
	t.Parallel()

	err := refuseDeepNesting([]byte("secret-token: " + strings.Repeat("[", MaxFlowOpeners+1)))
	require.ErrorContains(t, err, "line 1, column")
	require.NotContains(t, err.Error(), "secret")
}
