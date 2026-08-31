package flowfile_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The corpus is the formatter's canon, and this file is what keeps saying so.
//
// #850 measured `flow fmt` against `examples/` and found zero of the workflows
// byte-identical to what it writes: every one was re-indented, re-quoted, or had
// its durations re-spelled. A formatter nothing in the repository agrees with is
// not canon, it is one opinion among the files. The emitter's defaults were
// settled against what the corpus already spelled, the corpus was reformatted
// once, and this is the assertion that lands in the same change so the two can
// never drift apart again.
//
// Three claims, and the order matters because each would hide the next:
//
//  1. every workflow under `examples/` is already exactly what [flowfile.Format]
//     writes for it — the fixed point;
//  2. formatting that output again changes nothing — idempotence, which is the
//     property `flow fmt` on a file twice in a row depends on;
//  3. the formatted document compiles to the *same workflow* — semantic
//     preservation.
//
// All three are checked over bytes and over the compiled tree, never by asking
// whether the result still validates. CLAUDE.md's rewriter section is explicit
// about why: both `flow fix` corruptions on record produced files that validated
// perfectly and computed something else, so "it still validates" is the check
// that let them through.

// exampleWorkflow is one Flowfile from the corpus, with the bytes it holds on
// disk.
type exampleWorkflow struct {
	path   string
	rel    string
	source []byte
}

// corpusWorkflows reads every workflow under examples/.
//
// Which files those are is asked of the same two functions `flow fmt` asks
// rather than answered again here. `examples/` holds YAML that is not a
// Flowfile at all — an auth policy, an egress policy, a compose file — and
// [flowfile.LooksLikeFlowfile] is what the command's own directory walk uses to
// leave them alone; a `*.test.yaml` is a Flowfile of the other document kind,
// which [flowfile.LooksLikeFlowfileTest] separates and `flow fmt` passes over.
// Matching on file names here instead would be a second spelling of the walk,
// free to disagree with the command about which files canon even covers.
func corpusWorkflows(t *testing.T) []exampleWorkflow {
	t.Helper()

	root := filepath.Join("..", "..", "..", "..")
	examples := filepath.Join(root, "examples")

	var out []exampleWorkflow
	require.NoError(t, filepath.WalkDir(examples, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if ext := filepath.Ext(path); ext != ".yaml" && ext != ".yml" {
			return nil
		}

		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !flowfile.LooksLikeFlowfile(source) || flowfile.LooksLikeFlowfileTest(source) {
			return nil
		}

		rel, err := filepath.Rel(examples, path)
		if err != nil {
			return err
		}
		out = append(out, exampleWorkflow{path: path, rel: filepath.ToSlash(rel), source: source})
		return nil
	}))

	sort.Slice(out, func(i, j int) bool { return out[i].rel < out[j].rel })

	// The walk finding nothing is the failure this whole file is least able to
	// notice on its own: a broken path makes every claim below vacuously true.
	// The floor is deliberately well under the corpus's real size and well over
	// zero — it is here to catch a walk that found nothing, not to be a count
	// anybody maintains.
	require.Greater(t, len(out), 40,
		"the walk over examples/ found %d workflows, which is too few to be the corpus — the path is wrong", len(out))
	return out
}

// TestExamplesContainNoCredentialShapedLiterals is deliberately narrower than
// a prose linter. It catches only unmistakable credential formats that should
// never be committed to a Flowfile; placeholders and secret references remain
// valid teaching material.
func TestExamplesContainNoCredentialShapedLiterals(t *testing.T) {
	t.Parallel()

	patterns := map[string]*regexp.Regexp{
		"AWS access key":     regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
		"GitHub token":       regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{20,}`),
		"OpenAI API key":     regexp.MustCompile(`sk-[A-Za-z0-9]{20,}`),
		"PEM private key":    regexp.MustCompile(`-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----`),
		"Slack access token": regexp.MustCompile(`xox[baprs]-[A-Za-z0-9-]{20,}`),
	}

	for _, example := range corpusWorkflows(t) {
		for kind, pattern := range patterns {
			assert.False(t, pattern.Match(example.source),
				"examples/%s contains a credential-shaped %s literal; use a secret reference instead", example.rel, kind)
		}
	}
}

// TestEveryExampleIsAlreadyWhatTheFormatterWrites is claim 1: the corpus is a
// fixed point of the formatter, byte for byte.
//
// Byte equality rather than "it still parses" or "it still validates", because
// the whole content of the claim is the bytes: a corpus that merely means the
// same thing after formatting is a corpus `flow fmt --check` still fails on, and
// the charter's R8 asks for byte-identity precisely so that CI can assert it.
//
// A failure here is one of two things and the message says which to look for: an
// example edited by hand into a shape the formatter does not write (run
// `flow fmt examples/`), or an emitter change that moved canon out from under
// the corpus (which needs the reformat run again, deliberately, in the same
// commit as the change).
func TestEveryExampleIsAlreadyWhatTheFormatterWrites(t *testing.T) {
	t.Parallel()

	for _, example := range corpusWorkflows(t) {
		t.Run(example.rel, func(t *testing.T) {
			t.Parallel()

			workflow, _, err := flowfile.ParseFile(example.path)
			require.NoError(t, err, "the example does not compile, so it says nothing about the formatter")

			formatted, err := flowfile.Format(example.source, workflow)
			require.NoError(t, err,
				"the formatter refuses an example in the corpus it is canon for; a refusal names the position, "+
					"and the fix is the one it names rather than an exception here")

			if !bytes.Equal(formatted, example.source) {
				assert.Equal(t, string(example.source), string(formatted),
					"examples/%s is not what `flow fmt` writes for it. Either the file was hand-edited into a "+
						"shape the formatter does not write — run `go run ./cmd/flow fmt examples/` — or the "+
						"emitter's canonical form moved, which needs that same reformat landed in the same "+
						"commit as the change that moved it", example.rel)
			}
		})
	}
}

// TestFormattingAnExampleTwiceChangesNothing is claim 2, and it is not implied
// by claim 1.
//
// A fixed point says formatting the *corpus* is a no-op; idempotence says
// formatting the formatter's own output is. The two come apart exactly where a
// corpus was reformatted by a version of the emitter that has since moved: every
// file matches the bytes on disk and the second pass moves them anyway. This
// runs the second pass over the output of the first, so the claim is about the
// formatter rather than about what happens to be committed.
func TestFormattingAnExampleTwiceChangesNothing(t *testing.T) {
	t.Parallel()

	for _, example := range corpusWorkflows(t) {
		t.Run(example.rel, func(t *testing.T) {
			t.Parallel()

			workflow, _, err := flowfile.ParseFile(example.path)
			require.NoError(t, err)

			once, err := flowfile.Format(example.source, workflow)
			require.NoError(t, err)

			// Compiled from the formatted bytes at the example's own path, so a
			// `call:` still resolves relative to the directory it was written in.
			again, _, err := flowfile.ParseAt(once, example.path)
			require.NoError(t, err,
				"the formatter wrote a document that no longer compiles, which is the corruption the "+
					"whole command must never produce")

			twice, err := flowfile.Format(once, again)
			require.NoError(t, err)

			assert.Equal(t, string(once), string(twice),
				"formatting examples/%s twice is not the same as formatting it once, so `flow fmt` run on a "+
					"file it just wrote would rewrite it again", example.rel)
		})
	}
}

// TestFormattingAnExampleKeepsTheWorkflowItCompilesTo is claim 3, and it is the
// one that catches a formatter that is stable and wrong.
//
// Bytes that never move say nothing at all about meaning: a rewriter that
// consistently drops a `retry:` block, or reads a bare name as a reference to
// the step of that name — both defects this repository has actually shipped —
// produces a document that is a perfect fixed point of itself. So the tree is
// compared as well, with [proto.Equal] over the compiled workflow, which is the
// same object both execution drivers run.
func TestFormattingAnExampleKeepsTheWorkflowItCompilesTo(t *testing.T) {
	t.Parallel()

	for _, example := range corpusWorkflows(t) {
		t.Run(example.rel, func(t *testing.T) {
			t.Parallel()

			before, _, err := flowfile.ParseFile(example.path)
			require.NoError(t, err)

			formatted, err := flowfile.Format(example.source, before)
			require.NoError(t, err)

			after, _, err := flowfile.ParseAt(formatted, example.path)
			require.NoError(t, err)

			if !proto.Equal(before, after) {
				assert.Equal(t, before.String(), after.String(),
					"formatting examples/%s changed the workflow it compiles to, which is the formatter "+
						"rewriting what the file means rather than how it is written", example.rel)
			}
		})
	}
}

// TestTheCorpusSpellsTheCanonicalDefaults is the readable half of claim 1.
//
// The three defaults #850 left unsettled — duration spelling, scalar quoting and
// sequence indentation — are decided in marshal.go and asserted to the byte
// above, but a byte comparison against a committed file cannot say *which*
// decision it is holding: change the emitter and the corpus together and the
// test above stays green while canon moves. These probe the corpus for the
// spellings themselves, so a silent change of mind has something to fail.
func TestTheCorpusSpellsTheCanonicalDefaults(t *testing.T) {
	t.Parallel()

	var (
		paddedDurations []string
		flushSequences  []string
		escapedQuotes   []string
	)

	for _, example := range corpusWorkflows(t) {
		lines := strings.Split(string(example.source), "\n")
		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") {
				continue
			}

			// `24h0m0s` rather than `24h`: [time.Duration.String]'s padding,
			// which is what the emitter used to write.
			for _, padded := range []string{"h0m0s", "m0s"} {
				if strings.HasSuffix(trimmed, padded) && !strings.HasSuffix(trimmed, "0m0s\"") {
					paddedDurations = append(paddedDurations, example.rel+":"+itoa(i+1)+": "+trimmed)
				}
			}

			// A `- ` at the same column as the key above it, which is the
			// emitter's own default and not the corpus's.
			if strings.HasPrefix(trimmed, "- ") && i > 0 {
				previous := strings.TrimRight(lines[i-1], " ")
				if strings.HasSuffix(previous, ":") && !strings.HasPrefix(strings.TrimSpace(previous), "#") {
					if indentOf(line) == indentOf(previous) {
						flushSequences = append(flushSequences, example.rel+":"+itoa(i+1)+": "+trimmed)
					}
				}
			}

			// `"${\"a\" + b}"` — a scalar the emitter wrapped in double quotes
			// and then had to escape the author's own quotes inside, where the
			// single-quoted style carries them exactly as written.
			//
			// The value has to be read off the line rather than the whole line
			// searched, because a `\"` is also how CEL escapes a quote inside
			// its own string literal, and a *plain* scalar holding one is
			// canonical already: `message: ${"a \"b\" c".format(...)}` is the
			// spelling this rule prefers, not a violation of it.
			if value, ok := valueOnLine(trimmed); ok &&
				strings.HasPrefix(value, `"`) && strings.Contains(value, `\"`) && !strings.Contains(value, `'`) {
				escapedQuotes = append(escapedQuotes, example.rel+":"+itoa(i+1)+": "+trimmed)
			}
		}
	}

	assert.Empty(t, paddedDurations,
		"a duration is written in the padded form `time.Duration.String` produces; canon is the shortest "+
			"exact spelling (`24h`, not `24h0m0s`) — see durationToYAML")
	assert.Empty(t, flushSequences,
		"a block sequence is written flush against the key that holds it; canon indents it — see Marshal's "+
			"IndentSequence")
	assert.Empty(t, escapedQuotes,
		"a scalar is double-quoted with escaped quotes where the single-quoted style would carry them "+
			"verbatim; canon prefers plain, then single-quoted — see scalarStyles")
}

// valueOnLine returns what a `key: value` line writes as its value.
//
// Deliberately conservative: a line whose key is quoted, or that carries no
// `: ` at all, reports nothing rather than being guessed at. This reads the
// corpus for a spelling rather than parsing it — [flowfile.Format]'s own output
// is what the byte assertions above hold — so a line it declines to read is a
// line those assertions still cover.
func valueOnLine(line string) (string, bool) {
	name, value, found := strings.Cut(line, ": ")
	if !found || strings.ContainsAny(name, `"'#`) {
		return "", false
	}
	return strings.TrimSpace(value), true
}

// indentOf is the number of leading spaces on a line.
func indentOf(line string) int { return len(line) - len(strings.TrimLeft(line, " ")) }

// itoa keeps the line numbers in the messages above readable without dragging
// strconv into a file that needs it for nothing else.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for ; n > 0; n /= 10 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
	}
	return string(digits)
}
