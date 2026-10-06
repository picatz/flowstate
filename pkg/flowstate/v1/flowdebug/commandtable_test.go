package flowdebug_test

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

var updateCommandTable = flag.Bool("update", false, "rewrite docs/DEBUGGING.md's generated command table")

const (
	commandTableStart = "<!-- commands:start -->"
	commandTableEnd   = "<!-- commands:end -->"
)

// TestTheDebuggingDocCommandTableIsTheTable fails when the table in
// docs/DEBUGGING.md is not the one the command table renders, so the document
// cannot teach a verb a front does not answer: regenerate with
// `go test ./pkg/flowstate/v1/flowdebug -run TestTheDebuggingDocCommandTableIsTheTable -update`.
func TestTheDebuggingDocCommandTableIsTheTable(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "docs", "DEBUGGING.md")
	// Required, not skipped: the document is part of the repository contract.
	raw, err := os.ReadFile(path)
	require.NoError(t, err, "docs/DEBUGGING.md holds the generated command table this test checks")
	doc := string(raw)

	start, end := strings.Index(doc, commandTableStart), strings.Index(doc, commandTableEnd)
	require.True(t, start >= 0 && end > start, "docs/DEBUGGING.md needs %s then %s around the generated table", commandTableStart, commandTableEnd)
	want := doc[:start+len(commandTableStart)] + "\n\n" + flowdebug.CommandTableMarkdown() + "\n" + doc[end:]

	if *updateCommandTable {
		require.NoError(t, os.WriteFile(path, []byte(want), 0o644))

		return
	}
	assert.Equal(t, want, doc, "the command table in docs/DEBUGGING.md is not what the command table renders; regenerate it with -update")
}

// TestEveryVerbIsAcceptedOrRefusedByNameOnEveryFront walks the whole table
// against each front that reads command lines. A verb a front answers is never
// called unknown, and one it does not is refused with the sentence the table
// gives, naming what to type instead: "unknown command" sends an author looking
// for a misspelling that is not there.
func TestEveryVerbIsAcceptedOrRefusedByNameOnEveryFront(t *testing.T) {
	t.Parallel()

	held := &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}

	for _, entry := range flowdebug.Vocabulary() {
		for _, spelling := range entry.Spellings {
			t.Run("driver/"+spelling, func(t *testing.T) {
				t.Parallel()

				result, err := flowdebug.NewDriver(&scriptedTarget{snapshot: held}).Do(t.Context(), spelling)
				answer := ""
				if err != nil {
					answer = err.Error()
				} else if result != nil {
					answer = result.Text
				}
				assert.NotContains(t, answer, "unknown command", "%q is in the vocabulary", spelling)
				if entry.Driver {
					assert.False(t, entry.Elsewhere != "" && answer == entry.Elsewhere, "the driver answers %q, so it must not refuse it as another front's", spelling)

					return
				}
				require.Error(t, err, "%q is not a driver verb, so the driver refuses it", spelling)
				assert.Equal(t, entry.Elsewhere, err.Error())
			})

			t.Run("prompt/"+spelling, func(t *testing.T) {
				t.Parallel()

				// `continue` after it, so a verb the prompt answers by moving
				// or leaving cannot strand the run.
				out, _, _ := runDebugged(t, spelling+"\ncontinue\n", flowdebug.Options{})
				assert.NotContains(t, out, "unknown command", "%q is in the vocabulary", spelling)
				if !entry.Prompt {
					assert.Contains(t, out, entry.Elsewhere, "the prompt does not answer %q, so it names where it does", spelling)
				}
			})

			t.Run("script/"+spelling, func(t *testing.T) {
				t.Parallel()

				problems, _ := flowdebug.CheckScript([]string{spelling}, nil)
				for _, p := range problems {
					assert.NotContains(t, p.Message, "unknown command", "%q is in the vocabulary", spelling)
				}
				if !entry.Prompt {
					require.NotEmpty(t, problems, "a script is the prompt's stream, so a verb the prompt refuses is refused before the run")
					assert.Equal(t, entry.Elsewhere, problems[0].Message)
				}
			})
		}
	}
}

// TestADriverAndAPromptSayTheSameThingsAboutAVerb: each front's `help` lists
// exactly the verbs that front answers, and nothing it refuses.
func TestADriverAndAPromptSayTheSameThingsAboutAVerb(t *testing.T) {
	t.Parallel()

	prompt, _, _ := runDebugged(t, "help\ncontinue\n", flowdebug.Options{})
	for _, entry := range flowdebug.Vocabulary() {
		assert.Equal(t, entry.Prompt, helpListsVerb(prompt, entry.Verb),
			"the prompt's help and its dispatch disagree about %q", entry.Verb)
		assert.Equal(t, entry.Driver, helpListsVerb(flowdebug.DriverHelp, entry.Verb),
			"the driver's help and its dispatch disagree about %q", entry.Verb)
	}
}

// helpListsVerb reports whether a help text has a line that names verb as its
// first word. The prompt is written without a newline after it, so it is read
// as the line break it is to a reader.
func helpListsVerb(help, verb string) bool {
	for line := range strings.SplitSeq(strings.ReplaceAll(help, flowdebug.Prompt, "\n"), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && strings.TrimRight(fields[0], ",") == verb {
			return true
		}
	}

	return false
}

// TestAVerbNotOnEveryFrontSaysWhereItIs: the sentence a front refuses with is
// the whole of the guidance an author gets, so a verb that is not everywhere has
// one, and a verb that is everywhere has none to give.
func TestAVerbNotOnEveryFrontSaysWhereItIs(t *testing.T) {
	t.Parallel()

	for _, entry := range flowdebug.Vocabulary() {
		everywhere := entry.Prompt && entry.Driver && entry.Autopsy
		onlyAutopsyMissing := entry.Prompt && entry.Driver && !entry.Autopsy
		switch {
		case everywhere, onlyAutopsyMissing:
			// The autopsy refuses a verb the prompt answers as having nothing to
			// act on, in words of its own.
			assert.Empty(t, entry.Elsewhere, "%q is on both live fronts, so no front refuses it", entry.Verb)
		default:
			assert.NotEmpty(t, entry.Elsewhere, "%q is not on every front and does not say where it is", entry.Verb)
			assert.Contains(t, entry.Elsewhere, "`"+entry.Verb+"`", "the refusal names the verb it refuses")
		}
	}
}
