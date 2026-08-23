package flowtest

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTranscriptBoundsRetainedOutputBytes pins the round-six P1 on #1052: the
// event count alone reads as a bound while each event may retain megabytes of
// cloned outputs — the resource an untrusted submission actually controls. A
// recorder past the byte budget drops the event and marks the account
// truncated, exactly as past the event count.
func TestTranscriptBoundsRetainedOutputBytes(t *testing.T) {
	t.Parallel()

	r := newRunRecorder(v1.NewVirtualClock(epoch))

	big := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"blob": v1.NewLiteral(strings.Repeat("x", 1<<20)),
	}}
	for i := 0; i < 32; i++ {
		r.StepFinished("bulky", big, nil, false)
	}

	require.True(t, r.bytesFull, "past the byte budget the account must say it is incomplete")
	require.Less(t, len(r.events), 32, "events past the budget are dropped, not retained")
	require.LessOrEqual(t, r.outputBytes, maxTranscriptOutputBytes)

	lines := r.render()
	require.NotEmpty(t, lines)
	last := lines[len(lines)-1].Text
	assert.Contains(t, last, "truncated")
	assert.Contains(t, last, "step-output bound",
		"the truncation line names the bound that stopped the account, not the one that did not")
	assert.NotContains(t, last, "event bound")
}

// TestFailureTextIsWithheldWhenTheSetCouldNotBeBuilt pins round seven's P1
// on #1052: withholdAll means no text is provably safe, and a failure
// message can embed the very value the walk could not enumerate — so the
// failure line withholds rather than applying an empty substring list.
func TestFailureTextIsWithheldWhenTheSetCouldNotBeBuilt(t *testing.T) {
	t.Parallel()

	text, tone := stepOutcomeText(
		transcriptEvent{kind: eventStepFinished, step: "boom", failure: "expression refused secret-material-here"},
		sensitiveInputs{withholdAll: true},
		map[string]switchFact{},
	)
	assert.Equal(t, ToneDanger, tone)
	assert.NotContains(t, text, "secret-material-here")
	assert.Contains(t, text, "[failure withheld")
}

// TestByteBudgetLatchesLikeTheEventBound pins round eight's P2 on #1052: an
// account that kept recording after dropping an event would carry a hole in
// its middle while claiming the run "continued unrecorded" — once either
// bound trips, nothing further is recorded, so the account is always a
// truncated prefix.
func TestByteBudgetLatchesLikeTheEventBound(t *testing.T) {
	t.Parallel()

	r := newRunRecorder(v1.NewVirtualClock(epoch))
	r.StepFinished("first", &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"blob": v1.NewLiteral(strings.Repeat("x", maxTranscriptOutputBytes)),
	}}, nil, false)
	require.True(t, r.bytesFull)
	before := len(r.events)

	r.StepSkipped("later")
	r.StepFinished("small", &v1.Node_Outputs{}, nil, false)
	require.Equal(t, before, len(r.events),
		"nothing records past a tripped bound; a hole mid-account would belie the truncation line")
}

// TestAFirstEventOverBudgetStillRendersTheTruncationLine pins round
// eleven's finding: an account emptied by its own bound must still say so —
// zero events with a tripped latch renders the truncation sentence, never
// nothing.
func TestAFirstEventOverBudgetStillRendersTheTruncationLine(t *testing.T) {
	t.Parallel()

	r := newRunRecorder(v1.NewVirtualClock(epoch))
	r.StepFinished("giant", &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"blob": v1.NewLiteral(strings.Repeat("x", maxTranscriptOutputBytes+1)),
	}}, nil, false)

	require.True(t, r.bytesFull)
	require.Empty(t, r.events)

	lines := r.render()
	require.Len(t, lines, 1, "the discarded account's one line is the sentence explaining the discard")
	require.Contains(t, lines[0].Text, "truncated")
}

// TestFailureTextCountsAgainstTheByteBudget pins round nine's first P1: a
// tolerated failure's message is a string the event retains just as surely
// as cloned outputs, and a budget counting only protobuf bytes let repeated
// large messages hold gigabytes under an 8 MiB label.
func TestFailureTextCountsAgainstTheByteBudget(t *testing.T) {
	t.Parallel()

	r := newRunRecorder(v1.NewVirtualClock(epoch))
	huge := errors.New(strings.Repeat("e", 1<<20))
	for i := 0; i < 32; i++ {
		r.StepFinished("shrugged", nil, huge, true)
	}

	require.True(t, r.bytesFull, "retained failure strings must trip the same budget outputs do")
	require.Less(t, len(r.events), 32)
}

// TestShortSensitiveKeyRedactsInKeyPosition pins round nine's second P1: a
// sensitive struct key below the substring floor ("zq") still redacts where
// it is rendered as a key, by exact match — the floor exists to stop global
// shredding, not to exempt key positions.
func TestShortSensitiveKeyRedactsInKeyPosition(t *testing.T) {
	t.Parallel()

	sensitive := sensitiveInputs{values: []any{"zq"}}
	text, _ := stepOutcomeText(transcriptEvent{
		kind: eventStepFinished,
		step: "use",
		outputs: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"zq":   v1.NewLiteral("ok"),
			"kept": v1.NewLiteral("visible"),
		}},
	}, sensitive, map[string]switchFact{})

	require.NotContains(t, text, "zq")
	require.Contains(t, text, `[redacted]: "ok"`)
	require.Contains(t, text, `kept: "visible"`)
}

// TestOverlappingSensitiveSubstringsRedactWhole pins round eight's P1: with
// secrets `abcd` and `abcdef`, replacing the shorter first splits the longer
// into `[redacted]ef` — a partial leak decided by map iteration order. The
// replacement site orders longest-first, whatever order the set arrives in.
func TestOverlappingSensitiveSubstringsRedactWhole(t *testing.T) {
	t.Parallel()

	for _, order := range [][]string{
		{"abcd", "abcdef"},
		{"abcdef", "abcd"},
	} {
		got := redactSensitiveSubstrings("token abcdef here", order)
		require.Equal(t, "token [redacted] here", got,
			"order %v must not leak a suffix of the longer secret", order)
	}
}

// TestIntersectingSensitiveSubstringsRedactWhole pins round fourteen's P1:
// two secrets that intersect without containment — `ABCDE` and `CDEFG`
// across derived text `ABCDEFG` — leak a fragment under sequential
// replacement in either order. The union of matches has no order to get
// wrong; self-overlapping matches are covered by the same union.
func TestIntersectingSensitiveSubstringsRedactWhole(t *testing.T) {
	t.Parallel()

	for _, order := range [][]string{
		{"ABCDE", "CDEFG"},
		{"CDEFG", "ABCDE"},
	} {
		got := redactSensitiveSubstrings("xx ABCDEFG yy", order)
		require.Equal(t, "xx [redacted] yy", got,
			"order %v must not leak either secret's fragment", order)
	}

	require.Equal(t, "[redacted]", redactSensitiveSubstrings("aaa", []string{"aa"}),
		"self-overlapping matches all enter the union")
}

// TestSuiteTranscriptBudgetDropsWithASentence pins round thirteen's P1:
// per-case bounds do not compose, so the suite doles a whole-run budget out
// in case order, and an account past it becomes the one line explaining the
// drop — never a silent absence, never unbounded retention.
func TestSuiteTranscriptBudgetDropsWithASentence(t *testing.T) {
	t.Parallel()

	b := &suiteTranscriptBudget{remaining: 20}

	kept := b.take([]TranscriptLine{{Text: "0123456789"}})
	require.Len(t, kept, 1)
	require.Equal(t, "0123456789", kept[0].Text)

	dropped := b.take([]TranscriptLine{{Text: strings.Repeat("x", 11)}})
	require.Len(t, dropped, 1)
	require.Contains(t, dropped[0].Text, "account dropped")
	require.Equal(t, ToneWarning, dropped[0].Tone)

	require.Nil(t, b.take(nil), "a case with no account stays a case with no account")
}

// TestNonStringSensitiveScalarsJoinTheSubstringBackstop pins round fifteen's
// P1: a `sensitive: true` integer converted to text — `${string(inputs.pin)}`
// — matches neither the typed equality nor a string-only substring set, so
// its canonical rendering joins the backstop under the same floor and root
// exemption a string descendant gets.
func TestNonStringSensitiveScalarsJoinTheSubstringBackstop(t *testing.T) {
	t.Parallel()

	set := sensitiveNativeValues(&v1.Scope{Inputs: map[string]*v1.Value{
		"pin": v1.NewLiteral(int64(8231)),
	}}, map[string]bool{"pin": true})

	require.Contains(t, set.substrings, "8231",
		"the number's canonical text must be replaceable wherever a conversion strands it in a string")
	require.Equal(t, "code [redacted] here", redactSensitiveSubstrings("code 8231 here", set.substrings))
}

// TestNestedSensitiveKeysRedact pins round ten's P1: redactSensitiveTree
// redacted values at every depth but preserved map keys, so a sensitive key
// nested inside an output's structured value printed — including one below
// the substring floor. Keys now redact by exact match at every level, in the
// one shared walk.
func TestNestedSensitiveKeysRedact(t *testing.T) {
	t.Parallel()

	got := redactSensitiveTree(map[string]any{
		"outer": map[string]any{"zq": "v", "kept": "w"},
	}, []any{"zq"})

	outer, ok := got.(map[string]any)["outer"].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, outer, "zq")
	require.Contains(t, outer, sensitiveMarker)
	require.Contains(t, outer, "kept")
}

// TestTranscriptLinesEscapeControlRunes pins round ten's P2: a scripted
// sender subject carrying a newline or an ANSI escape must not fabricate
// apparent transcript entries or restyle the terminal — a TranscriptLine is
// one physical line, enforced at the one point every line is formed.
func TestTranscriptLinesEscapeControlRunes(t *testing.T) {
	t.Parallel()

	line := transcriptLine(0, 4, "gate", "sender: evil\nFAKE PASS line \x1b[32mgreen", ToneInfo)
	require.NotContains(t, line.Text, "\n")
	require.NotContains(t, line.Text, "\x1b")
	require.Contains(t, line.Text, `\n`)
	require.Contains(t, line.Text, `\x1b`)
}

// TestShortDurationKeepsSubMillisecondTime pins the round-six timing finding:
// `sleep: 500us` is legal, and a timing account that rendered it as 0s would
// report that no virtual time passed.
func TestShortDurationKeepsSubMillisecondTime(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "0s", shortDuration(0))
	assert.Equal(t, "500µs", shortDuration(500*time.Microsecond))
	assert.Equal(t, "30s", shortDuration(30*time.Second))
	assert.Equal(t, "5m", shortDuration(5*time.Minute))
	assert.Equal(t, "1h30m", shortDuration(90*time.Minute))
	assert.Equal(t, "1h", shortDuration(time.Hour))
}
