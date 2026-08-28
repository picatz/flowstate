package flowdebug

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The two vocabularies this package shares with the schema, walked rather than
// remembered.
//
// Both tables are hand-written and both have to be, because Go cannot
// enumerate a package's constants at run time — which is the same position
// `tools/fuzztargets` and `tools/vacuity`'s corpus registry are in, and the
// answer is theirs: a written-out list is trustworthy only when something walks
// the thing it claims to cover and fails when the two disagree. A verb known to
// the prompt and unknown on the wire is exactly the bug the command table's own
// comment exists to prevent, one surface further out.

// TestEveryCommandVerbIsOnTheWire fails in both directions, which is what makes
// the table impossible to leave stale.
//
// A verb added to [commands] with no enum value fails here, and an enum value
// added with no verb fails here, and a verb removed from the table while the
// enum keeps its value fails here too — the last being the one a rename
// produces.
func TestEveryCommandVerbIsOnTheWire(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, commands, "the command table is empty, so every claim below is vacuous")

	// Every verb the prompt understands can be sent.
	for _, c := range commands {
		verb, ok := verbFor(c.verb)
		assert.True(t, ok,
			"the prompt understands %q and the wire has no verb for it, so a client cannot send a command a person can type", c.verb)
		assert.NotEqual(t, v1.DebugCommandVerb_DEBUG_COMMAND_VERB_UNSPECIFIED, verb,
			"%q maps to the unspecified verb, which no boundary accepts", c.verb)
	}

	// Every verb that can be sent is one the prompt understands, spelled the
	// way the prompt spells it. An alias here would be a wire vocabulary the
	// canonical table does not have.
	values := v1.DebugCommandVerb(0).Descriptor().Values()
	require.Positive(t, values.Len(), "the enum has no values, so every claim below is vacuous")

	for i := range values.Len() {
		verb := v1.DebugCommandVerb(values.Get(i).Number())
		if verb == v1.DebugCommandVerb_DEBUG_COMMAND_VERB_UNSPECIFIED {
			assert.NotContains(t, verbs, verb,
				"the unspecified verb was given a spelling, so a command with no verb renders as a command")

			continue
		}

		spelling, ok := verbs[verb]
		require.True(t, ok,
			"the wire has %v and this package has no verb for it, so a client can send a command the session cannot run", verb)

		known, ok := resolve(spelling)
		require.True(t, ok, "%v spells %q, which the prompt does not understand", verb, spelling)
		assert.Equal(t, known.verb, spelling,
			"%v spells %q, which is an alias rather than the canonical verb", verb, spelling)
	}

	assert.Len(t, verbs, len(commands),
		"the wire vocabulary and the prompt's are different sizes, so one of them has a verb the other does not")
}

// definedStepStates are the outcomes this package has a word for.
//
// Discovered rather than listed: a [StepState] is defined when it names itself
// something other than the default, so a member added with a [StepState.String]
// case turns up here without anybody editing this test. A member added *without*
// one would not — and that is the honest limit, stated rather than left for a
// reader to assume, because such a member is already broken in every place the
// prompt prints it.
func definedStepStates(t *testing.T) []StepState {
	t.Helper()

	// Generously past the vocabulary, so growing it does not silently outrun
	// the probe.
	const probe = 64

	states := []StepState{StepPending}
	for i := 1; i < probe; i++ {
		if state := StepState(i); state.String() != StepPending.String() {
			states = append(states, state)
		}
	}

	require.Greater(t, len(states), 1, "only the zero state was found, so the probe is not reaching the vocabulary")

	return states
}

// TestEveryStepStateHasAWireValue is the same walk over the outcome
// vocabulary, and it carries one claim the command walk does not need.
//
// The two vocabularies are deliberately offset by one. [StepPending] is Go's
// zero and means "this session watched nothing happen here"; the schema's zero
// is UNSPECIFIED and means "the producer did not say". A mapping that let those
// meet — a numeric conversion, or a table entry pointing PENDING at the
// unspecified value — would report every unreached step as an answer nobody
// gave, on a surface whose whole job is to be precise about what it does not
// know.
func TestEveryStepStateHasAWireValue(t *testing.T) {
	t.Parallel()

	states := definedStepStates(t)

	seen := map[v1.DebugStepState]StepState{}
	for _, state := range states {
		wire, ok := stepStates[state]
		require.True(t, ok,
			"%v is a step outcome with no wire value, so a window would report it as an answer nobody gave", state)
		assert.NotEqual(t, v1.DebugStepState_DEBUG_STEP_STATE_UNSPECIFIED, wire,
			"%v maps to the unspecified state, which means the producer said nothing — and this producer did", state)

		if other, clash := seen[wire]; clash {
			t.Errorf("%v and %v both map to %v, so two outcomes are one on the wire", state, other, wire)
		}
		seen[wire] = state
	}

	assert.Len(t, stepStates, len(states),
		"the table has entries for states this package does not define, so a removed outcome left a spelling behind")

	// And the reverse: every value the schema has is one this package can
	// produce, or the wire advertises an outcome nothing reports.
	values := v1.DebugStepState(0).Descriptor().Values()
	require.Positive(t, values.Len(), "the enum has no values, so every claim above is vacuous")

	for i := range values.Len() {
		wire := v1.DebugStepState(values.Get(i).Number())
		if wire == v1.DebugStepState_DEBUG_STEP_STATE_UNSPECIFIED {
			continue
		}
		assert.Contains(t, seen, wire,
			"the schema has %v and nothing in this package produces it", wire)
	}
}

// TestPendingIsNotTheWireZero states the offset on its own, because the walk
// above would still pass if PENDING and UNSPECIFIED were the same number and
// every other member shifted with them.
func TestPendingIsNotTheWireZero(t *testing.T) {
	t.Parallel()

	assert.Equal(t, v1.DebugStepState_DEBUG_STEP_STATE_PENDING, stepStates[StepPending],
		"the zero Go outcome does not map to the schema's PENDING, so a session that watched nothing "+
			"is reported as a producer that said nothing")
	assert.Equal(t, StepPending, StepState(0),
		"StepPending is no longer the zero value, which is what makes the offset above load-bearing")
}

// TestValidDeclarationRefusesWhatTheWireCannotSay is the door's rule, on the
// one count an embedder chooses freely.
//
// Extracted to a function taking its input for the reason CLAUDE.md gives: the
// real inventories in this tree all hold small non-negative numbers, so a check
// written where those are read is one no test can reach, and deleting it would
// survive.
//
// Both directions matter and they fail differently. Too large wraps, and a
// wrapped declaration names a *different invocation*. Negative does not wrap
// and is rejected by `DebugStep.declaration`'s own `gte: 0`, so it would
// produce a message the schema refuses — which is why the refusal happens at
// the door, where there is an error to return (Codex, #1194).
func TestValidDeclarationRefusesWhatTheWireCannotSay(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		in   int
		ok   bool
	}{
		{name: "the root declaration", in: 0, ok: true},
		{name: "an ordinary descent", in: 7, ok: true},
		{name: "the largest that fits", in: 1<<31 - 1, ok: true},
		{name: "one past it, which wraps negative under a bare conversion", in: 1 << 31},
		{name: "negative, which the schema's gte rejects", in: -1},
		{name: "the smallest int32, which fits the type and not the rule", in: -1 << 31},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.ok, validDeclaration(tc.in))
		})
	}
}

// TestNoSpellingOfAVerbProducesALineTheRendererRefuses is the corpus repair,
// and the reason it is here rather than beside its siblings.
//
// The pair's invariant already had a test, and that test missed an alias
// expanding past the bound — `p ` plus 65,534 bytes is exactly
// [MaxCommandBytes] as typed and eight bytes over it once `p` becomes
// `inspect` (Codex, #1194). The corpus was hand-listed and used the canonical
// spelling at the bound, so the one length where an alias differs from its verb
// was the one length it did not try.
//
// A hand-listed corpus is only as good as what somebody thought of, so this one
// is *derived*: every spelling in [commands], canonical and alias alike, at
// exactly the bound. An alias added to the table is covered without anybody
// editing this test, which is the property `tools/fuzztargets` exists for and
// the same one the two vocabulary walks above have.
func TestNoSpellingOfAVerbProducesALineTheRendererRefuses(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, commands, "the command table is empty, so this test is vacuous")

	checked := 0
	for _, c := range commands {
		for _, spelling := range append([]string{c.verb}, c.aliases...) {
			// Exactly at the bound as typed, which is where a spelling shorter
			// than its canonical verb crosses it once expanded.
			prefix := spelling + " "
			require.LessOrEqual(t, len(prefix), MaxCommandBytes)
			line := prefix + strings.Repeat("x", MaxCommandBytes-len(prefix))
			require.Len(t, line, MaxCommandBytes, "the line under test is not at the bound")

			command, ok := CommandProto(line)
			if !ok {
				// Refused on the way in is a fine answer — what must not happen
				// is a message this cannot render.
				continue
			}
			checked++

			back, err := CommandLine(command)
			require.NoError(t, err,
				"%q parsed into a message the renderer refuses, so the two conversions are not a pair", line)
			assert.LessOrEqual(t, len(back), MaxCommandBytes,
				"%q rendered to a line longer than the session accepts", line)
		}
	}

	assert.Positive(t, checked,
		"every spelling was refused at the bound, so this test is about refusals rather than about the pair")
}

// One message describes one stop.
//
// A scope answer is made of many evaluations, and reading the session's current
// pause per evaluation lets a run that resumes partway through answer the later
// names from a *different* stop — one message attributing values from two steps
// to the names listed at one (Codex, #1194). The two tests below are the
// mechanism and the property.

// taggedScope is a scope of n steps, every one carrying the same tag, so a
// rendering says which scope it came from.
func taggedScope(tag string, n int) *v1.Scope {
	values := make(map[string]*v1.Node_Outputs, n)
	for i := range n {
		values[fmt.Sprintf("s%03d", i)] = &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"tag": v1.NewLiteral(tag)},
		}
	}

	return &v1.Scope{Outputs: &v1.Workflow_StepOutputs{StepValues: values}}
}

// TestAnEvaluationIsPinnedToThePauseItWasGiven is the mechanism, stated where a
// fixture can drive it rather than left to a concurrent interleave.
//
// [Session.evaluateIn] answers against the pause it is handed and
// [Session.Evaluate] answers against the session's current one. That difference
// is the whole of what makes a multi-evaluation answer coherent, and it is
// deterministic: no goroutine has to win a race for this to be checkable.
func TestAnEvaluationIsPinnedToThePauseItWasGiven(t *testing.T) {
	t.Parallel()

	session, err := New(Options{Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	session.prompting(promptSubject{scope: taggedScope("A", 1)})

	session.mu.Lock()
	first := session.at
	session.mu.Unlock()

	// The premise: the pause that was captured really does answer, so the
	// claim after the move is about the pinning rather than about an empty
	// subject.
	before, _, err := session.evaluateIn(t.Context(), first, "steps.s000.tag")
	require.NoError(t, err)
	require.Contains(t, before, "A")

	// The run moves on.
	session.prompting(promptSubject{scope: taggedScope("B", 1)})

	after, _, err := session.evaluateIn(t.Context(), first, "steps.s000.tag")
	require.NoError(t, err)
	assert.Contains(t, after, "A",
		"an evaluation handed a pause answered from the session's current one instead, so an answer "+
			"made of several of them would mix two stops")

	// And the session's own accessor does follow the session, which is what
	// makes the pinning above a difference rather than a coincidence.
	current, _, err := session.Evaluate(t.Context(), "steps.s000.tag")
	require.NoError(t, err)
	assert.Contains(t, current, "B",
		"Evaluate stopped following the session's current pause, so the two are no longer different questions")
}

// TestAScopeMessageDescribesOneStop is the property, under a run that keeps
// moving.
//
// Fifty names is enough evaluation for a flipping pause to land in the middle
// of the answer, and both scopes carry the same *names* so only the values can
// differ — which makes "this message mixes two stops" a thing an assertion can
// see rather than a thing a reader would have to notice.
func TestAScopeMessageDescribesOneStop(t *testing.T) {
	t.Parallel()

	const names = 50

	session, err := New(Options{Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	scopes := []*v1.Scope{taggedScope("A", names), taggedScope("B", names)}
	session.prompting(promptSubject{scope: scopes[0]})

	stop := make(chan struct{})
	flipping := make(chan struct{})
	go func() {
		defer close(flipping)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			session.prompting(promptSubject{scope: scopes[i%2]})
		}
	}()

	mixed := 0
	for range 20 {
		message, scopeErr := session.ScopeProto(t.Context(), -1)
		require.NoError(t, scopeErr)

		tags := map[string]int{}
		for _, group := range message.GetGroups() {
			for _, binding := range group.GetBindings() {
				rendered := binding.GetRendered()
				switch {
				case strings.Contains(rendered, "A"):
					tags["A"]++
				case strings.Contains(rendered, "B"):
					tags["B"]++
				}
			}
		}
		if len(tags) > 1 {
			mixed++
		}
	}

	close(stop)
	<-flipping

	assert.Zero(t, mixed,
		"%d of 20 scope messages carried values from two different pauses, so one message described two stops", mixed)
}

// TestTheProducerCapsHowMuchOneScopeAnswerEvaluates is the bound on the
// resource a caller's budget does not bound.
//
// [v1.DefaultCostLimit] bounds one evaluation; nothing bounded how many one
// answer performs, and a workload chooses how many names a scope holds — so a
// caller asking for all of them bought unbounded compilation and evaluation
// with one message (Codex, #1194). The ceiling is the producer's and the budget
// is the caller's, and asking for more than the ceiling is the same answer as
// asking for exactly it.
func TestTheProducerCapsHowMuchOneScopeAnswerEvaluates(t *testing.T) {
	t.Parallel()

	// Comfortably past the ceiling, so the clamp is what decides the answer.
	const names = MaxScopeEvaluations + 137

	session, err := New(Options{Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	session.prompting(promptSubject{scope: taggedScope("A", names)})

	// What the run can reach, read from the session rather than assumed: the
	// steps are most of it and the scope carries other groups too, and a total
	// this test wrote down would be a second copy of a number the session
	// already computes.
	reachable := 0
	for _, group := range session.scopeNames(taggedScope("A", names), nil) {
		reachable += len(group.Names)
	}
	require.Greater(t, reachable, MaxScopeEvaluations,
		"the fixture holds fewer names than the ceiling, so the clamp below is never reached")

	for _, tc := range []struct {
		name  string
		limit int
	}{
		{name: "a negative limit asks for as many as the producer will do", limit: -1},
		{name: "a limit past the ceiling is the ceiling", limit: names},
		{name: "and so is one absurdly past it", limit: 1 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			message, scopeErr := session.ScopeProto(t.Context(), tc.limit)
			require.NoError(t, scopeErr)

			resolved, listed := 0, 0
			for _, group := range message.GetGroups() {
				for _, binding := range group.GetBindings() {
					listed++
					if binding.GetAnswer() != nil {
						resolved++
					}
				}
			}

			assert.Equal(t, MaxScopeEvaluations, resolved,
				"one answer evaluated %d values, which is not the producer's ceiling", resolved)

			// The totals are untouched by the ceiling, which is what keeps an
			// elision honest: a total that reported the bound back would say
			// the run can reach fewer names than it can.
			assert.Equal(t, reachable, listed,
				"the ceiling dropped names rather than leaving their values unresolved")
			assert.Equal(t, int32(reachable), message.GetTotal(),
				"the ceiling was reported back as the size of the scope")
		})
	}

	// The other direction, so the clamp is a ceiling rather than a floor: a
	// caller asking for less still gets less.
	fewer, err := session.ScopeProto(t.Context(), 3)
	require.NoError(t, err)

	resolved := 0
	for _, group := range fewer.GetGroups() {
		for _, binding := range group.GetBindings() {
			if binding.GetAnswer() != nil {
				resolved++
			}
		}
	}
	assert.Equal(t, 3, resolved, "a caller's smaller budget was overridden by the producer's ceiling")
}
