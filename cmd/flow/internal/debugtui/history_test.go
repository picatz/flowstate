package debugtui

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A recorded run, as the server reads it: five points, the first before the run
// installed a debug session and the rest holding one at each of the fake
// program's steps, the last the closing event. The scope is the fake's, so the
// values are the ones the live screens are tested with; what the record adds is
// that nothing here runs.

// recordedPoints are the run's boundaries. The first holds no session.
var recordedPoints = []int64{3, 9, 15, 21, 27}

// noSessionEvent is the point that held no debug session.
const noSessionEvent = 3

// noSessionAnswer is what the server says to an inspection at that point.
const noSessionAnswer = "the run held no session at this point, so there is nothing to inspect"

type recorded struct {
	fake *fakeTarget

	mu     sync.Mutex
	events []int64
	asked  []*v1.DebugHistoryInspection
}

func newRecorded() *recorded {
	fake := newFake()
	for i := range fake.groups {
		if fake.groups[i].name == "steps" {
			fake.groups[i].names = append(fake.groups[i].names, fakeName{name: "lost", rendered: "no such key", typ: "error"})
		}
	}
	// What a typed expression says: one that evaluates, and one that does not.
	fake.eval = func(expression string, _ int) (string, string, bool) {
		switch expression {
		case "1 + 1":
			return "2", "", true
		case "nope":
			return "", "no such attribute: nope", true
		}

		return "", "", false
	}

	return &recorded{fake: fake}
}

func (r *recorded) read(ctx context.Context, event int64, inspections ...*v1.DebugHistoryInspection) (*v1.DebugHistoryResponse, error) {
	if event == 0 {
		event = recordedPoints[len(recordedPoints)-1]
	}
	r.mu.Lock()
	r.events = append(r.events, event)
	r.asked = append(r.asked, inspections...)
	r.mu.Unlock()

	answer := &v1.DebugHistoryResponse{
		EventId: event, Boundaries: recordedPoints, Fidelity: v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED,
		Progress: &v1.RunProgress{StepId: "checkout", Path: []string{"release", "checkout"}},
	}
	index := -1
	for i, point := range recordedPoints {
		if point == event {
			index = i
		}
	}
	held := event != noSessionEvent
	if held {
		r.fake.mu.Lock()
		r.fake.at = index - 1
		answer.Snapshot = proto.CloneOf(r.fake.snapshot())
		r.fake.mu.Unlock()
	}
	if index == len(recordedPoints)-1 {
		answer.Outcome = v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED
	}
	for _, asked := range inspections {
		result := &v1.DebugInspectResponse{Error: noSessionAnswer}
		if held {
			var err error
			result, err = r.fake.Inspect(ctx, &v1.DebugInspectRequest{
				Expression: asked.GetExpression(), Children: asked.GetChildren(), Offset: asked.GetOffset(), Limit: asked.GetLimit(),
			})
			if err != nil {
				return nil, err
			}
		}
		fidelity := v1.DebugFidelity_DEBUG_FIDELITY_HYPOTHETICAL
		if asked.GetExpression() == "" {
			fidelity = v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED
		}
		answer.Inspected = append(answer.Inspected, &v1.DebugHistoryInspected{Result: result, Fidelity: fidelity})
	}

	return answer, nil
}

func (r *recorded) reads() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]int64(nil), r.events...)
}

// historyOf opens the screen over the record at an event, with the verbs the
// command derives from the target's capabilities.
func historyOf(t *testing.T, run *recorded, event int64, mods ...func(*Config)) Model {
	t.Helper()

	history, err := flowdebug.OpenHistorical(t.Context(), run.read, flowdebug.AtEvent(event))
	require.NoError(t, err)
	t.Cleanup(func() { _ = history.Close() })
	snapshot, err := history.Snapshot(t.Context())
	require.NoError(t, err)

	cfg := Config{
		Target: history, Driver: flowdebug.NewDriver(history), Style: plain, Size: tui.Size{W: 200, H: 60},
		Verbs: flowdebug.VerbsFor(snapshot.GetCapabilities()), Record: snapshot.GetCapabilities().GetHistory(), Frame: flowdebug.FrameOptions{Inventory: run.fake.inventory()},
	}
	for _, mod := range mods {
		mod(&cfg)
	}
	model, err := New(t.Context(), cfg)
	require.NoError(t, err)

	return tuitest.Start(model).(Model)
}

// badges are the marks a screen can carry for how a value is known.
var badges = []string{"[rec]", "[hyp]", "[n/a]"}

// valueRows are the tree rows that show a value: everything but the groups,
// which list names and hold none.
func valueRows(m Model) (rows []string, badged map[string]string) {
	badged = map[string]string{}
	for _, row := range m.screen.Tree.Rows() {
		if row.Kind != 0 || strings.HasPrefix(row.ID, "g:") || strings.HasPrefix(row.ID, "x:") {
			continue
		}
		rows = append(rows, row.ID)
		badged[row.ID] = row.Badge
	}

	return rows, badged
}

// TestAHistoryFrameLabelsEveryValue: at a recorded point the bar says
// reconstructed, and every row that shows a value says how it is known: a name
// of the scope is "rec", an expression typed at the console or watched is
// "hyp", a value that cannot be produced is "n/a". The words are drawn, so the
// screen reads the same without colour, and a live stop carries none of it.
func TestAHistoryFrameLabelsEveryValue(t *testing.T) {
	t.Parallel()

	run := newRecorded()
	m := historyOf(t, run, recordedPoints[2])
	assert.Contains(t, lines(view(m))[0], "reconstructed", "the bar does not say the run is a reconstruction")

	// Every name of the scope, as listed.
	m.screen.Tree.Expand("g:inputs")
	m.screen.Tree.Expand("g:steps")
	m.screen.Tree.Expand("g:run")
	rows, badged := valueRows(m)
	require.Contains(t, rows, "inputs.version")
	require.Contains(t, rows, "steps.lost")
	for _, id := range rows {
		want := "[rec]"
		if id == "steps.lost" {
			want = "[n/a]"
		}
		assert.Equal(t, want, badged[id], "scope row %s", id)
	}

	// Something typed, something watched, and a watch that cannot be evaluated.
	m = typed(m, "inspect inputs.version")
	m = typed(m, "watch 1 + 1")
	m = typed(m, "watch nope")
	_, badged = valueRows(m)
	assert.Equal(t, "[hyp]", badged[resultPrefix+"inputs.version"], "an inspection typed at the console")
	assert.Equal(t, "[hyp]", badged[watchPrefix+"1 + 1"], "a watch")
	assert.Equal(t, "[n/a]", badged[watchPrefix+"nope"], "a watch that cannot be evaluated")
	assert.Equal(t, "[rec]", badged["inputs.version"], "the typed expression changed how the scope row is known")

	// A value read below a scope row is the scope's; one below a typed
	// expression is as hypothetical as the expression.
	m = typed(m, "expand steps.checkout")
	m.setFocus(paneScope)
	m.screen.Tree.Select("steps.checkout")
	m = send(m, tuitest.Key("enter"))
	_, badged = valueRows(m)
	assert.Equal(t, "[rec]", badged["steps.checkout.sha"], "a child of a scope row")
	assert.Equal(t, "[hyp]", badged[childID(resultPrefix+"steps.checkout", "steps.checkout.sha")], "a child of an expression typed at the console")

	// All of it is on the screen, as words.
	text := strings.Join(paneLines(t, m, paneScope), "\n")
	for _, badge := range badges {
		assert.Contains(t, text, badge)
	}
	drawn := 0
	rows, badged = valueRows(m)
	for _, id := range rows {
		assert.NotEmpty(t, badged[id], "%s shows a value and says nothing of how it is known", id)
		if badged[id] != "" {
			drawn++
		}
	}
	assert.Equal(t, drawn, strings.Count(text, "[rec]")+strings.Count(text, "[hyp]")+strings.Count(text, "[n/a]"),
		"a badge was not drawn, or was drawn twice")
	assert.Contains(t, strings.Join(paneLines(t, m, paneInspector), "\n")+text, "reconstructed", "nothing says what [rec] means")

	// The same in the styled screen: the words are there with or without colour.
	for _, v := range styles {
		with := historyOf(t, newRecorded(), recordedPoints[2], func(c *Config) { c.Style = v.style })
		with.screen.Tree.Expand("g:inputs")
		with = typed(with, "inspect inputs.version")
		assert.Contains(t, view(with), "[hyp]", v.name)
		assert.Contains(t, view(with), "[rec]", v.name)
	}
}

// TestAHistoryFrameLabelsNothingOnALiveStop: the badge belongs to a
// reconstruction. The same operations on a live run draw none, and its bar does
// not say reconstructed.
func TestAHistoryFrameLabelsNothingOnALiveStop(t *testing.T) {
	t.Parallel()

	fake := newRecorded().fake
	m := started(t, fake)
	m.screen.Tree.Expand("g:steps")
	m = typed(m, "inspect inputs.version")
	m = typed(m, "watch 1 + 1")

	rows, badged := valueRows(m)
	require.NotEmpty(t, rows)
	for _, id := range rows {
		assert.Empty(t, badged[id], "%s is badged on a live run", id)
	}
	text := view(m)
	for _, badge := range badges {
		assert.NotContains(t, text, badge)
	}
	assert.NotContains(t, lines(text)[0], "reconstructed")
}

// TestAHistoryScreenFitsEverySize: the badge column and the bar's word are
// inside the terminal at every size, and a size that is too small says so.
func TestAHistoryScreenFitsEverySize(t *testing.T) {
	t.Parallel()

	for _, size := range tuitest.Sizes {
		t.Run(size.String(), func(t *testing.T) {
			t.Parallel()

			m := historyOf(t, newRecorded(), recordedPoints[2], func(c *Config) { c.Size = size })
			m.screen.Tree.Expand("g:steps")
			m = typed(m, "inspect steps.lost")
			tuitest.Fits(t, view(m), size)
		})
	}
}

// TestAHistoryWalkHidesTheVerbsItRefuses: a recorded run refuses what needs it
// to run, so those verbs have no key and no hint, and the mouse gestures that
// stand for them say so. Typed anyway they are refused by name with the run's
// own reason and move nothing. Every other way of moving works, both directions.
func TestAHistoryWalkHidesTheVerbsItRefuses(t *testing.T) {
	t.Parallel()

	run := newRecorded()
	m := historyOf(t, run, recordedPoints[2])
	live0 := started(t, newFake())

	// No key, no hint, no help for what the record cannot do.
	for key, verb := range map[string]string{"p": "pause", "u": "until", "B": "break"} {
		_, bound := m.keys.Match(key)
		assert.False(t, bound, "%q is bound on a recorded run, and %s cannot be done there", key, verb)
	}
	hints := m.keys.Hints(200, plain.Theme)
	for _, word := range []string{"pause", "until", "break"} {
		assert.NotContains(t, hints, word)
	}
	help := strings.Join(helpLines(m.keys, m.screen.Verbs, m.screen.options(120, 60, plain, true)), "\n")
	liveHelp := strings.Join(helpLines(live0.keys, live0.screen.Verbs, live0.screen.options(120, 60, plain, true)), "\n")
	for _, verb := range flowdebug.DriverVerbs() {
		if slices.ContainsFunc(m.screen.Verbs, func(v flowdebug.Verb) bool { return v.Name == verb.Name }) {
			continue
		}
		assert.NotContains(t, help, verb.Help, "the help teaches %s, which the record refuses", verb.Name)
		assert.Contains(t, liveHelp, verb.Help, "the comparison is vacuous: a live run's help does not teach %s", verb.Name)
	}
	assert.NotContains(t, help, "run until it", "enter is taught as running until a step")

	// Leaving a record releases nothing and lets nothing go, and the keys say so;
	// a live run's say what they have always said.
	assert.Contains(t, help, "leave the record")
	assert.NotContains(t, help, "let the run go on unattended")
	assert.NotContains(t, help, "release the run")
	assert.Contains(t, liveHelp, "let the run go on unattended")
	assert.Contains(t, liveHelp, "run until it")
	for key, verb := range map[string]string{"s": "step", "n": "next", "c": "continue", "b": "back", "r": "reverse-continue", "g": "goto", "f": "finish"} {
		_, bound := m.keys.Match(key)
		assert.True(t, bound, "%q (%s) is not bound, and a recorded run can do it", key, verb)
	}

	// The other direction: on a live front the same keys are there.
	live := started(t, newFake())
	for _, key := range []string{"p", "u", "B"} {
		_, bound := live.keys.Match(key)
		assert.True(t, bound, "%q is not bound on a live run", key)
	}

	// A key that is not bound does nothing at all.
	before := m.screen.Frame.Snapshot.GetRevision()
	reads := len(run.reads())
	m = send(m, tuitest.Key("p"), tuitest.Key("u"), tuitest.Key("B"))
	assert.Equal(t, before, m.screen.Frame.Snapshot.GetRevision())
	assert.Len(t, run.reads(), reads, "an unbound key reached the record")
	assert.False(t, m.screen.Toast.Active())

	// The gestures that stand for them are declined, by name.
	for _, test := range []struct {
		name string
		do   func() Model
		verb string
	}{
		{"double click on a step runs until it", func() Model { got, _ := m.flowUntil(); return got.(Model) }, "until"},
		{"right click on a step toggles a breakpoint", func() Model { got, _ := m.flowBreak(); return got.(Model) }, "break"},
		{"a click on a line's number arms a breakpoint", func() Model { got, _ := m.sourceBreak(1); return got.(Model) }, "break"},
	} {
		got := test.do()
		assert.Contains(t, got.screen.Toast.Text(), "does not answer "+test.verb, test.name)
		assert.Equal(t, before, got.screen.Frame.Snapshot.GetRevision(), test.name)
	}

	// Typed anyway, each is refused by name with the record's own reason.
	for line, reason := range map[string]string{
		"until build":    "a recorded run cannot run until a boundary",
		"break build":    "a recorded run cannot stop at a breakpoint",
		"pause":          "a recorded run is not running",
		"log build hi":   "a recorded run cannot stop at a breakpoint",
		"catch all":      "a recorded run cannot stop at a breakpoint",
		"delete build":   "",
		"clear":          "a recorded run cannot stop at a breakpoint",
		"until nosuch":   "a recorded run cannot run until a boundary",
		"break build if": "",
	} {
		got := typed(m, line)
		assert.Equal(t, before, got.screen.Frame.Snapshot.GetRevision(), "%q moved the record", line)
		if reason == "" {
			continue
		}
		assert.Contains(t, transcript(got), reason, "%q was not refused with the record's reason", line)
		assert.True(t, got.screen.Toast.Active(), "%q was refused silently", line)
	}
	assert.Empty(t, run.fake.resumes, "a command reached the live fake instead of the record")
}

// TestAHistoryWalkGoesBothWays: next, back, goto and the strip all move the
// record to another point, forwards and backwards, and the ends refuse with
// their reason. Nothing is run: only the record is read.
func TestAHistoryWalkGoesBothWays(t *testing.T) {
	t.Parallel()

	run := newRecorded()
	m := historyOf(t, run, recordedPoints[0])
	current := func() int32 { return m.screen.Frame.Snapshot.GetTimeline().GetCurrent() }
	require.Equal(t, int32(0), current())
	require.Len(t, m.screen.Frame.Snapshot.GetTimeline().GetPoints(), len(recordedPoints))

	// Back from the first point has nowhere to go.
	m = send(m, tuitest.Key("b"))
	assert.Equal(t, int32(0), current())
	assert.Contains(t, m.screen.Toast.Text(), "nothing earlier")

	// Forward, one point at a time to the end, by each of the keys.
	for want, key := range []string{"n", "s", "space", "n"} {
		m = send(m, tuitest.Key(key))
		assert.Equal(t, int32(want+1), current(), "key %q", key)
		assert.False(t, m.screen.Toast.Active(), "key %q: %s", key, m.screen.Toast.Text())
	}
	m = send(m, tuitest.Key("n"))
	assert.Equal(t, int32(4), current())
	assert.Contains(t, m.screen.Toast.Text(), "nothing later")
	assert.Contains(t, lines(view(m))[0], "reconstructed")

	// And back again, one and then all the way, and out by the strip.
	m = send(m, tuitest.Key("b"))
	assert.Equal(t, int32(3), current())
	m = send(m, tuitest.Key("r"))
	assert.Equal(t, int32(0), current(), "reverse-continue returns to the first point of a record")
	m = send(m, tuitest.Key("c"))
	assert.Equal(t, int32(4), current(), "continue goes to the last")
	x, y := find(t, m, pointPrefix+"1")
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, int32(1), current(), "a click on the strip goes to the point it names")
	m = typed(m, "goto 3")
	assert.Equal(t, int32(3), current())
	m = typed(m, "goto 9")
	assert.Equal(t, int32(3), current(), "a point past the end moved the record")
	assert.Contains(t, transcript(m), "no point 9")

	// The record was read, never run, and each point's scope is the one it held:
	// the held step follows the point.
	assert.Empty(t, run.fake.resumes)
	assert.Equal(t, "flaky", m.screen.Frame.Snapshot.GetOccurrence().GetAddress())
	assert.Contains(t, transcript(m), "← back to", "a travel is one more thing that happened, and the transcript says so")
}

// TestAPointWithNoSessionHasNoScopeAndSaysSo: before the run installed a debug
// session there is nothing to list and nothing to evaluate, so the scope pane
// says that in its place instead of drawing nothing, a watch is not known
// there, and the next point that held a session has its scope again.
func TestAPointWithNoSessionHasNoScopeAndSaysSo(t *testing.T) {
	t.Parallel()

	run := newRecorded()
	m := historyOf(t, run, noSessionEvent)
	frame := m.screen.Frame
	require.Equal(t, int32(0), frame.Snapshot.GetTimeline().GetCurrent())
	assert.Nil(t, frame.Scope)
	assert.Empty(t, frame.Values)

	pane := strings.Join(paneLines(t, m, paneScope), "\n")
	assert.Contains(t, pane, "no scope at this point", "the scope pane is blank where there is no scope")
	assert.Contains(t, pane, "no debug session")
	assert.NotContains(t, pane, "inputs", "a scope from another point is still drawn")
	for _, badge := range badges {
		assert.NotContains(t, pane, badge, "a row was badged where there is no value")
	}
	assert.Contains(t, lines(view(m))[0], "reconstructed", "the point is still a reconstruction")
	assert.Empty(t, run.asked, "the record was asked for a scope it does not have")

	// Something typed is answered by the record, in its words, and is no row.
	m = typed(m, "inspect inputs.version")
	assert.Contains(t, transcript(m), noSessionAnswer)
	assert.Empty(t, valueRowIDs(m), "an inspection with no answer became a row")

	// A watch has nothing to read there, and says it is not known.
	m = typed(m, "watch 1 + 1")
	_, badged := valueRows(m)
	assert.Equal(t, "[n/a]", badged[watchPrefix+"1 + 1"])

	// The point after it held a session, and its scope is back, with nothing
	// of the empty point's note.
	m = send(m, tuitest.Key("esc"), tuitest.Key("n"))
	require.Equal(t, int32(1), m.screen.Frame.Snapshot.GetTimeline().GetCurrent())
	require.NotNil(t, m.screen.Frame.Scope)
	pane = strings.Join(paneLines(t, m, paneScope), "\n")
	assert.NotContains(t, pane, "no scope at this point")
	assert.Contains(t, pane, "inputs")
}

// valueRowIDs are the ids of the inspection's rows.
func valueRowIDs(m Model) []string {
	var ids []string
	for _, row := range m.screen.Tree.Rows() {
		if strings.HasPrefix(row.ID, resultPrefix) {
			ids = append(ids, row.ID)
		}
	}

	return ids
}

// TestAHistoryScreenGolden pins a recorded point with every kind of row on it, in
// the three variants every screen is: the bar's word, the strip, the scope with a
// reconstructed name, an unavailable one, a typed expression and a watch.
func TestAHistoryScreenGolden(t *testing.T) {
	for _, v := range styles {
		t.Run(v.name, func(t *testing.T) {
			m := historyOf(t, newRecorded(), recordedPoints[2], func(c *Config) { c.Style = v.style; c.Size = tui.Size{W: 120, H: 36} })
			m.screen.Tree.Expand("g:inputs")
			m.screen.Tree.Expand("g:steps")
			m = typed(m, "watch 1 + 1")
			m = typed(m, "inspect inputs.version")
			m = send(m, tuitest.Key("esc"))
			m.setFocus(paneScope)

			text := view(m)
			tuitest.Fits(t, text, tui.Size{W: 120, H: 36})
			tuitest.Golden(t, text)
		})
	}
}
