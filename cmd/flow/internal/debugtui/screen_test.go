package debugtui

import (
	"context"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestTheLayoutFoldsAtTheThresholds: the panes drawn at each width are the ones
// the design says, and below the floor nothing is laid out at all.
func TestTheLayoutFoldsAtTheThresholds(t *testing.T) {
	t.Parallel()

	layoutAt := func(w, h int) (geometry, error) {
		return Screen{Size: tui.Size{W: w, H: h}, Pane: paneSteps}.geometry()
	}
	cell := func(g geometry, name string) pane.Rect {
		for _, c := range g.layout.Cells {
			if c.Pane == name {
				return c.Rect
			}
		}

		return pane.Rect{}
	}

	for _, w := range []int{200, 121, 120} {
		g, err := layoutAt(w, 36)
		require.NoError(t, err)
		steps, scope, detail := cell(g, paneSteps), cell(g, paneScope), cell(g, paneInspector)
		assert.False(t, detail.Empty(), "w=%d: three columns have an inspector", w)
		assert.True(t, steps.X+steps.W <= scope.X && scope.X+scope.W <= detail.X, "w=%d: not three columns left to right", w)
		assert.Empty(t, g.layout.Tabs)
	}
	for _, w := range []int{119, 100} {
		g, err := layoutAt(w, 36)
		require.NoError(t, err)
		scope, detail := cell(g, paneScope), cell(g, paneInspector)
		assert.False(t, detail.Empty(), "w=%d: the inspector folded away instead of under the scope", w)
		assert.Equal(t, scope.X, detail.X, "w=%d: the inspector is not under the scope", w)
		assert.Greater(t, detail.Y, scope.Y)
	}
	for _, w := range []int{99, 80} {
		g, err := layoutAt(w, 36)
		require.NoError(t, err)
		assert.True(t, g.layout.Shown(paneSteps) && g.layout.Shown(paneScope), "w=%d", w)
		assert.False(t, g.layout.Shown(paneInspector), "w=%d: the inspector has no room here", w)
		assert.Empty(t, g.layout.Tabs)
	}
	for _, w := range []int{79, 60} {
		g, err := layoutAt(w, 36)
		require.NoError(t, err)
		assert.Equal(t, []string{paneSteps, paneScope}, g.layout.Tabs, "w=%d: panes are tabs below 80", w)
		assert.Len(t, g.layout.Cells, 1)
	}

	// The tab that is shown is the focused pane.
	g, err := Screen{Size: tui.Size{W: 70, H: 20}, Pane: paneScope}.geometry()
	require.NoError(t, err)
	assert.Equal(t, paneScope, g.layout.Cells[0].Pane)

	// Below the floor is refused, on either axis.
	for _, size := range []tui.Size{{W: 59, H: 36}, {W: 40, H: 12}, {W: 120, H: 11}, {W: 0, H: 0}} {
		_, err := layoutAt(size.W, size.H)
		var small tui.ErrTooSmall
		require.ErrorAs(t, err, &small, "%v", size)
		assert.Contains(t, small.Error(), "at least 60x12")
	}
	_, err = layoutAt(60, 12)
	require.NoError(t, err)
}

func TestAResizeRefoldsTheScreenAndKeepsItsState(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Click(find(t, m, scopePrefix+"g:inputs")))
	require.True(t, m.screen.Tree.Open("g:inputs"))

	m = send(m, tuitest.Resize(tui.Size{W: 70, H: 20}))
	text := view(m)
	assert.Contains(t, text, "tab switches", "below 80 columns the panes are tabs")
	tuitest.Fits(t, text, tui.Size{W: 70, H: 20})
	assert.True(t, m.screen.Tree.Open("g:inputs"), "a resize forgot what was open")

	m = send(m, tuitest.Resize(tui.Size{W: 40, H: 10}))
	assert.Contains(t, view(m), "needs at least 60x12")

	m = send(m, tuitest.Resize(tui.Size{W: 130, H: 40}))
	assert.Contains(t, view(m), "inspector")
	assert.True(t, m.screen.Tree.Open("g:inputs"))

	// While it is too small there is nothing to click, and a click does nothing.
	small := send(m, tuitest.Resize(tui.Size{W: 40, H: 10}))
	before := view(small)
	small = send(small, tuitest.Click(2, 2), tuitest.Wheel(2, 2, false))
	assert.Equal(t, before, view(small))
}

func TestTheTabsOfAFoldedLayoutAreClickable(t *testing.T) {
	t.Parallel()

	m := started(t, newFake(), func(c *Config) { c.Size = tui.Size{W: 70, H: 20} })
	require.Equal(t, "steps", m.screen.Pane)
	x, y := find(t, m, paneScope)
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, "scope", m.screen.Pane)
	assert.Equal(t, "scope", m.screen.Focus)
	assert.Contains(t, view(m), "inputs")
}

// ---- hygiene ----

// TestNoViewCodeReadsAClock: the screens are functions of the messages they were
// sent. Elapsed time, a toast's lifetime and a double click would otherwise be
// the first things a golden could not pin.
func TestNoViewCodeReadsAClock(t *testing.T) {
	t.Parallel()

	for _, dir := range []string{".", "../tui", "../pane", "../tui/tuitest"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		require.NoError(t, err)
		require.NotEmpty(t, files, dir)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			source, err := os.ReadFile(file)
			require.NoError(t, err)
			for _, banned := range []string{"time.Now(", "time.Tick(", "time.After(", "time.Since(", "time.NewTimer(", "time.NewTicker(", "tea.Tick(", "tea.Every("} {
				assert.NotContains(t, string(source), banned, "%s reads or waits on a clock", file)
			}
		}
	}
}

// TestTheShellImportsNoClient: tui imports neither the debugger nor the
// pane's client, and pane imports neither the shell nor the event loop.
func TestTheShellImportsNoClient(t *testing.T) {
	t.Parallel()

	forbidden := map[string][]string{
		"../tui":  {"flowstate/pkg/flowstate/v1/flowdebug", "cmd/flow/internal/debugtui", "cmd/flow/internal/watch", "cmd/flow/internal/debugpane"},
		"../pane": {"bubbletea", "cmd/flow/internal/tui", "cmd/flow/internal/debugtui", "flowstate/pkg/flowstate/v1/flowdebug"},
	}
	for dir, paths := range forbidden {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		require.NoError(t, err)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
			require.NoError(t, err)
			for _, spec := range parsed.Imports {
				for _, path := range paths {
					assert.NotContains(t, spec.Path.Value, path, "%s imports %s", file, path)
				}
			}
		}
	}
}

// ---- redaction ----

const theSecret = "hunter2-swordfish-3f9a"

// heldSession holds a real session at a step whose scope carries the secret, as
// a value and composed into a longer string, with the redactors a run installs.
func heldSession(t *testing.T, redacting bool) *flowdebug.Session {
	t.Helper()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	if redacting {
		session.SetRedactor(func(text string) string { return strings.ReplaceAll(text, theSecret, "[redacted]") })
		session.SetValueRedactor(func(value any) any {
			if text, ok := value.(string); ok && text == theSecret {
				return "[redacted]"
			}

			return value
		})
	}

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}})
	scope.AmbientVars = map[string]*v1.Value{
		"credential": v1.NewLiteral(theSecret),
		"header":     v1.NewLiteral("Bearer " + theSecret),
		"region":     v1.NewLiteral("eu-west-1"),
	}
	node := &v1.Node{Id: "deploy", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}

	finished := make(chan error, 1)
	go func() { finished <- session.BeforeStep(t.Context(), node, scope) }()
	t.Cleanup(func() {
		_ = session.Control(context.Background(), "continue")
		<-finished
	})

	var after uint64
	for {
		snapshot, err := session.WaitSnapshot(t.Context(), after)
		require.NoError(t, err)
		if snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
			return session
		}
		after = snapshot.GetRevision()
	}
}

// everythingDrawn opens a screen over the session, opens every group, selects
// the secret rows, types the inspections that name them, and returns every
// screen it drew along the way at every size.
func everythingDrawn(t *testing.T, session *flowdebug.Session) string {
	t.Helper()

	cfg := Config{
		Target: session, Driver: flowdebug.NewDriver(session), Style: plain, Size: tui.Size{W: 120, H: 36},
		Frame: flowdebug.FrameOptions{Source: session},
	}
	model, err := New(t.Context(), cfg)
	require.NoError(t, err)
	m := tuitest.Start(model).(Model)
	require.True(t, m.screen.Loaded)

	var drawn strings.Builder
	capture := func() {
		for _, size := range tuitest.Sizes {
			m = send(m, tuitest.Resize(size))
			drawn.WriteString(view(m) + "\n")
		}
		m = send(m, tuitest.Resize(tui.Size{W: 120, H: 36}))
	}

	capture()
	for _, group := range m.screen.Frame.Scope.GetGroups() {
		m.screen.Tree.Toggle(groupID(group.GetGroup()))
		for _, binding := range group.GetBindings() {
			m.screen.Tree.Select(binding.GetExpression())
			capture()
		}
	}
	m = send(m, tuitest.Key("?"))
	capture()
	m = send(m, tuitest.Key("esc"), tuitest.Key(":"))
	for _, line := range []string{"inspect vars.credential", "inspect vars.header", "scope", "expand vars"} {
		m = send(m, append(tuitest.Keys(line), tuitest.Key("enter"))...)
		capture()
	}
	// The transcript itself, not only what fits on a screen.
	drawn.WriteString(strings.Join(m.screen.Console.Lines(), "\n"))

	return drawn.String()
}

// TestNoPaneRevealsAWithheldLeaf: a value the session withholds is withheld from
// every pane, at every size, at every selection, and from the transcript of what
// the console asked.
func TestNoPaneRevealsAWithheldLeaf(t *testing.T) {
	// The control comes first: without it the refusal below passes on a screen
	// that never could have drawn the value.
	open := everythingDrawn(t, heldSession(t, false))
	require.Contains(t, open, theSecret, "the value never reached a screen even unredacted, so the refusal below proves nothing")

	redacted := everythingDrawn(t, heldSession(t, true))
	tuitest.NoSecret(t, redacted, theSecret)
	assert.Contains(t, redacted, "[redacted]", "the row vanished instead of being withheld, hiding that there is a name there")
	assert.Contains(t, redacted, "eu-west-1", "the screen drew nothing of the scope at all")
}
