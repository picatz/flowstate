package flowdebug_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The step list is answered as a window, and what that buys.
//
// A workflow's breadth is the author's, and a near-limit file declares
// thousands of nodes. Copying an entry per node at every stop is O(N) of
// allocation per stop and O(N²) across a walk of the run, for a pane that draws
// a dozen rows — which on a large file is an interactive debugger that stops
// being interactive (Codex, #1182).

// bigSession is a session over a workflow of n declared steps, none of them run.
func bigSession(t *testing.T, n int) *flowdebug.Session {
	t.Helper()

	steps := make([]flowdebug.Step, 0, n)
	for i := range n {
		steps = append(steps, flowdebug.Step{Workflow: "big", ID: fmt.Sprintf("s%05d", i)})
	}

	session, err := flowdebug.New(flowdebug.Options{Out: &strings.Builder{}, Steps: steps})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	return session
}

// TestTheStepListIsAnsweredAsAWindow is the bound, counted rather than timed.
//
// Entries copied is the resource, and it is the one a test can state exactly: a
// timing assertion would be a flake on a busy machine, and an allocation count
// would move with the runtime. What must hold is that the answer is the size
// asked for and not the size of the file.
func TestTheStepListIsAnsweredAsAWindow(t *testing.T) {
	t.Parallel()

	const (
		declared = 5_000
		asked    = 12
	)

	session := bigSession(t, declared)

	list := session.Steps(2_000, asked)

	assert.Len(t, list.Steps, asked,
		"the window copied a number of entries that is not the number asked for")
	assert.Equal(t, declared, list.Total,
		"a window has to carry the total or an elision cannot say how much it elided")
	assert.Equal(t, 2_000, list.Offset)
	assert.Equal(t, "s02000", list.Steps[0].ID, "the window starts somewhere other than its offset")

	// The property the O(N²) claim turns on: walking the run asks for a window
	// per stop, and every one of them costs the window rather than the file.
	copied := 0
	for offset := 0; offset < declared; offset++ {
		copied += len(session.Steps(offset, asked).Steps)
	}
	assert.Less(t, copied, declared*asked+1,
		"a stop copies more than the window it asked for")
	assert.Less(t, copied, declared*declared/100,
		"walking the run is still quadratic in the file's breadth")
}

// TestAWindowPastTheEndIsEmptyRatherThanAPanic covers the offsets a renderer
// can hand over on a list that changed under it, and the ones a wire message
// can carry from a client that made them up.
//
// A slice expression written inline would take the process down on any of
// these; each is a real value some caller produces.
func TestAWindowPastTheEndIsEmptyRatherThanAPanic(t *testing.T) {
	t.Parallel()

	session := bigSession(t, 10)

	for _, tc := range []struct {
		name          string
		offset, limit int
		want          int
	}{
		{name: "the whole list", offset: 0, limit: -1, want: 10},
		{name: "a limit past the end", offset: 5, limit: 100, want: 5},
		{name: "an offset at the end", offset: 10, limit: 3, want: 0},
		{name: "an offset past the end", offset: 500, limit: 3, want: 0},
		{name: "a negative offset", offset: -4, limit: 3, want: 3},
		{name: "a zero limit", offset: 0, limit: 0, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			list := session.Steps(tc.offset, tc.limit)

			assert.Len(t, list.Steps, tc.want)
			assert.Equal(t, 10, list.Total, "the total is a fact about the list, not about the window")
		})
	}
}

// TestStepPositionRefusesAnAmbiguousName is the other half of the fix for the
// same finding, at the surface that decides where a pane points.
//
// Pointing at a step the run is not at is the one thing a debugger must never
// do, so an id two workflows declare is answered with "I cannot tell" rather
// than with the first match.
func TestStepPositionRefusesAnAmbiguousName(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{
		Out: &strings.Builder{},
		Steps: []flowdebug.Step{
			{Workflow: "outer", ID: "build"},
			{Workflow: "outer", ID: "nested"},
			{Workflow: "inner", ID: "build"},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	// Named with the workflow, each resolves exactly.
	index, total := session.StepPosition("outer", "build")
	assert.Equal(t, 0, index)
	assert.Equal(t, 3, total)

	index, _ = session.StepPosition("inner", "build")
	assert.Equal(t, 2, index, "the callee's step resolved to the caller's row")

	// Without one, the id names two rows and the answer is neither.
	index, _ = session.StepPosition("", "build")
	assert.Equal(t, -1, index,
		"an ambiguous name resolved to the first match, which is the pane pointing at the wrong step")

	// An id only one workflow declares still resolves with no workflow given,
	// which is what keeps a run carrying no runtime position usable.
	index, _ = session.StepPosition("", "nested")
	assert.Equal(t, 1, index)

	// And a name the list does not hold at all.
	index, _ = session.StepPosition("outer", "absent")
	assert.Equal(t, -1, index)
}

// TestWhichInventoriesCarryAnUnattributableID states the rule in both
// directions, which is the only way to state it: "some ids are unattributable"
// is satisfied by a session that says every id is, and by one that says none is,
// and each of those is a defect.
//
// The rule keys on two *workflows* declaring one id, so the rows that must keep
// their outcomes are as much the point as the rows that must lose them.
func TestWhichInventoriesCarryAnUnattributableID(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		inventory []flowdebug.Step
		want      int
	}{
		{
			name:      "distinct ids in one workflow",
			inventory: []flowdebug.Step{{Workflow: "w", ID: "build"}, {Workflow: "w", ID: "test"}},
		},
		{
			// A `for_each` body declaring the same id as a top-level step is
			// two rows of one workflow: the run reaches them separately and
			// the session can still say which finished, because the engine's
			// isolation is per call rather than per row.
			name:      "one workflow declaring an id twice",
			inventory: []flowdebug.Step{{Workflow: "w", ID: "build"}, {Workflow: "w", ID: "build"}},
		},
		{
			// A caller that named no workflows at all: no evidence of a second
			// declaring workflow, so nothing is blanked. Failing closed on a
			// guess here would take the states off every ordinary duplicate.
			name:      "an inventory that names no workflow",
			inventory: []flowdebug.Step{{ID: "build"}, {ID: "build"}},
		},
		{
			name:      "two workflows declaring one id",
			inventory: []flowdebug.Step{{Workflow: "outer", ID: "build"}, {Workflow: "inner", ID: "build"}},
			want:      2,
		},
		{
			// The mixed case the first draft of this rule got backwards: a
			// named caller and a callee with no `name:` are two workflows, and
			// an outcome naming `build` still belongs to one of them.
			name:      "a named caller and an unnamed callee",
			inventory: []flowdebug.Step{{Workflow: "outer", ID: "build"}, {ID: "build"}},
			want:      2,
		},
		{
			// Only the shared id: the refusal is scoped to the ids it is about
			// rather than to the list.
			name: "one shared id among others",
			inventory: []flowdebug.Step{
				{Workflow: "outer", ID: "build"},
				{Workflow: "outer", ID: "nested"},
				{Workflow: "inner", ID: "build"},
			},
			want: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, err := flowdebug.New(flowdebug.Options{Out: &strings.Builder{}, Steps: tc.inventory})
			require.NoError(t, err)
			t.Cleanup(func() { _ = session.Close() })

			assert.Equal(t, tc.want, session.Steps(0, -1).Unattributed)
		})
	}
}

// TestAnUnsharedIDKeepsItsOutcome is the same rule seen through a real run,
// where an outcome exists to be kept or lost.
func TestAnUnsharedIDKeepsItsOutcome(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("continue\n"),
		Out:   &console,
		Steps: []flowdebug.Step{{ID: "build"}, {ID: "test"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, runErr := v1.Run(ctx, &v1.Workflow{Name: "w", Steps: []*v1.Node{markStep("build"), markStep("test")}})
	require.NoError(t, runErr)

	list := session.Steps(0, -1)

	assert.Zero(t, list.Unattributed)
	require.Len(t, list.Steps, 2)
	for _, step := range list.Steps {
		assert.Equal(t, flowdebug.StepDone, step.State, "%s lost its outcome", step.ID)
	}
}
