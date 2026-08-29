package debugpane

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The window a step pane draws, tested where its answers can differ.
//
// [window] takes its inputs rather than reading them off a frame for the reason
// CLAUDE.md's "assert where the answers differ" section gives: every interesting
// case here is one the real fixtures rarely produce — a list shorter than the
// budget, a position at either end, a list nothing points into — and a
// comparison written inline against real data is one no test can reach.
//
// Each row states the whole answer rather than a property of it, because the
// properties (it holds `budget` rows, it contains `at`) are individually
// satisfiable by windows that are wrong: `[0, budget)` satisfies both whenever
// the position happens to be near the front.
func TestTheStepWindowCentresOnThePosition(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		n, at, size int
		first, last int
	}{
		{name: "everything fits", n: 4, at: 2, size: 12, first: 0, last: 4},
		{name: "exactly fits", n: 12, at: 6, size: 12, first: 0, last: 12},
		{name: "centred in the middle", n: 100, at: 50, size: 5, first: 48, last: 53},
		{name: "clamped at the front", n: 100, at: 0, size: 5, first: 0, last: 5},
		{name: "clamped near the front", n: 100, at: 1, size: 5, first: 0, last: 5},
		{name: "clamped at the back", n: 100, at: 99, size: 5, first: 95, last: 100},
		// The odd row goes below the position: a reader looking at where a run
		// is held is looking forward more than back. With a budget of four and
		// a position at fifty, one row is above and two below.
		{name: "the odd row goes forward", n: 100, at: 50, size: 4, first: 49, last: 53},
		// No position at all is an autopsy: the run is over, nothing points
		// into the list, and the front is where it began.
		{name: "no position windows the front", n: 100, at: -1, size: 5, first: 0, last: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			first, last := window(tc.n, tc.at, tc.size)

			assert.Equal(t, tc.first, first, "first")
			assert.Equal(t, tc.last, last, "last")
		})
	}
}

// TestTheStepWindowNeverLeavesTheList is the invariant every row above is one
// instance of, and the one a caller slices with.
//
// Written as a sweep rather than as more rows, because the failure it guards is
// a panic on an index rather than a layout somebody would notice: `steps[first:last]`
// with either end out of range takes the whole command down at a breakpoint.
func TestTheStepWindowNeverLeavesTheList(t *testing.T) {
	t.Parallel()

	checked := 0
	for n := range 20 {
		for at := -1; at < n; at++ {
			for size := 1; size <= 20; size++ {
				first, last := window(n, at, size)

				assert.GreaterOrEqual(t, first, 0, "n=%d at=%d size=%d", n, at, size)
				assert.LessOrEqual(t, last, n, "n=%d at=%d size=%d", n, at, size)
				assert.LessOrEqual(t, first, last, "n=%d at=%d size=%d", n, at, size)
				assert.LessOrEqual(t, last-first, max(size, 0), "n=%d at=%d size=%d", n, at, size)

				if n > 0 && at >= 0 && size <= n {
					assert.GreaterOrEqual(t, at, first, "the position fell out of its own window")
					assert.Less(t, at, last, "the position fell out of its own window")
				}

				checked++
			}
		}
	}

	// The sweep ran, which is the claim every assertion above is inside a loop
	// for. See CLAUDE.md on a green test that asserts nothing.
	assert.Positive(t, checked, "the sweep covered no cases at all")
}

// TestPaneRowsSplitTheTerminalBetweenTwoPanes is the other extracted decision.
//
// The number matters in three places a fixture cannot reach through [Render]:
// a terminal too short to hold both panes, one tall enough that the cap decides
// instead of the height, and a stream with no measurable height at all.
func TestPaneRowsSplitTheTerminalBetweenTwoPanes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		height int
		want   int
	}{
		{name: "unmeasurable", height: 0, want: MaxPaneRows},
		{name: "negative is unmeasurable too", height: -1, want: MaxPaneRows},
		{name: "tall enough for the cap", height: 60, want: MaxPaneRows},
		{name: "an ordinary terminal", height: 24, want: 9},
		{name: "short", height: 12, want: MinPaneRows},
		{name: "shorter than its own chrome", height: 2, want: MinPaneRows},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, paneRows(tc.height))
		})
	}

	// The property the rows have to keep, stated where the arithmetic is: two
	// panes plus their chrome fit on the terminal they were sized for. A budget
	// that scrolled its own heading off is the failure the floor trades against,
	// and it is a trade rather than an accident only if it is bounded to the
	// short terminals the floor exists for.
	for height := 14; height <= 200; height++ {
		assert.LessOrEqual(t, paneRows(height)*2+6, height,
			"two panes and their chrome do not fit on a %d-row terminal", height)
	}
}

// The qualifier a row is drawn against, tested where its answers differ.
//
// [qualifiers] takes its rows rather than reading them off a frame for the
// reason [window] does: only the first case below is what real fixtures
// produce, and the others have to be built. Each row states the whole answer,
// because "some rows are qualified" is satisfied by qualifying all of them and
// by qualifying none.
func TestWhichRowsAreDrawnAgainstAQualifier(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		steps []flowdebug.Step
		want  []string
	}{
		{
			// The ordinary workflow: nothing shares an id, so nothing is
			// qualified. A prefix on every row is noise, and the rows are
			// meant to be typed back at the prompt where the name is the id.
			name: "no id is shared",
			steps: []flowdebug.Step{
				{Workflow: "w", ID: "build"},
				{Workflow: "w", ID: "test"},
			},
			want: []string{"", ""},
		},
		{
			// A singleton row *inside a callee* carries a Via, and must still
			// not be qualified: nothing else answers to its id, so a prefix
			// tells the reader nothing and costs a column.
			name: "a singleton inside a callee",
			steps: []flowdebug.Step{
				{Workflow: "outer", ID: "the_call"},
				{Workflow: "inner", Declaration: 1, Via: "the_call", ID: "only_here"},
			},
			want: []string{"", ""},
		},
		{
			// Two workflows, two names: the name is what a reader already has
			// in the file, so it is what they are drawn against.
			name: "shared id, different workflows",
			steps: []flowdebug.Step{
				{Workflow: "outer", ID: "build"},
				{Workflow: "inner", Declaration: 1, Via: "the_call", ID: "build"},
			},
			want: []string{"outer", "inner"},
		},
		{
			// One callee invoked twice. The name cannot separate them, so the
			// `call:` step an author wrote does.
			name: "shared id, one workflow name, two call sites",
			steps: []flowdebug.Step{
				{Workflow: "inner", Declaration: 1, Via: "first_call", ID: "build"},
				{Workflow: "inner", Declaration: 2, Via: "second_call", ID: "build"},
			},
			want: []string{"first_call", "second_call"},
		},
		{
			// Nothing distinguishes them at all — a hand-built inventory that
			// said neither. Drawn unqualified rather than decorated with
			// something that separates nothing.
			name: "shared id and nothing to tell them apart",
			steps: []flowdebug.Step{
				{Workflow: "w", ID: "build"},
				{Workflow: "w", ID: "build"},
			},
			want: []string{"", ""},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, qualifiers(tc.steps))
		})
	}
}
