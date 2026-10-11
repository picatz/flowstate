package flowdap_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// locatedFlowfile has a comment, blank lines and steps whose bodies span
// lines, so every kind of line that is not a step start is present.
const locatedFlowfile = `edition: v2026.4
name: located

# a comment is not a step
steps:
  - id: first
    log:
      message: one

  - id: second
    log:
      message: two
`

func locate(t *testing.T, c *client, seq int, args map[string]any) map[string]any {
	t.Helper()

	c.send(seq, "breakpointLocations", args)

	return c.await("response", "breakpointLocations")
}

func locatedLines(answer map[string]any) []int {
	lines := []int{}
	for _, entry := range body(answer)["breakpoints"].([]any) {
		lines = append(lines, int(entry.(map[string]any)["line"].(float64)))
	}

	return lines
}

func TestBreakpointLocationsAreAdvertisedOnlyForLineBreakpoints(t *testing.T) {
	t.Parallel()

	c, _, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	assert.Equal(t, true, body(c.await("response", "initialize"))["supportsBreakpointLocationsRequest"])

	// A session built with no source to resolve lines through resolves none.
	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	bare := newClient(t)
	t.Cleanup(func() { _ = bare.Close() })
	server := flowdap.NewServer(session, bare)
	go func() { _ = server.Serve(t.Context()) }()
	bare.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	assert.NotEqual(t, true, body(bare.await("response", "initialize"))["supportsBreakpointLocationsRequest"])
}

func TestBreakpointLocationsAgreeWithWhatSetBreakpointsVerifies(t *testing.T) {
	t.Parallel()

	c, program, _ := launchedWith(t, "located.yaml", locatedFlowfile, nil)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	src := map[string]any{"path": program}

	total := strings.Count(locatedFlowfile, "\n")
	answer := locate(t, c, 3, map[string]any{"source": src, "line": 1, "endLine": total})
	require.Equal(t, true, answer["success"], answer)
	offered := locatedLines(answer)
	// Step starts only: the comment, the blank lines and a step's own body
	// are not offered, though a line inside a step still verifies.
	assert.Equal(t, []int{6, 10}, offered)
	for _, entry := range body(answer)["breakpoints"].([]any) {
		assert.Equal(t, float64(5), entry.(map[string]any)["column"], "the step's `- ` is not part of its span")
	}

	// Every line of the file, asked of setBreakpoints in one replacement.
	entries := make([]map[string]any, 0, total)
	for line := 1; line <= total; line++ {
		entries = append(entries, map[string]any{"line": line})
	}
	c.send(4, "setBreakpoints", map[string]any{"source": src, "breakpoints": entries})
	set := c.await("response", "setBreakpoints")
	require.Equal(t, true, set["success"], set)
	verified := map[int]bool{}
	for i, entry := range body(set)["breakpoints"].([]any) {
		verified[i+1] = entry.(map[string]any)["verified"] == true
	}
	for _, line := range offered {
		assert.True(t, verified[line], "line %d was offered and would not verify", line)
	}
	for _, line := range []int{1, 2, 3, 4, 9} {
		assert.False(t, verified[line], "line %d names no step", line)
		assert.NotContains(t, offered, line)
	}

	// The range is honored, and a file URI names the same source.
	assert.Equal(t, []int{10}, locatedLines(locate(t, c, 5, map[string]any{"source": src, "line": 7, "endLine": 11})))
	assert.Equal(t, []int{6}, locatedLines(locate(t, c, 6, map[string]any{"source": map[string]any{"path": "file://" + program}, "line": 6})))
	assert.Empty(t, locatedLines(locate(t, c, 7, map[string]any{"source": src, "line": 4})), "a comment has no location")
	assert.Empty(t, locatedLines(locate(t, c, 8, map[string]any{"source": src, "line": 9})), "a blank line has no location")
	assert.Empty(t, locatedLines(locate(t, c, 9, map[string]any{"source": src, "line": 500, "endLine": 600})))

	// Columns narrow the first and last line of the range.
	assert.Empty(t, locatedLines(locate(t, c, 10, map[string]any{"source": src, "line": 6, "column": 6, "endLine": 6})))
	assert.Empty(t, locatedLines(locate(t, c, 11, map[string]any{"source": src, "line": 6, "endLine": 6, "endColumn": 5})))
	assert.Equal(t, []int{6}, locatedLines(locate(t, c, 12, map[string]any{"source": src, "line": 6, "column": 5, "endLine": 6, "endColumn": 6})))
}

func TestBreakpointLocationsRefuseWhatTheyCannotAnswer(t *testing.T) {
	t.Parallel()

	c, program, _ := launchedWith(t, "located.yaml", locatedFlowfile, nil)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")

	// No program yet: nothing to resolve a line through.
	early := locate(t, c, 2, map[string]any{"source": map[string]any{"path": program}, "line": 6})
	assert.Equal(t, false, early["success"])

	c.send(3, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	src := map[string]any{"path": program}
	for name, args := range map[string]map[string]any{
		"unknown source":   {"source": map[string]any{"path": "/elsewhere/other.yaml"}, "line": 6},
		"unlisted file":    {"source": map[string]any{"path": "/etc/passwd"}, "line": 1},
		"reference source": {"source": map[string]any{"path": program, "sourceReference": 4}, "line": 6},
		"no source":        {"line": 6},
		"empty path":       {"source": map[string]any{"path": ""}, "line": 6},
		"oversized path":   {"source": map[string]any{"path": strings.Repeat("a", 5000)}, "line": 6},
		"no line":          {"source": src},
		"line before file": {"source": src, "line": 0},
		"end before start": {"source": src, "line": 10, "endLine": 6},
		"column before 1":  {"source": src, "line": 6, "column": 0},
		"wrapping line":    {"source": src, "line": 1, "endLine": 1 << 40},
	} {
		assert.Equal(t, false, locate(t, c, 100, args)["success"], name)
	}
	c.send(101, "breakpointLocations", nil)
	assert.Equal(t, false, c.await("response", "breakpointLocations")["success"])

	// An endLine past the file is clamped to it, not refused.
	assert.Equal(t, []int{6, 10}, locatedLines(locate(t, c, 102, map[string]any{"source": src, "line": 1, "endLine": 1 << 31})))
}

// manySteps is a file of n three-line steps, past what a session holds and,
// for the larger n, past what one request examines.
func manySteps(n int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: many\nsteps:\n")
	for i := range n {
		fmt.Fprintf(&b, "  - id: s%d\n    log:\n      message: x\n", i)
	}

	return b.String()
}

func TestBreakpointLocationsAreBounded(t *testing.T) {
	t.Parallel()

	const steps = 2800 // 8403 lines: more than a request examines.
	c, program, _ := launchedWith(t, "many.yaml", manySteps(steps), nil)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	src := map[string]any{"path": program}

	// More step starts in range than a session holds: the answer stops at the cap.
	answer := locate(t, c, 3, map[string]any{"source": src, "line": 1, "endLine": 3*flowdebug.MaxBreakpoints + 300})
	require.Equal(t, true, answer["success"], answer)
	lines := locatedLines(answer)
	assert.Len(t, lines, flowdebug.MaxBreakpoints)
	assert.Equal(t, 4, lines[0])

	// A range over the work bound is refused, not truncated.
	assert.Equal(t, false, locate(t, c, 4, map[string]any{"source": src, "line": 1, "endLine": 3*steps + 3})["success"])
}

// A client whose columns start at 0 sees the source's fifth column as 4, and
// column 0 is a real column there, not an unknown one: the filter keys off the
// source map's column, and the response always carries the column. A step
// never starts at source column 1 (steps are nested in a list), so this pins
// the zero-based filtering the guard serves, not the guard's unknown-column
// branch itself.
func TestBreakpointLocationsFilterByColumnForAZeroBasedClient(t *testing.T) {
	t.Parallel()

	c, program, _ := launchedWith(t, "located.yaml", locatedFlowfile, nil)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate", "linesStartAt1": false, "columnsStartAt1": false})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	src := map[string]any{"path": program}

	// Source line 6 is client line 5; the step starts at client column 4.
	all := locate(t, c, 3, map[string]any{"source": src, "line": 0, "endLine": 20})
	require.Equal(t, true, all["success"], all)
	assert.Equal(t, []int{5, 9}, locatedLines(all))
	assert.Equal(t, float64(4), body(all)["breakpoints"].([]any)[0].(map[string]any)["column"])

	assert.Equal(t, []int{5}, locatedLines(locate(t, c, 4, map[string]any{"source": src, "line": 5, "column": 4, "endLine": 5, "endColumn": 5})))
	assert.Empty(t, locatedLines(locate(t, c, 5, map[string]any{"source": src, "line": 5, "column": 5, "endLine": 5})))
	assert.Empty(t, locatedLines(locate(t, c, 6, map[string]any{"source": src, "line": 5, "endLine": 5, "endColumn": 4})))
	assert.Empty(t, locatedLines(locate(t, c, 7, map[string]any{"source": src, "line": 5, "endLine": 5, "endColumn": 0})))
}

// A range ending at the largest line must be bounded like any other: the line
// counter must not wrap past it, and the work must not scale with the range.
func TestBreakpointLocationsRefuseARangeToTheLastLine(t *testing.T) {
	t.Parallel()

	c, program, _ := launchedWith(t, "many.yaml", manySteps(4000), nil)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	src := map[string]any{"path": program}

	// Lines 4 and up, ending at MaxUint32: past the work bound, so refused.
	assert.Equal(t, false, locate(t, c, 3, map[string]any{"source": src, "line": 4, "endLine": math.MaxUint32 - 1})["success"])
	// A range within the bound that ends past the document still answers.
	assert.Equal(t, true, locate(t, c, 4, map[string]any{"source": src, "line": 11900, "endLine": math.MaxUint32 - 1})["success"])
}

// A zero-based client's largest endLine must not overflow the range check.
func TestBreakpointLocationsRefuseAZeroBasedMaxIntEndLine(t *testing.T) {
	t.Parallel()

	c, program, _ := launchedWith(t, "located.yaml", locatedFlowfile, nil)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate", "linesStartAt1": false, "columnsStartAt1": false})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	src := map[string]any{"path": program}

	answer := locate(t, c, 3, map[string]any{"source": src, "line": 0, "endLine": math.MaxInt})
	assert.Equal(t, false, answer["success"], answer)
}

// A durable target reports no source breakpoints of its own, but the adapter
// holds a verified map from the attach and answers locations from it.
func TestBreakpointLocationsAreOfferedAfterADurableAttach(t *testing.T) {
	t.Parallel()

	program := filepath.Join(t.TempDir(), "located.yaml")
	require.NoError(t, os.WriteFile(program, []byte(locatedFlowfile), 0o600))
	read := []byte(locatedFlowfile)
	workflow, positions, err := flowfile.ParseAt(read, program)
	require.NoError(t, err)
	sourceMap := flowfile.SourceMap(program, read, workflow, positions)

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	require.False(t, v1.DurableDebugCapabilities().GetSourceBreakpoints())
	remote := &fakeRemote{snapshot: &v1.DebugSnapshot{
		Revision:     1,
		State:        v1.DebugRunState_DEBUG_RUN_STATE_RUNNING,
		Capabilities: v1.DurableDebugCapabilities(),
	}}
	server := flowdap.NewServer(nil, c,
		flowdap.WithLaunch(func(context.Context, flowdap.LaunchArguments) (*flowdap.Launch, error) {
			return nil, errors.New("this test only attaches")
		}),
		flowdap.WithAttach(func(context.Context, flowdap.AttachArguments) (*flowdap.Attachment, error) {
			return &flowdap.Attachment{Target: remote, SourceMap: sourceMap}, nil
		}))
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "attach", map[string]any{"workflowId": "wf-1"})
	c.await("response", "attach")
	caps := body(c.await("event", "capabilities"))["capabilities"].(map[string]any)
	assert.Equal(t, true, caps["supportsBreakpointLocationsRequest"])
	assert.Equal(t, []int{6, 10}, locatedLines(locate(t, c, 3, map[string]any{"source": map[string]any{"path": program}, "line": 1, "endLine": 20})))
}
