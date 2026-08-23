package engine

// The state query carries its own two bounds, and both are reasoned in prose in
// progress.go while nothing drives either to the point where it triggers. These
// tests pin the three behaviors the comments promise:
//
//   - setLoopState accepts up to entityStateMaxLoopEntries distinct keys and
//     refuses the one past it, setting loopStateTruncated;
//   - stateSnapshot's byte bound is a backstop *behind* the count bound: a map
//     well under the entry count but over entityStateMaxBytes is discarded whole,
//     answering only {Truncated: true};
//   - loopStateTruncated is sticky — once a snapshot has dropped something, it
//     keeps saying so, including across a clearLoopState of an active loop.
//
// These are unexported methods on *progress, so the test is package-internal.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestSetLoopStateRefusesPastTheEntryBound drives the count bound to its exact
// edge: entityStateMaxLoopEntries distinct keys are all accepted, and the one
// past it is refused rather than growing the map without bound.
//
// The point is that the bound is *reached* — asserting the map holds exactly
// entityStateMaxLoopEntries after filling it, and that loopStateTruncated is
// still false at the edge, is what distinguishes a real cap from a test that
// merely never pushed hard enough to hit one.
func TestSetLoopStateRefusesPastTheEntryBound(t *testing.T) {
	p := &progress{}

	// Fill exactly to the bound. Every one of these is a new key under the
	// limit, so every one is accepted and nothing is marked truncated.
	for i := 0; i < entityStateMaxLoopEntries; i++ {
		p.setLoopState(fmt.Sprintf("loop-%d", i), v1.NewLiteral(i))
	}

	require.Len(t, p.loopState, entityStateMaxLoopEntries,
		"every key up to the bound should be accepted")
	require.False(t, p.loopStateTruncated,
		"reaching the bound exactly must not mark the snapshot truncated")

	// One past the bound: a new key with the map already full. It is refused —
	// the map does not grow — and the refusal is recorded.
	p.setLoopState("loop-overflow", v1.NewLiteral("dropped"))

	assert.Len(t, p.loopState, entityStateMaxLoopEntries,
		"a key past the bound must not be added")
	assert.NotContains(t, p.loopState, "loop-overflow",
		"the refused key must be absent, not silently overwriting another")
	assert.True(t, p.loopStateTruncated,
		"refusing a key must set loopStateTruncated")

	// Overwriting an already-tracked key when full is still allowed: it is not a
	// new entry, so it cannot grow the map past its bound.
	p.setLoopState("loop-0", v1.NewLiteral("updated"))
	assert.Len(t, p.loopState, entityStateMaxLoopEntries,
		"updating an existing key when full must neither grow nor shrink the map")
}

// TestStateSnapshotByteBoundWinsOverCountBound pins that the byte bound is a
// genuinely independent backstop: a map with only a handful of entries — far
// under entityStateMaxLoopEntries, so the count bound never fired and
// loopStateTruncated is false — still gets discarded whole when its serialized
// size crosses entityStateMaxBytes, answering nothing but {Truncated: true}.
func TestStateSnapshotByteBoundWinsOverCountBound(t *testing.T) {
	p := &progress{}

	// A handful of large values. Each string is ~96 KiB, so four of them put the
	// serialized EntityState well over the 256 KiB byte bound while the entry
	// count (4) stays far below entityStateMaxLoopEntries (64).
	const big = 96 * 1024
	for i := 0; i < 4; i++ {
		p.setLoopState(fmt.Sprintf("loop-%d", i), v1.NewLiteral(strings.Repeat("x", big)))
	}

	require.Less(t, len(p.loopState), entityStateMaxLoopEntries,
		"this case must stay under the count bound so the byte bound is what fires")
	require.False(t, p.loopStateTruncated,
		"the count bound must not have fired — the byte bound is what we are testing")

	got := p.stateSnapshot()
	require.NotNil(t, got)

	assert.True(t, got.GetTruncated(),
		"a snapshot over the byte bound must report truncated")
	assert.Empty(t, got.GetLoopState(),
		"an over-size snapshot must discard the whole map, not return it partially")
	assert.Empty(t, got.GetVars(),
		"an over-size snapshot must carry nothing but the truncated flag")
}

// TestLoopStateTruncatedIsStickyAcrossClear pins the comment's exact claim: once
// loopStateTruncated is set, it stays set for the rest of the segment, including
// across a clearLoopState of a loop that is ending. A snapshot that has dropped
// something must keep saying so; clearing an active loop's tracked value is a
// normal event and must not reset that admission.
func TestLoopStateTruncatedIsStickyAcrossClear(t *testing.T) {
	p := &progress{}

	// Fill to the bound, then overflow it to set loopStateTruncated.
	for i := 0; i < entityStateMaxLoopEntries; i++ {
		p.setLoopState(fmt.Sprintf("loop-%d", i), v1.NewLiteral(i))
	}
	p.setLoopState("loop-overflow", v1.NewLiteral("dropped"))
	require.True(t, p.loopStateTruncated, "precondition: overflow set the flag")

	// A tracked loop finishes and its state is cleared. This drops the key from
	// the map but must leave the truncated admission untouched.
	p.clearLoopState("loop-0")

	assert.NotContains(t, p.loopState, "loop-0",
		"clearLoopState must drop the cleared loop's tracked value")
	assert.True(t, p.loopStateTruncated,
		"clearing a loop must not reset loopStateTruncated — it stays sticky for the segment")

	// The snapshot still reports truncated even though the map now has room
	// again, because the flag records that an answer was once incomplete.
	assert.True(t, p.stateSnapshot().GetTruncated(),
		"a snapshot after a clear still reports the earlier truncation")
}

// TestCurrentDetailsMarkdownMatchesPositionPathShape pins
// [progress.currentDetailsMarkdown]'s two claims: it renders nothing before a
// position exists (the same honest absence [progress.snapshot] treats an
// empty stepID as), and once one exists it joins the step id and its path the
// same "outer > inner" way `cmd/flow/get.go`'s positionPath does, so a reader
// of `flow get`/`flow watch` sees the same shape in Temporal Web's Details
// view (#753).
func TestCurrentDetailsMarkdownMatchesPositionPathShape(t *testing.T) {
	p := &progress{}

	assert.Empty(t, p.currentDetailsMarkdown(),
		"a position with no stepID must render nothing, not an invented \"on step 1\"")

	p.enter(0, "deploy")
	assert.Equal(t, "On step `deploy`", p.currentDetailsMarkdown())

	p.enter(1, "each")
	p.enter(2, "upload")
	assert.Equal(t, "On step `deploy` > `each` > `upload`", p.currentDetailsMarkdown(),
		"a nested position must join outer to inner, the same order positionPath renders")

	// Re-entering a shallower depth truncates the path exactly as [progress.enter]
	// documents, and the rendering must follow it rather than keep a stale tail.
	p.enter(0, "cleanup")
	assert.Equal(t, "On step `cleanup`", p.currentDetailsMarkdown(),
		"entering a new top-level step must drop the previous step's deeper path")
}
