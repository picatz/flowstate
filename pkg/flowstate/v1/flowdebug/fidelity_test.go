package flowdebug_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// scopedHistory is a recorded run of three points whose middle one held a debug
// session with a scope, and whose first held none. It answers the inspections a
// Frame asks as the server does: the roots, a group's names, and an expression.
type scopedHistory struct{ asked []string }

var scopedPoints = []int64{3, 9, 15}

func (s *scopedHistory) read(_ context.Context, event int64, inspections ...*v1.DebugHistoryInspection) (*v1.DebugHistoryResponse, error) {
	if event == 0 {
		event = scopedPoints[len(scopedPoints)-1]
	}
	answer := &v1.DebugHistoryResponse{
		EventId: event, Boundaries: scopedPoints, Fidelity: v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED,
	}
	if event != scopedPoints[0] {
		answer.Snapshot = &v1.DebugSnapshot{
			Revision: uint64(event), State: v1.DebugRunState_DEBUG_RUN_STATE_HELD,
			Session: &v1.DebugSession{SessionId: "recorded", Run: &v1.RunAddress{WorkflowId: "w", RunId: "r"}},
			Occurrence: &v1.DebugOccurrence{
				Site: &v1.DebugSite{Workflow: "w", Path: []string{"build"}}, Address: "build",
			},
		}
	}
	for _, asked := range inspections {
		s.asked = append(s.asked, asked.GetExpression())
		result := &v1.DebugInspectResponse{}
		switch asked.GetExpression() {
		case "":
			result.Total = 1
			result.Children = []*v1.DebugVariable{{Name: "steps", Value: &v1.DebugValue{
				Type: "scope", Rendered: "1 names", Children: 1, Expression: "@scope:steps",
			}}}
		case "@scope:steps":
			result.Total = 2
			result.Children = []*v1.DebugVariable{
				{Name: "build", Value: &v1.DebugValue{Type: "int", Rendered: "7", Expression: "steps.build"}},
				{Name: "lost", Value: &v1.DebugValue{Type: "error", Rendered: "no such key", Expression: "steps.lost"}},
			}
		default:
			result.Value = &v1.DebugValue{Type: "int", Rendered: "7", Expression: asked.GetExpression()}
		}
		fidelity := v1.DebugFidelity_DEBUG_FIDELITY_HYPOTHETICAL
		if asked.GetExpression() == "" {
			fidelity = v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED
		}
		answer.Inspected = append(answer.Inspected, &v1.DebugHistoryInspected{Result: result, Fidelity: fidelity})
	}

	return answer, nil
}

func openScoped(t *testing.T) (*flowdebug.Historical, *scopedHistory) {
	t.Helper()

	reader := &scopedHistory{}
	h, err := flowdebug.OpenHistorical(t.Context(), reader.read)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	_, err = h.Travel(t.Context(), "to-middle", 0, 1)
	require.NoError(t, err)

	return h, reader
}

// TestAFrameSaysHowItsValuesAreKnown: a frame of a recorded point is
// reconstructed, a name listed from its scope is as the replay held it, an
// expression somebody typed is hypothetical, and a value that could not be
// produced is unavailable. A live frame carries none of it, and so no badge.
func TestAFrameSaysHowItsValuesAreKnown(t *testing.T) {
	t.Parallel()

	h, _ := openScoped(t)
	frame, err := flowdebug.ReadFrame(t.Context(), h, flowdebug.FrameOptions{})
	require.NoError(t, err)
	require.NotNil(t, frame.Scope, frame.ScopeNote)

	assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED, frame.Fidelity)
	build, lost := frame.Values["steps.build"], frame.Values["steps.lost"]
	require.NotNil(t, build)
	require.NotNil(t, lost)

	assert.Equal(t, "rec", flowdebug.FidelityBadge(frame.ValueFidelity(build, false)))
	assert.Equal(t, "hyp", flowdebug.FidelityBadge(frame.ValueFidelity(build, true)), "a typed expression is computed now")
	assert.Equal(t, "n/a", flowdebug.FidelityBadge(frame.ValueFidelity(lost, false)), "a value that errored is not known")
	assert.Equal(t, "n/a", flowdebug.FidelityBadge(frame.ValueFidelity(nil, true)), "no value is not known either")

	// The other direction: a live frame has no fidelity, whatever it is asked.
	live := flowdebug.Frame{}
	for _, typed := range []bool{false, true} {
		assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_UNSPECIFIED, live.ValueFidelity(build, typed))
		assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_UNSPECIFIED, live.ValueFidelity(nil, typed))
	}
	assert.Empty(t, flowdebug.FidelityBadge(v1.DebugFidelity_DEBUG_FIDELITY_UNSPECIFIED))
	assert.Equal(t, "reconstructed", flowdebug.FidelityName(frame.Fidelity))
	assert.Empty(t, flowdebug.FidelityName(live.Fidelity))
}

// TestAFrameAtAPointWithNoSessionHasNoScope: before the run installed a debug
// session there is nothing to list or evaluate, so the frame says so and the
// reader is never asked a question it would only refuse.
func TestAFrameAtAPointWithNoSessionHasNoScope(t *testing.T) {
	t.Parallel()

	h, reader := openScoped(t)
	_, err := h.Travel(t.Context(), "to-first", 0, 0)
	require.NoError(t, err)
	reader.asked = nil

	frame, err := flowdebug.ReadFrame(t.Context(), h, flowdebug.FrameOptions{})
	require.NoError(t, err)

	assert.Nil(t, frame.Scope)
	assert.Empty(t, frame.Values)
	assert.Equal(t, flowdebug.NoScopeHere, frame.ScopeNote)
	assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED, frame.Fidelity, "the point is still a reconstruction")
	assert.Empty(t, reader.asked, "an inspection was sent for a point that held no session")

	// And the next point, which held one, has its scope again.
	_, err = h.Travel(t.Context(), "to-second", 0, 1)
	require.NoError(t, err)
	frame, err = flowdebug.ReadFrame(t.Context(), h, flowdebug.FrameOptions{})
	require.NoError(t, err)
	require.NotNil(t, frame.Scope, frame.ScopeNote)
	assert.Empty(t, frame.ScopeNote)
}

// TestTheVerbsOfARecordedRunAreTheOnesItAnswers: the verbs a front leaves
// without a key for a recorded run are exactly the ones the run refuses, by
// name and with its own reason, and every verb that keeps a key is taken. A
// live durable run keeps the whole table.
func TestTheVerbsOfARecordedRunAreTheOnesItAnswers(t *testing.T) {
	t.Parallel()

	h, _ := openScoped(t)
	snapshot, err := h.Snapshot(t.Context())
	require.NoError(t, err)
	offered := map[string]bool{}
	for _, verb := range flowdebug.VerbsFor(snapshot.GetCapabilities()) {
		offered[verb.Name] = true
	}
	all := flowdebug.DriverVerbs()
	require.Greater(t, len(all), len(offered))

	// What each hidden verb says when it is typed anyway.
	typed := map[string]string{
		"until": "until build", "pause": "pause", "break": "break build", "log": "log build hello",
		"catch": "catch all", "delete": "delete build", "clear": "clear",
	}
	hidden := 0
	for _, verb := range all {
		if offered[verb.Name] {
			continue
		}
		hidden++
		line, ok := typed[verb.Name]
		require.True(t, ok, "%q is hidden from a recorded run and this test does not know how to type it", verb.Name)

		driver := flowdebug.NewDriver(h)
		result, err := driver.Do(t.Context(), line)
		if err == nil {
			assert.False(t, flowdebug.Accepted(result.Receipt), "%q was hidden and is accepted: %s", line, result.Text)
			assert.NotEmpty(t, result.Text, "%q was refused without a reason", line)
		}
	}
	assert.Equal(t, len(typed), hidden)

	// The negative direction: a verb that keeps its key is taken, from the
	// middle point, in whichever way it moves.
	for _, line := range []string{"step", "next", "finish", "back", "goto 0", "continue", "reverse-continue"} {
		fresh, _ := openScoped(t)
		result, err := flowdebug.NewDriver(fresh).Do(t.Context(), line)
		require.NoError(t, err, line)
		assert.True(t, flowdebug.Accepted(result.Receipt), "%q is offered and was refused: %s", line, result.Text)
	}

	// A live durable run keeps every verb, and `back` points at the walk.
	assert.Len(t, flowdebug.VerbsFor(v1.DurableDebugCapabilities()), len(all))
	assert.Len(t, flowdebug.VerbsFor(nil), len(all))
}

// TestALiveDurableRunSaysWhereItsRecordIs: `back` and `goto` on a durable run
// that is held live have no earlier stop to return to, and the refusal names the
// command that walks its record.
func TestALiveDurableRunSaysWhereItsRecordIs(t *testing.T) {
	t.Parallel()

	_, client := serveWaits(t)
	remote, _, err := flowdebug.AttachRemote(t.Context(), client, "w", "", flowdebug.RemoteOptions{Heartbeat: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Disconnect() })

	driver := flowdebug.NewDriver(remote)
	for _, line := range []string{"back", "reverse-continue", "goto 0"} {
		_, err := driver.Do(t.Context(), line)
		require.Error(t, err, line)
		assert.ErrorContains(t, err, "a live durable run cannot", line)
		assert.ErrorContains(t, err, "flow debug attach --history --run-id", line)
	}

	// A plain local session is not a durable run, and keeps naming what it lacks.
	plain, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	_, err = flowdebug.NewDriver(plain).Do(t.Context(), "back")
	assert.ErrorContains(t, err, "this session cannot step back")
	assert.NotContains(t, fmt.Sprint(err), "--history")
}

// TestADriverMovesARecordedRunForwardWithoutWaiting: a step over a recorded
// run is a read of the next point, and the driver answers with it at once; it
// used to wait for a move past the one its receipt named, which nothing makes.
func TestADriverMovesARecordedRunForwardWithoutWaiting(t *testing.T) {
	t.Parallel()

	reader := &scopedHistory{}
	h, err := flowdebug.OpenHistorical(t.Context(), reader.read, flowdebug.AtEvent(scopedPoints[0]))
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	driver := flowdebug.NewDriver(h)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for i, line := range []string{"step", "next", "finish"} {
		result, err := driver.Do(ctx, line)
		require.NoError(t, err, line)
		if i < 1 {
			require.True(t, flowdebug.Accepted(result.Receipt), result.Text)
			assert.Equal(t, int32(1), result.Snapshot.GetTimeline().GetCurrent())

			continue
		}
		if i == 1 {
			require.True(t, flowdebug.Accepted(result.Receipt), result.Text)
			assert.Equal(t, int32(2), result.Snapshot.GetTimeline().GetCurrent())

			continue
		}
		// The last point has nothing after it, and says so rather than waiting.
		assert.False(t, flowdebug.Accepted(result.Receipt))
		assert.Contains(t, result.Text, "nothing later")
	}
	require.NoError(t, ctx.Err())

	result, err := driver.Do(ctx, "back")
	require.NoError(t, err)
	assert.Equal(t, int32(1), result.Snapshot.GetTimeline().GetCurrent())
}

// TestADriversInspectSaysHowTheValueIsKnown: a typed inspect or expand at a
// recorded point carries the hypothetical badge in its text and its fidelity in
// its result, by the rule every front shares; a value that could not be produced
// is n/a; and a live stop's answer carries neither (the live tests assert its
// text byte for byte).
func TestADriversInspectSaysHowTheValueIsKnown(t *testing.T) {
	t.Parallel()

	h, _ := openScoped(t)
	driver := flowdebug.NewDriver(h)

	result, err := driver.Do(t.Context(), "inspect steps.build")
	require.NoError(t, err)
	assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_HYPOTHETICAL, result.Fidelity)
	assert.Equal(t, "[hyp] 7\n", result.Text)

	result, err = driver.Do(t.Context(), "expand @scope:steps")
	require.NoError(t, err)
	assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_HYPOTHETICAL, result.Fidelity)
	assert.Contains(t, result.Text, "[hyp] build  int  7\n")
	assert.Contains(t, result.Text, "[n/a] lost  error  no such key\n", "an unproduced child was not marked")
}
