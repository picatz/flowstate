package debugpane_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/picatz/flowstate/cmd/flow/internal/debugpane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The panes drawn from a Frame read through the typed contract, which is how
// `flow debug attach` reaches a run it does not own.

// heldAt holds a controlled session at step, with the scope's ambient vars, and
// returns it once the typed state says held. configure runs before the stop.
func heldAt(t *testing.T, vars map[string]*v1.Value, step string, configure func(*flowdebug.Session)) *flowdebug.Session {
	t.Helper()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	if configure != nil {
		configure(session)
	}

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}})
	scope.AmbientVars = vars

	finished := make(chan error, 1)
	go func() { finished <- session.BeforeStep(t.Context(), markStep(step), scope) }()
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

// bare hides everything a Session offers beyond the Target contract, so a read
// of it is a read of a remote.
type bare struct{ flowdebug.Target }

func drawn(t *testing.T, frame debugpane.Frame) string {
	t.Helper()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)

	return debugpane.Render(frame, ui.NewTheme(true, caps), caps.Symbols(), debugpane.Layout{Width: 80, Height: 24})
}

func fromTarget(t *testing.T, target flowdebug.Target, opts flowdebug.FrameOptions) (debugpane.Frame, string) {
	t.Helper()

	read, err := flowdebug.ReadFrame(t.Context(), target, opts)
	require.NoError(t, err)
	frame, paused := debugpane.FromFrame(read)
	require.True(t, paused)

	return frame, drawn(t, frame)
}

// TestAFrameDrawsWhatTheSessionPaneDraws holds the Target-read path to the same
// rows the in-process path draws at the same held stop, so attach paints the
// panes `flow test --debug` paints rather than a lookalike.
func TestAFrameDrawsWhatTheSessionPaneDraws(t *testing.T) {
	t.Parallel()

	vars := map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1"), "count": v1.NewLiteral(3)}
	session := heldAt(t, vars, "deploy", nil)

	layout := debugpane.Layout{Width: 80, Height: 24}
	old, paused := debugpane.Snapshot(t.Context(), session, layout)
	require.True(t, paused)
	require.NotEmpty(t, old.Bindings)

	got, text := fromTarget(t, session, flowdebug.FrameOptions{Source: session, StepRows: debugpane.StepRows(layout)})

	assert.Equal(t, old.Bindings, got.Bindings)
	assert.Equal(t, old.BindingsTotal, got.BindingsTotal)
	assert.Equal(t, old.Steps, got.Steps)
	assert.Equal(t, old.Held, got.Held)
	assert.Equal(t, old.At, got.At)
	assert.Equal(t, drawn(t, old), text)
}

// TestAPaneFromTheWireSaysThereIsNoInventory: no program, no step list, and the
// pane says so and says what to pass, never a guessed list.
func TestAPaneFromTheWireSaysThereIsNoInventory(t *testing.T) {
	t.Parallel()

	session := heldAt(t, map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")}, "deploy", nil)

	frame, text := fromTarget(t, bare{session}, flowdebug.FrameOptions{})
	assert.Empty(t, frame.Steps)
	assert.Contains(t, text, debugpane.NoInventoryNote)
	assert.Contains(t, text, "--program")
	assert.NotContains(t, text, "step(s)", "a step count was drawn for a list nobody named")
	assert.NotContains(t, text, "deploy", "a step was named with no program to name it")
	assert.Contains(t, text, "vars.region", "the scope the target did answer was not drawn")

	// With the program's list the pane draws it, which is the other direction.
	_, text = fromTarget(t, bare{session}, flowdebug.FrameOptions{Inventory: declared("m", "build", "deploy")})
	assert.NotContains(t, text, debugpane.NoInventoryNote)
	assert.Contains(t, text, "build")
	assert.Contains(t, text, "deploy")
}

// TestAPaneWhoseInspectIsRefusedSaysSo: the scope pane states the refusal
// instead of drawing nothing.
func TestAPaneWhoseInspectIsRefusedSaysSo(t *testing.T) {
	t.Parallel()

	session := heldAt(t, map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")}, "deploy", nil)

	read, err := flowdebug.ReadFrame(t.Context(), denied{session}, flowdebug.FrameOptions{})
	require.NoError(t, err)
	require.Empty(t, read.Values)

	frame, paused := debugpane.FromFrame(read)
	require.True(t, paused)
	text := drawn(t, frame)

	assert.Contains(t, text, "scope ", "the scope pane vanished instead of saying why")
	assert.Contains(t, text, "inspect is not permitted")
	assert.NotContains(t, text, "eu-west-1")
}

type denied struct{ flowdebug.Target }

func (denied) Inspect(context.Context, *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("workload.debug_inspect is required"))
}

// TestNoPanePaintsTheSecretAFrameWithheld: a session that withholds a value
// withholds it from the Frame and from every pane drawn of it.
func TestNoPanePaintsTheSecretAFrameWithheld(t *testing.T) {
	t.Parallel()

	vars := map[string]*v1.Value{
		"credential": v1.NewLiteral(theSecret),
		"header":     v1.NewLiteral("Bearer " + theSecret),
	}
	redact := func(s *flowdebug.Session) {
		s.SetRedactor(func(text string) string { return strings.ReplaceAll(text, theSecret, "[redacted]") })
		s.SetValueRedactor(func(value any) any {
			if text, ok := value.(string); ok && text == theSecret {
				return "[redacted]"
			}

			return value
		})
	}

	// The control: unredacted, the value does reach the pane through this path.
	open := heldAt(t, vars, "deploy", nil)
	_, text := fromTarget(t, bare{open}, flowdebug.FrameOptions{})
	require.Contains(t, text, theSecret, "the value never reached the pane even unredacted, so the refusal proves nothing")

	closed := heldAt(t, vars, "deploy", redact)
	read, err := flowdebug.ReadFrame(t.Context(), bare{closed}, flowdebug.FrameOptions{})
	require.NoError(t, err)
	frame, _ := debugpane.FromFrame(read)

	text = drawn(t, frame)
	assert.NotContains(t, text, theSecret)
	assert.Contains(t, text, "[redacted]")
	assert.NotContains(t, fmt.Sprintf("%+v", frame), theSecret)
	assert.NotContains(t, protojson.Format(read.Scope)+protojson.Format(read.Snapshot), theSecret)
	for _, value := range read.Values {
		assert.NotContains(t, protojson.Format(value), theSecret)
	}
}
