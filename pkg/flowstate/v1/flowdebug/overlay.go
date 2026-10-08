package flowdebug

import (
	"cmp"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// NodeState is what a run has done at one static site of its program, as a view
// of the program's structure draws it.
//
// It is not [StepState]: that is one row of a step list, and this is one node of
// a tree that also holds the containers around the held step and the step the run
// is stopped before. The zero value is [NodePending], so a node the run has not
// been seen to reach reads as not yet and never as finished.
type NodeState uint8

const (
	// NodePending is a node nothing has been observed to do.
	NodePending NodeState = iota

	// NodeRunning is a container the held step sits inside (a loop, a parallel
	// group, a call): entered, and not finished.
	NodeRunning

	// NodeHeld is the step the run is held before.
	NodeHeld

	// NodeWaiting is a step that began waiting for a signal or a timer.
	NodeWaiting

	// NodeDone is a step that finished without an error.
	NodeDone

	// NodeTolerated is a step that failed and whose failure the run absorbed.
	NodeTolerated

	// NodeFailed is a step whose failure propagated.
	NodeFailed

	// NodeSkipped is a step whose `if:` evaluated false.
	NodeSkipped
)

// String is the word a pane writes beside a node in this state.
func (s NodeState) String() string {
	switch s {
	case NodeRunning:
		return "running"
	case NodeHeld:
		return "held"
	case NodeWaiting:
		return "waiting"
	case NodeDone:
		return "done"
	case NodeTolerated:
		return "tolerated"
	case NodeFailed:
		return "failed"
	case NodeSkipped:
		return "skipped"
	default:
		return "pending"
	}
}

// MaxOverlayNodes bounds the sites one [Overlay] records. The snapshot's
// observations are bounded already ([MaxObservations]); this is the bound on
// what is kept of them, and what is not kept is counted in [Overlay.Dropped].
const MaxOverlayNodes = 2048

// Overlay is what a snapshot says the run did, keyed by static site address.
//
// A static address is an occurrence address with every dynamic qualifier
// removed (see [StaticAddress]): `pages/page` is the `page` in the loop `pages`
// whichever iteration reached it, so a program's nodes can be joined to the
// states without a mapping table. The zero Overlay is valid and says nothing.
type Overlay struct {
	// States holds a state for each static address the snapshot named, at most
	// [MaxOverlayNodes] of them. A site absent from it is [NodePending].
	States map[string]NodeState

	// Held is the static address of the step the run is held before, or empty:
	// a running run, an autopsy and a finished run hold nothing.
	Held string

	// Dropped counts the sites past [MaxOverlayNodes] that were not recorded.
	Dropped int
}

// State is the state of the site at a static address.
//
// The held site is [NodeHeld] unless its outcome there was a failure, which a
// failure stop is held at and which stays [NodeFailed]; [Overlay.Held] names it
// either way.
func (o Overlay) State(address string) NodeState {
	if state, ok := o.States[address]; ok {
		return state
	}
	if address != "" && address == o.Held {
		return NodeHeld
	}

	return NodePending
}

// StaticAddress is an occurrence address with its dynamic qualifiers removed: an
// iteration `[3]`, a branch `#1`, a case `?0`, and a call's callee `(child)`.
// `fan_out(child)/pages[2]/page` becomes `fan_out/pages/page`, the address of the
// same site in the program as written.
func StaticAddress(address string) string {
	if !strings.ContainsAny(address, "[#?(") {
		return address
	}

	var b strings.Builder
	b.Grow(len(address))
	for i := 0; i < len(address); i++ {
		switch c := address[i]; c {
		case '[', '(':
			// A callee's name is the engine's text and may hold a slash; it is
			// skipped to its closing bracket, not to the next separator.
			closer := byte(']')
			if c == '(' {
				closer = ')'
			}
			if end := strings.IndexByte(address[i:], closer); end >= 0 {
				i += end
			} else {
				i = len(address)
			}
		case '#', '?':
			for i+1 < len(address) && address[i+1] >= '0' && address[i+1] <= '9' {
				i++
			}
		default:
			b.WriteByte(c)
		}
	}

	return b.String()
}

// overlayOf derives the overlay of one snapshot.
//
// Observations are read in the order they happened, so the latest outcome at a
// site is the one that stays. The held occurrence then overrides its own site,
// as a step window does: a second pass over a loop body is held before a step an
// earlier pass finished, and that earlier outcome is not this arrival's. The
// containers around it are running, which the run has not finished.
func overlayOf(snapshot *v1.DebugSnapshot) Overlay {
	var overlay Overlay
	set := func(address string, state NodeState) {
		if address == "" {
			return
		}
		if _, seen := overlay.States[address]; !seen && len(overlay.States) >= MaxOverlayNodes {
			overlay.Dropped++

			return
		}
		if overlay.States == nil {
			overlay.States = make(map[string]NodeState)
		}
		overlay.States[address] = state
	}

	for _, observation := range snapshot.GetObservations() {
		var state NodeState
		switch observation.GetKind() {
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED:
			state = NodeDone
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_SKIPPED:
			state = NodeSkipped
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED:
			state = NodeFailed
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TOLERATED:
			state = NodeTolerated
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_WAITING:
			state = NodeWaiting
		default:
			continue
		}
		set(StaticAddress(cmp.Or(observation.GetAddress(), observation.GetStepId())), state)
	}

	if snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD ||
		snapshot.GetReason() == v1.DebugStopReason_DEBUG_STOP_REASON_AUTOPSY {
		return overlay
	}
	occurrence := snapshot.GetOccurrence()
	held := StaticAddress(occurrence.GetAddress())
	if held == "" {
		return overlay
	}

	path := ""
	for _, segment := range occurrence.GetSegments() {
		path += segment.GetStepId()
		if overlay.States[path] != NodeFailed {
			set(path, NodeRunning)
		}
		path += "/"
	}
	if overlay.States[held] != NodeFailed {
		set(held, NodeHeld)
	}
	overlay.Held = held

	return overlay
}
