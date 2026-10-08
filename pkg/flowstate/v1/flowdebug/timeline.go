package flowdebug

import (
	"errors"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// MaxTimelinePoints bounds the stops one [v1.DebugTimeline] carries: the same
// bound the schema puts on its points. A target that has shown more evicts the
// oldest and says how many in [v1.DebugTimeline.Dropped].
const MaxTimelinePoints = 1024

// heldStop is one stop a [Session] held, kept for its timeline. The occurrence
// is as the engine named it; the snapshot redacts it as it does its own.
type heldStop struct {
	revision   uint64
	occurrence *v1.DebugOccurrence
	reason     v1.DebugStopReason
}

// errCannotTravel is what a session that was not built to be replayed from its
// start, or read from a recorded history, says to `goto`, at its prompt and
// through a [Driver] alike.
var errCannotTravel = errors.New("this session cannot go to a point on its timeline: only a run replayed from its start, or a recorded history, can")
