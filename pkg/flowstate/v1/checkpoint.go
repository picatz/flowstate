package flowstatev1

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// CheckpointVersion is the only [Checkpoint] format this package reads and
// writes.
const CheckpointVersion = 1

// Refusals a resume gives. They are sentinels so a caller (an RPC handler, the
// CLI) can tell "this checkpoint is not for this interpreter" from "this patch
// rewrites history" without parsing a message.
var (
	// ErrCheckpointTampered reports a checkpoint whose recorded digest or
	// position does not match the state it carries.
	ErrCheckpointTampered = errors.New("flowstate: checkpoint does not match its state")

	// ErrCheckpointBuild reports a checkpoint emitted by a different
	// interpreter build than the one asked to resume it.
	ErrCheckpointBuild = errors.New("flowstate: checkpoint was emitted by a different interpreter build")

	// ErrCheckpointPatch reports a patch that changes a step the origin run had
	// already executed, or that removes a step the position still points past.
	ErrCheckpointPatch = errors.New("flowstate: patch rewrites steps the origin run already executed")
)

// NewCheckpoint wraps state, taken at a boundary the interpreter could have
// continued as new from, into a [Checkpoint] linked to origin.
//
// The state is cloned, so later mutation of the caller's copy cannot change
// what the checkpoint vouches for. The result is validated and size-checked
// before it is returned: an invalid or oversize checkpoint is never emitted.
func NewCheckpoint(state *RunState, build string, origin *CheckpointOrigin) (*Checkpoint, error) {
	cp := &Checkpoint{
		Version:          CheckpointVersion,
		State:            proto.Clone(state).(*RunState),
		SpecHash:         CanonicalDigest(state.GetWorkflow()),
		InterpreterBuild: build,
		Origin:           origin,
	}
	if err := cp.Verify(); err != nil {
		return nil, err
	}

	return cp, nil
}

// Verify reports whether the checkpoint is well formed and consistent with
// itself: its schema rules hold, the state fits [MaxRunStateBytes], the
// recorded spec hash is the digest of the carried workflow, and the position
// lies inside that workflow. It does not decide whether any build may resume
// it; see [Checkpoint.Resume].
func (cp *Checkpoint) Verify() error {
	if err := Validate(cp); err != nil {
		return err
	}
	if err := CheckRunStateSize(cp.GetState()); err != nil {
		return err
	}
	if got := CanonicalDigest(cp.GetState().GetWorkflow()); got == "" || got != cp.GetSpecHash() {
		return fmt.Errorf("%w: spec hash %q is not the digest of the carried workflow", ErrCheckpointTampered, cp.GetSpecHash())
	}
	if pos, steps := checkpointPosition(cp.GetState()), len(cp.GetState().GetWorkflow().GetSteps()); pos > steps {
		return fmt.Errorf("%w: position %d is past the workflow's %d steps", ErrCheckpointTampered, pos, steps)
	}

	return nil
}

// Resume returns the [RunState] a new run starts from. build is the resuming
// interpreter's own build name. When patch is non-nil it replaces the workflow,
// provided every top-level step before the checkpoint's position is unchanged:
// the origin run executed those, and a program that disagrees about them would
// continue from a past that never happened. Everything else in the state (the
// inputs, identity, trigger, vars, outputs, pending signals and undo
// registrations) is carried exactly; nothing here offers a way to alter it.
//
// The returned state is a copy, so the checkpoint can be resumed again.
func (cp *Checkpoint) Resume(build string, patch *Workflow) (*RunState, error) {
	if err := cp.Verify(); err != nil {
		return nil, err
	}
	if cp.GetInterpreterBuild() != build {
		return nil, fmt.Errorf("%w: emitted by %q, resumed by %q", ErrCheckpointBuild, cp.GetInterpreterBuild(), build)
	}

	state := proto.Clone(cp.GetState()).(*RunState)
	if patch == nil {
		return state, nil
	}

	pos := checkpointPosition(state)
	steps := patch.GetSteps()
	if len(steps) < pos {
		return nil, fmt.Errorf("%w: patch has %d steps, the position is after step %d", ErrCheckpointPatch, len(steps), pos)
	}
	for i, executed := range state.GetWorkflow().GetSteps()[:pos] {
		if !proto.Equal(executed, steps[i]) {
			return nil, fmt.Errorf("%w: step %d (%q) differs", ErrCheckpointPatch, i, executed.GetId())
		}
	}
	state.Workflow = proto.Clone(patch).(*Workflow)
	if err := CheckRunStateSize(state); err != nil {
		return nil, err
	}

	return state, nil
}

// checkpointPosition is the index of the next top-level step: the outermost
// frame when there is one, else the legacy next_step.
func checkpointPosition(state *RunState) int {
	if frames := state.GetFrames(); len(frames) > 0 {
		return int(frames[0].GetNextNode())
	}

	return int(state.GetNextStep())
}
