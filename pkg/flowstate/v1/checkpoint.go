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

	// ErrCheckpointUnsupported reports a position this contract does not yet
	// admit: inside a call, a loop, or concurrent work.
	ErrCheckpointUnsupported = errors.New("flowstate: checkpoint position is not a fork-eligible boundary")

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
	return verifyCheckpointPosition(cp)
}

// verifyCheckpointPosition holds the position to the one shape this contract
// admits: between two top-level steps, with no frame standing inside a call, a
// loop or a for_each. Deeper positions carry callee outputs and iteration
// results that a patch could contradict; admitting them waits for the
// interpreter to define fork-eligible boundaries once (#2249), so they are
// refused here rather than half-checked. The cursor must also name the step the
// origin said it was about to run, so a position edited to rewind past an
// executed step is caught.
func verifyCheckpointPosition(cp *Checkpoint) error {
	state := cp.GetState()
	steps := state.GetWorkflow().GetSteps()
	pos := checkpointPosition(state)
	if pos < 0 || pos > len(steps) {
		return fmt.Errorf("%w: position %d is outside the workflow's %d steps", ErrCheckpointTampered, pos, len(steps))
	}
	if frames := state.GetFrames(); len(frames) > 1 || (len(frames) == 1 && !proto.Equal(frames[0], &Frame{NextNode: int32(pos)})) {
		return fmt.Errorf("%w: only a position between top-level steps can be checkpointed", ErrCheckpointUnsupported)
	}
	next := ""
	if pos < len(steps) {
		next = steps[pos].GetId()
	}
	if next != cp.GetOrigin().GetStep() {
		return fmt.Errorf("%w: position %d is step %q, the origin recorded %q", ErrCheckpointTampered, pos, next, cp.GetOrigin().GetStep())
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
	if err := Validate(state); err != nil {
		return nil, fmt.Errorf("patched workflow: %w", err)
	}
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
