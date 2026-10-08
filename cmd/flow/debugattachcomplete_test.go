package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// breakpointTarget is an attached run that holds one breakpoint and refuses
// every inspection, the way a caller without the durable inspect action is
// answered.
type breakpointTarget struct {
	flowdebug.Target

	snapshot func(context.Context) (*v1.DebugSnapshot, error)
}

func (b breakpointTarget) Snapshot(ctx context.Context) (*v1.DebugSnapshot, error) {
	return b.snapshot(ctx)
}

func (breakpointTarget) Inspect(context.Context, *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	return &v1.DebugInspectResponse{Error: "not authorized"}, nil
}

func TestAnAttachConsoleCompletesWhatTheRunSaysOfItself(t *testing.T) {
	t.Parallel()

	target := breakpointTarget{snapshot: func(context.Context) (*v1.DebugSnapshot, error) {
		return &v1.DebugSnapshot{Revision: 1, Breakpoints: []*v1.DebugBreakpointState{{Id: "price"}}}, nil
	}}
	complete := attachCompleter(t.Context(), flowdebug.NewDriver(target))

	// The text before the cursor is what is completed, not the rest of the line.
	answer := complete("delete pr and more", len("delete pr"))
	require.Len(t, answer.Candidates, 1)
	assert.Equal(t, "price", answer.Candidates[0].Text)
	assert.Equal(t, "pr", answer.Prefix)

	// A caller the target will not let inspect still gets its commands.
	verbs := complete("bre", 3)
	require.NotEmpty(t, verbs.Candidates)
	assert.Equal(t, "break ", verbs.Candidates[0].Text)

	// And no names it was refused.
	for _, candidate := range complete("inspect ", len("inspect ")).Candidates {
		assert.NotEqual(t, "inputs", candidate.Text, "an inspection the target refused still offered a name")
	}
}

func TestAnAttachConsoleDoesNotHoldTheTerminalForASlowTarget(t *testing.T) {
	t.Parallel()

	target := breakpointTarget{snapshot: func(ctx context.Context) (*v1.DebugSnapshot, error) {
		<-ctx.Done()

		return nil, errors.Join(errors.New("slow"), ctx.Err())
	}}
	complete := attachCompleter(t.Context(), flowdebug.NewDriver(target))

	started := time.Now()
	answer := complete("break ", len("break "))

	assert.Empty(t, answer.Candidates, "a target that did not answer offered something")
	assert.Less(t, time.Since(started), completionTimeout+2*time.Second, "the keystroke waited past its bound")
}

func TestOnlyAFailedTerminalReadIsAnErrorAtTheAttachPrompt(t *testing.T) {
	t.Parallel()

	assert.NoError(t, unexpectedPromptError(nil))
	assert.NoError(t, unexpectedPromptError(io.EOF), "ctrl-D ends the session")
	assert.NoError(t, unexpectedPromptError(flowdebug.ErrConsoleInterrupted), "ctrl-C ends the session")
	assert.NoError(t, unexpectedPromptError(fmt.Errorf("wrapped: %w", io.EOF)))

	failed := errors.New("input/output error")
	assert.ErrorIs(t, unexpectedPromptError(failed), failed, "a terminal that cannot be read is not the person leaving")
}
