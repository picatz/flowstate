package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestAReplayIsSilentUntilItIsTheRunShown: the caller was shown a case's
// account the first time, so a rewind's re-execution says nothing until it
// replaces the run before it, and a stopped run never speaks again.
func TestAReplayIsSilentUntilItIsTheRunShown(t *testing.T) {
	t.Parallel()

	initial := &stubbedRun{initial: true, finish: func(*v1.TestReport) {}}
	replay := &stubbedRun{finish: func(*v1.TestReport) {}}

	assert.True(t, initial.speaks(), "the first run is the caller's from its first word")
	assert.False(t, replay.speaks(), "a replay spoke before it was shown")

	replay.shown()
	assert.True(t, replay.speaks(), "a shown replay is silent")

	replay.stopped.Store(true)
	assert.False(t, replay.speaks(), "a stopped run spoke")
}
