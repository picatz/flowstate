package server

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/temporal"
)

// TestOnlyARunsOwnEndIsAFailureOfTheRun: a waiting delivery answers `failed`
// only for an error that says the run ended. A transport or decode error is
// about the wait, so it is answered `running` and the sender is never told a
// healthy run failed.
func TestOnlyARunsOwnEndIsAFailureOfTheRun(t *testing.T) {
	t.Parallel()

	cause := temporal.NewApplicationError("the ledger refused", "")

	assert.True(t, runEnded(&temporal.WorkflowExecutionError{}))
	assert.True(t, runEnded(fmt.Errorf("wrapped: %w", &temporal.WorkflowExecutionError{})))

	assert.False(t, runEnded(errors.New("connection reset by peer")))
	assert.False(t, runEnded(fmt.Errorf("decode result: %w", cause)))
	assert.False(t, runEnded(nil))
}
