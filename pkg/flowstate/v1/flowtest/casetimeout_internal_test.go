package flowtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCaseTimeoutForClamps pins the three regions: zero takes the default,
// a request in range is honoured, and one past the ceiling is cut to it.
func TestCaseTimeoutForClamps(t *testing.T) {
	t.Parallel()

	require.Equal(t, maxCaseWallTime, caseTimeoutFor(0))
	require.Equal(t, maxCaseWallTime, caseTimeoutFor(-time.Second))
	require.Equal(t, 5*time.Second, caseTimeoutFor(5*time.Second))
	require.Equal(t, maxCaseTimeout, caseTimeoutFor(maxCaseTimeout))
	require.Equal(t, maxCaseTimeout, caseTimeoutFor(24*time.Hour), "a Go caller cannot lift the ceiling")
}
