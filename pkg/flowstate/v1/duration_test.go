package flowstatev1

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParseDurationManyDayComponents is the regression for a duration whose
// accepted-size input used to make expandDays repeatedly copy its entire prefix.
// The allocation bound is deliberately generous; it distinguishes bounded
// buffer growth from one allocation per component without depending on timing.
func TestParseDurationManyDayComponents(t *testing.T) {
	const components = 4_000
	input := strings.Repeat("1d", components)

	var (
		got time.Duration
		err error
	)
	allocs := testing.AllocsPerRun(1, func() {
		got, err = ParseDuration(input)
	})
	require.NoError(t, err)
	require.Equal(t, time.Duration(components)*24*time.Hour, got)
	require.Less(t, allocs, float64(100), "day expansion allocated with the number of components")
}
