package flowdebug

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestARefusedSetBreakpointsKeepsTheSetItHad checks the ordering the method
// depends on: the inventory check runs before anything is replaced.
//
// The set is reachable only from inside the package, and the property is worth
// pinning here rather than inferring from the code's order — a later edit that
// moved the check below the critical section would leave a client's markers and
// the session's breakpoints disagreeing, which is the same class of silence
// #1367 is about.
func TestARefusedSetBreakpointsKeepsTheSetItHad(t *testing.T) {
	t.Parallel()

	session, err := New(Options{
		Controlled:  true,
		Out:         io.Discard,
		Steps:       []Step{{ID: "build"}, {ID: "deploy"}},
		Breakpoints: []string{"build"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	require.Error(t, session.SetBreakpoints([]string{"deploy", "deploi"}))

	session.mu.Lock()
	_, held := session.breakpoints["build"]
	count := len(session.breakpoints)
	session.mu.Unlock()

	require.True(t, held, "the breakpoint the session already held was dropped by a refused call")
	require.Equal(t, 1, count, "a refused call must not install any part of the new set")
}
