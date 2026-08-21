package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// mustNew is [New] for a test whose subject is not the construction.
//
// [New] reports an error because an [Option] can refuse — see [WithNamespace],
// which checks the namespace grammar every tenant-scoped derivation in this
// package assumes. A test that means "a server configured like this" should
// stop at the construction with the option's own message rather than nil-panic
// several lines later on something unrelated.
//
// A test whose subject *is* the refusal calls [New] directly and asserts on the
// error; see TestNewRefusesANamespaceOutsideTheGrammar.
//
// testing.TB rather than *testing.T so the conformance-driven tables, whose
// callback takes a TB, can use it too.
func mustNew(t testing.TB, temporalClient client.Client, opts ...Option) *FlowstateServer {
	t.Helper()

	s, err := New(temporalClient, opts...)
	require.NoError(t, err)

	return s
}
