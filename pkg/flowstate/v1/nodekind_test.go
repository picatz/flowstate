package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeKindNamesEveryKind is [TestEveryNodeKindIsCovered]'s guard applied
// to the other hand-written switch over Node.kind: every arm the schema
// defines answers with its own name, never the "step" fallback.
//
// The fallback is reachable and correct for exactly one shape — a node whose
// kind is unset — and for nothing else: a real kind answered as "step" is a
// drift this repository has already shipped once. `loop:` fell to the default
// on every surface that shares this spelling (the debugger prompt's
// `break at countup (step)`, backtraces, DAP stop descriptions, `info`),
// because nothing made the switch answer for a ninth arm the day the schema
// grew one. Reusing [nodeKindBuilders] means a future arm fails here the same
// day it fails the OutputNames guard, with no second fixture to keep current.
func TestNodeKindNamesEveryKind(t *testing.T) {
	t.Parallel()

	schemaKinds := nodeKindOneofNames(t)
	builders := nodeKindBuilders()

	for _, kindName := range schemaKinds {
		node, ok := builders[kindName]
		require.True(t, ok, "no builder for kind %q", kindName)

		t.Run(kindName, func(t *testing.T) {
			t.Parallel()

			assert.NotEqual(t, "step", NodeKind(node),
				"kind %q fell to the fallback: every declared kind names itself, "+
					"on every front that shares this spelling", kindName)
		})
	}

	assert.Equal(t, "step", NodeKind(&Node{}),
		"a node with no kind set is the one shape the fallback is for")
}
