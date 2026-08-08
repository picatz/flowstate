package netpolicy

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestShippedEgressPolicyExampleBuilds compiles the egress policy example the
// repository ships. ParseConfig only unmarshals; a CEL rule in the file is compiled
// only when the policy is built, so a rule that names an attribute wrong — an
// `identity.tenant` that should be `identity.namespace` — parses cleanly and fails
// only here. Building the shipped example is what keeps it a working demonstration
// rather than a plausible-looking one.
func TestShippedEgressPolicyExampleBuilds(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "..", "examples", "egress-policy.yaml")
	data, err := os.ReadFile(path)
	require.NoError(t, err, "locating the shipped egress policy example")

	cfg, err := ParseConfig(data)
	require.NoError(t, err, "the shipped egress policy example must parse")

	_, err = cfg.Policy()
	require.NoError(t, err, "the shipped egress policy example must compile into a policy")
}
