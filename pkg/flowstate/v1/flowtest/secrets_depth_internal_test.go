package flowtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A reference nested past the scan's depth bound must still be refused: the
// gate answers may-hold past the bound (v1.ValueHoldsSecretRef), so the
// resolver runs and its own depth refusal fires. Before the gate answered
// conservatively, depth 33 was invisible and a case passed with no `secrets:`
// entry at all, the same fail-open the shallower tests close at depths one
// and two. Internal because the depth cannot be written in a YAML fixture a
// reader could take in at a glance; the construction is the point.
func TestASecretRefPastTheScanDepthIsRefused(t *testing.T) {
	t.Parallel()

	deep := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "TOKEN"}}}
	for range 40 {
		deep = &v1.Value{Kind: &v1.Value_Structure_{Structure: &v1.Value_Structure{
			Kind: &v1.Value_Structure_Map_{Map: &v1.Value_Structure_Map{
				Entries: map[string]*v1.Value{"k": deep},
			}},
		}}}
	}

	_, err := resolveSecretInputs(context.Background(), map[string]*v1.Value{"json": deep})
	require.Error(t, err, "a ref hidden past the scan depth must not fold in silently")
	require.Contains(t, err.Error(), "nested more than", "the refusal names the depth bound")
}
