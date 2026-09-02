package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/stretchr/testify/require"
)

func TestValidateSourceRefusesOversizedParallelAtomicBlock(t *testing.T) {
	var source strings.Builder
	source.WriteString("edition: v2026.3\nname: oversized-parallel\nsteps:\n  - id: block\n    parallel:\n")
	for branch := range 51 {
		source.WriteString("      - steps:\n")
		for step := range 100 {
			fmt.Fprintf(&source, "          - id: b%d_s%d\n            log:\n              message: skipped\n            if: ${false}\n", branch, step)
		}
	}

	diagnostics, err := flowfile.ValidateSource([]byte(source.String()))
	require.NoError(t, err)
	require.Contains(t, diagnostics.Error(),
		`step "block": `+v1.AtomicBlockBodyActivitiesError(v1.MaxAtomicBlockActivities).Error())
}
