package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestLoadingAWorkflowCompilesItOnce pins #1795: loadWorkflow allocates what
// validating the file allocates, not that plus a second compile. It used to
// call ParseFile and then ValidateSourceFile, and the second compiled the file
// again from its bytes, so a load cost a parse more than a validation did —
// 2.3× a parse against 1.3× on this fixture. Allocations rather than time,
// because they are deterministic under `-race` and on a loaded runner.
func TestLoadingAWorkflowCompilesItOnce(t *testing.T) {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: chain\nsteps:\n  - id: s0\n    value: 0\n")
	for i := 1; i < 200; i++ {
		fmt.Fprintf(&b, "  - id: s%d\n    value: ${steps.s%d.value + 1}\n", i, i-1)
	}
	path := filepath.Join(t.TempDir(), "chain.yaml")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o600))

	parse := testing.AllocsPerRun(5, func() {
		if _, _, err := flowfile.ParseFile(path); err != nil {
			t.Error(err)
		}
	})
	validate := testing.AllocsPerRun(5, func() {
		if _, err := flowfile.ValidateSourceFile(path); err != nil {
			t.Error(err)
		}
	})
	load := testing.AllocsPerRun(5, func() {
		if _, err := loadWorkflow(path); err != nil {
			t.Error(err)
		}
	})

	require.Greater(t, parse, 0.0)
	assert.LessOrEqual(t, load, validate*1.1,
		"loading allocates %.0f where validating alone allocates %.0f and a parse %.0f; the load is compiling the file twice", load, validate, parse)
}
