package flowfile_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// This file is the reproduction and regression pinning for #334.
//
// The premise the issue raised was that the compiler's document-nesting bound
// (maxDepth, 64) and the schema walks' structure-nesting bound
// ([v1.MaxStructureDepth], 32) disagree, and that a Flowfile the compiler
// accepts could compile a [v1.Value_Structure] deeper than a schema-layer
// walk — the secret authority walk, Continue-As-New reference collection —
// can fully inspect. That premise held: document nesting and structure
// nesting share one YAML depth budget while compiling, but a `headers:`
// value that is nothing but nested one-element lists spends only one level
// of document depth per level of structure nesting, so 64 levels of document
// budget comfortably admits a structure past 32 while the surrounding
// document (workflow, step, task, input) has consumed only a handful of that
// budget itself. Before this fix, depth 40 and depth 55 both compiled; now
// both are refused, and depth 32 still compiles and is fully inspectable.

// nestedListSecretHeader returns a Flowfile whose `headers.H` is a secret
// reference wrapped in depth levels of one-element YAML lists, so the
// compiled value is a [v1.Value_Structure] nested depth levels deep.
func nestedListSecretHeader(depth int) string {
	val := `"${secret('env:API_KEY')}"`
	for range depth {
		val = "[" + val + "]"
	}
	return `edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: "https://example.com"
      headers:
        H: ` + val + "\n"
}

// headerStructureDepth walks the compiled `headers` value counting
// [v1.Value_Structure] levels below its top-level map — the map entry itself
// (depth 1, for `H:`) plus every nested one-element list beneath it — the
// same count [v1.MaxStructureDepth]'s own doc measures.
func headerStructureDepth(v *v1.Value) int {
	depth := 0
	for {
		structure := v.GetStructure()
		if structure == nil {
			return depth
		}

		switch kind := structure.GetKind().(type) {
		case *v1.Value_Structure_Map_:
			entries := kind.Map.GetEntries()
			next, ok := entries["H"]
			if !ok {
				return depth
			}
			v = next
		case *v1.Value_Structure_List_:
			if len(kind.List.GetValues()) == 0 {
				return depth
			}
			v = kind.List.GetValues()[0]
		default:
			return depth
		}
		depth++
	}
}

// TestNestedListStructureWithinTheBoundCompilesAndIsFullyInspectable pins the
// accept side of the boundary: a structure at exactly [v1.MaxStructureDepth]
// must compile, and the reference at the bottom of it must still be visible
// to the walk that names which secrets a step reads — the naming surface a
// walk that merely detects (rather than fully inspects) would not need to
// get right.
func TestNestedListStructureWithinTheBoundCompilesAndIsFullyInspectable(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(nestedListSecretHeader(v1.MaxStructureDepth - 1)))
	require.NoError(t, err)

	headers := wf.GetSteps()[0].GetTask().GetInputs()["headers"]
	require.Equal(t, v1.MaxStructureDepth, headerStructureDepth(headers))
	require.Equal(t, "env:API_KEY", strings.Join(v1.SecretRefsIn(wf.GetSteps()[0].GetTask()), ","),
		"a reference exactly at the bound must be named, which only holds if the walk reaches it")
}

// TestNestedListStructurePastTheBoundIsRefused is the reproduction: a
// Flowfile whose `headers.H` nests one level past [v1.MaxStructureDepth],
// still nowhere near the document's own 64-level bound, is refused at
// compile time with a diagnostic naming the bound — rather than compiling
// into a [v1.Value_Structure] that the secret-authority and Continue-As-New
// walks cannot fully inspect.
func TestNestedListStructurePastTheBoundIsRefused(t *testing.T) {
	t.Parallel()

	for _, depth := range []int{v1.MaxStructureDepth + 1, 40, 55} {
		t.Run("depth-"+strconv.Itoa(depth), func(t *testing.T) {
			t.Parallel()

			_, err := flowfile.Unmarshal([]byte(nestedListSecretHeader(depth)))
			require.Error(t, err, "a Flowfile compiling a structure past the bound must be refused")
			require.Contains(t, err.Error(), "32")
			require.Contains(t, err.Error(), "nests a structure")
		})
	}
}
