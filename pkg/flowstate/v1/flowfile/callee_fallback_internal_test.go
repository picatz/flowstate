package flowfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const (
	fallbackCaller = "edition: v2026.4\nname: caller\nsteps:\n  - id: place\n    call: ./lib/callee.yaml\n"
	fallbackCallee = "edition: v2026.4\nname: callee\ninputs:\n  shards:\n    type: int\n    default: 0\n    must: this >= 1\nsteps:\n  - id: work\n    log:\n      message: hi\n"
)

func fallbackFixture(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "lib"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(fallbackCaller), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib", "callee.yaml"), []byte(fallbackCallee), 0o644))

	return filepath.Join(dir, "workflow.yaml")
}

// A callee the compile did not build (the workflow was cloned between compile
// and validate) cannot be placed in its file, and must still be said: at the call
// step, with the callee's name in front, exactly as it was before callees had
// files of their own.
func TestCalleeProblemWithoutItsFrameIsReportedAtTheCallStep(t *testing.T) {
	path := fallbackFixture(t)
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	wf, positions, err := parse(Env{}, data, path, nil, new(int), nil)
	require.NoError(t, err)
	require.NotEmpty(t, positions.callees, "the compile records the callee it built")

	t.Run("the compiled workflow is placed in the callee's file", func(t *testing.T) {
		ds, err := validateParsed(Env{}, wf, positions, nil)
		require.NoError(t, err)

		var placed []Diagnostic
		for _, d := range ds {
			if d.File != "" {
				placed = append(placed, d)
			}
		}
		require.Len(t, placed, 1)
		require.Equal(t, filepath.Join(filepath.Dir(path), "lib", "callee.yaml"), placed[0].File)
		require.Equal(t, [2]int{6, 14}, [2]int{placed[0].Line, placed[0].Column})
	})

	t.Run("a clone has no frame and keeps the old report", func(t *testing.T) {
		clone := proto.Clone(wf).(*v1.Workflow)

		ds, err := validateParsed(Env{}, clone, positions, nil)
		require.NoError(t, err)

		require.NotEmpty(t, ds)
		for _, d := range ds {
			require.Empty(t, d.File, "nothing can be placed in a file the compile did not record: %v", d)
			require.Equal(t, "place", d.Step)
		}
		var prefixed []Diagnostic
		for _, d := range ds {
			if d.Field == "inputs.shards.default" {
				prefixed = append(prefixed, d)
			}
		}
		require.Len(t, prefixed, 1, "the callee's problem is not lost: %v", ds)
		require.Contains(t, prefixed[0].Message, `workflow "callee": `)
		require.Equal(t, 4, prefixed[0].Line, "positioned at the call step, where the old report put it")
		require.Equal(t, []string{"place"}, prefixed[0].Path, "and takes its path")
	})

	t.Run("a workflow with no positions at all keeps the old report", func(t *testing.T) {
		ds := Env{}.Validate(proto.Clone(wf).(*v1.Workflow))

		require.NotEmpty(t, ds)
		for _, d := range ds {
			require.Empty(t, d.File)
			require.Equal(t, "place", d.Step)
		}
		require.Contains(t, ds.Error(), `workflow "callee": input "shards" must satisfy`)
	})

	t.Run("a file validated by name never reaches the fallback", func(t *testing.T) {
		ds, err := ValidateSourceFile(path)
		require.NoError(t, err)

		require.NotEmpty(t, ds)
		for _, d := range ds {
			require.NotContains(t, d.Message, `workflow "callee":`, "%v", d)
		}
	})
}
