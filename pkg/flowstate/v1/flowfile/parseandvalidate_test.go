package flowfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestParseAndValidateFileAgreesWithItsTwoHalves: the one-pass entry (#1795)
// hands back exactly the workflow ParseFile compiles and exactly the
// diagnostics ValidateSourceFile reports, on every shipped example and on the
// three ways a file can fail — a diagnostic the compiler accepts but the
// validator refuses, a file that does not compile, and an old edition the
// rewrite path reports beyond the gate.
func TestParseAndValidateFileAgreesWithItsTwoHalves(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples were found, so this test proves nothing")

	dir := t.TempDir()
	write := func(name, source string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
		return path
	}
	paths = append(paths,
		// Compiles, and validation objects: a reference to a step that does not exist.
		write("dangling.yaml", "edition: v2026.3\nname: dangling\nsteps:\n  - id: a\n    value: ${steps.ghost.value}\n"),
		// Does not compile, with a step id the validator has something to add about.
		write("broken.yaml", "edition: v2026.3\nname: broken\nsteps:\n  - id: in\n    value: ${steps.in.value +}\n"),
		// An edition this build rewrites, with a mistake below the gate that only
		// the rewrite path reports.
		write("old.yaml", "edition: v2026.1\nname: old\nsteps:\n  - id: a\n    value: ${vars.a +}\n"),
		// Not a Flowfile at all.
		write("not-yaml.yaml", "steps: ["),
	)

	for _, path := range paths {
		t.Run(filepath.Base(filepath.Dir(path))+"/"+filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			workflow, diagnostics, err := flowfile.ParseAndValidateFile(path)

			parsed, _, parseErr := flowfile.ParseFile(path)
			validated, validateErr := flowfile.ValidateSourceFile(path)

			if parseErr != nil || validateErr != nil {
				require.Error(t, err, "the halves refuse the file and the one pass accepted it")
				require.Error(t, validateErr, "the file compiles for ValidateSourceFile but not for the one pass")
				assert.Equal(t, validateErr.Error(), err.Error(),
					"the one pass refuses the file differently from `flow validate`")
				assert.Nil(t, workflow)
				assert.Nil(t, diagnostics)
				return
			}

			require.NoError(t, err)
			assert.True(t, proto.Equal(parsed, workflow), "the one pass compiled a different workflow")
			assert.Equal(t, validated, diagnostics, "the one pass reported different diagnostics")
		})
	}
}
