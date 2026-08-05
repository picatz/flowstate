package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestLoadAcceptsAnEditionMarker is the regression for the second half of
// issue #203: `flow fix` stamps `edition:` into any document it recognizes as
// a Flowfile or a Flowfile test, and a `*.test.yaml` is the latter. Before
// [flowtest.File] carried an Edition field, that stamp landed on a struct
// parsed with `yaml.Strict()` — the same fail-closed parsing the issue's
// egress-policy example was refused over — so migrating
// examples/call-a-workflow/workflow.test.yaml forward would have reproduced
// #203's exact failure while "fixing" #203's own drift example: `flow fix`
// exits 0, and the next `flow test` on the file it just edited fails with
// `unknown field "edition"`.
func TestLoadAcceptsAnEditionMarker(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "x.test.yaml")
	writeFile(t, path, `edition: v2026.2
tests:
  - name: a case
    workflow: ./workflow.yaml
    expect: {}
`)

	file, err := flowtest.Load(path)
	require.NoError(t, err, "a *.test.yaml carrying the edition flow fix stamps must still load")
	require.Len(t, file.Tests, 1)
	require.Equal(t, "v2026.2", file.Edition)
}
