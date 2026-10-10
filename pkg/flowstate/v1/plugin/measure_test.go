package plugin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// writeFakePlugin installs bytes that are not a runnable program, so that a
// measurement which launched anything would fail loudly.
func writeFakePlugin(t *testing.T, dir, name, body string) string {
	t.Helper()

	path := filepath.Join(dir, BinaryPrefix+name)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o700))

	return path
}

func TestMeasureDistributionsHashesWithoutLaunching(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	writeFakePlugin(t, dir, "alpha", "not an executable image\n")
	writeFakePlugin(t, dir, "beta", "another\n")

	got, err := MeasureDistributions(Config{SearchPath: []string{dir}})
	require.NoError(t, err)

	want, err := flowstatev1.ContentDigestOf(strings.NewReader("not an executable image\n"))
	require.NoError(t, err)
	assert.Equal(t, want, got["alpha"])
	assert.Len(t, got, 2)
	assert.NotEqual(t, got["alpha"], got["beta"])

	only, err := MeasureDistributions(Config{SearchPath: []string{dir}, Only: []string{"beta"}})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"beta": got["beta"]}, only)
}

func TestMeasureDistributionsSeesAChangedBinary(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	path := writeFakePlugin(t, dir, "alpha", "v1")

	before, err := MeasureDistributions(Config{SearchPath: []string{dir}})
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("v2"), 0o700))

	after, err := MeasureDistributions(Config{SearchPath: []string{dir}})
	require.NoError(t, err)
	assert.NotEqual(t, before["alpha"], after["alpha"])
}

func TestValidatePinsAppliesTheHostCheck(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidatePins(nil))
	require.ErrorIs(t, ValidatePins(map[string]string{"GitHub": "sha256:" + strings.Repeat("ab", 32)}), ErrDigestPin)
	require.ErrorIs(t, ValidatePins(map[string]string{"github": "sha256:short"}), ErrDigestPin)
	require.NoError(t, ValidatePins(map[string]string{"github": "sha256:" + strings.Repeat("ab", 32)}))
}
