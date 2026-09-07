package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReadBoundedFileHoldsTheBoundAndNamesTheKind pins the helper every
// whole-file read in this package goes through: a file at the bound is read,
// one byte over is refused naming the kind and the bound, a directory is
// refused as not a regular file, and an absent file is still recognisable as
// absent through errors.Is, which `flow docs`'s walk up the tree relies on.
func TestReadBoundedFileHoldsTheBoundAndNamesTheKind(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const max = 64

	at := filepath.Join(dir, "at")
	require.NoError(t, os.WriteFile(at, []byte(strings.Repeat("a", max)), 0o600))
	data, err := readBoundedFile(at, "a fixture", max)
	require.NoError(t, err)
	require.Len(t, data, max, "a file of exactly the bound is read whole")

	over := filepath.Join(dir, "over")
	require.NoError(t, os.WriteFile(over, []byte(strings.Repeat("a", max+1)), 0o600))
	_, err = readBoundedFile(over, "a fixture", max)
	require.Error(t, err)
	require.Contains(t, err.Error(), "larger than the 64 byte limit a fixture is read up to")

	_, err = readBoundedFile(dir, "a fixture", max)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a regular file")
	require.Contains(t, err.Error(), "a fixture is read as bytes")

	_, err = readBoundedFile(filepath.Join(dir, "absent"), "a fixture", max)
	require.True(t, errors.Is(err, os.ErrNotExist), "an absent file is reported as os.Open reports it: %v", err)
}
