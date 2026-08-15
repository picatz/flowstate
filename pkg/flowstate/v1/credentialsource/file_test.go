package credentialsource_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

func TestFileSource_ReadsFreshOnEveryCall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("first-token\n"), 0o600))

	source := credentialsource.NewFileSource(path)

	token, err := source.Token(t.Context())
	require.NoError(t, err)
	raw, ok := token.Bearer()
	require.True(t, ok)
	assert.Equal(t, "first-token", raw, "trailing newline must be trimmed")

	// The rotation case this Source exists for: Kubernetes rewrites the file
	// in place, and the next call must see the new content without anything
	// having been cached.
	require.NoError(t, os.WriteFile(path, []byte("rotated-token"), 0o600))

	token, err = source.Token(t.Context())
	require.NoError(t, err)
	raw, ok = token.Bearer()
	require.True(t, ok)
	assert.Equal(t, "rotated-token", raw)
}

// TestFileSource_MissingFile_FailsClosed is the negative direction: a
// configured file that is not there must be an error, never a Source that
// silently reports "no credential" the way an unconfigured one legitimately
// can.
func TestFileSource_MissingFile_FailsClosed(t *testing.T) {
	source := credentialsource.NewFileSource(filepath.Join(t.TempDir(), "does-not-exist"))

	token, err := source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	assert.True(t, token.IsZero())
}

func TestFileSource_EmptyFile_FailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty")
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	source := credentialsource.NewFileSource(path)

	_, err := source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
}

// TestFileSource_OversizedFile_FailsClosed proves the bound is enforced, per
// this repository's rule that anything reading from a file is bounded by
// bytes.
func TestFileSource_OversizedFile_FailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "huge")
	oversized := strings.Repeat("a", credentialsource.MaxFileTokenBytes+1)
	require.NoError(t, os.WriteFile(path, []byte(oversized), 0o600))

	source := credentialsource.NewFileSource(path)

	_, err := source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
}
