package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReplacesOnlyAfterSuccessfulGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "generated.txt")
	require.NoError(t, os.WriteFile(path, []byte("current"), 0o600))

	want := errors.New("generation failed")
	err := write(path, func(out io.Writer) error {
		_, writeErr := io.WriteString(out, "partial")
		require.NoError(t, writeErr)
		return want
	})
	require.ErrorIs(t, err, want)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "current", string(got))

	require.NoError(t, write(path, func(out io.Writer) error {
		_, err := io.WriteString(out, "next")
		return err
	}))
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "next", string(got))
}
