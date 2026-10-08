package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestARecordedSessionReplaysToTheSameStops: what --record writes is the
// session, so replaying it answers byte for byte what the typed session did. A
// mistyped command and a refused break are not in it.
func TestARecordedSessionReplaysToTheSameStops(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	recording := filepath.Join(t.TempDir(), "session.script")

	typed := runFlowStdin(t, "break second\nbogus\nbreak nosuchstep\ncontinue\ninspect steps.first\ncontinue\n",
		"run", "local", path, "--debug", "--record", recording)
	require.NoError(t, typed.Err)

	got, err := os.ReadFile(recording)
	require.NoError(t, err)
	assert.Equal(t, "break second\ncontinue\ninspect steps.first\ncontinue\n", string(got),
		"the recording holds a line the session did not accept, or lost one it did")

	info, err := os.Stat(recording)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "a recording can name anything in scope")

	// The typed session's account also narrates the two refusals, which the
	// recording rightly leaves out, so the comparison is with the session that
	// had only the accepted commands.
	accepted := runFlowStdin(t, "break second\ncontinue\ninspect steps.first\ncontinue\n", "run", "local", path, "--debug")
	require.NoError(t, accepted.Err)

	replayed := runFlow(t, "debug", "replay", recording, path)
	require.NoError(t, replayed.Err)
	assert.Equal(t, accepted.Stderr, replayed.Stderr, "the recording reached different stops than the session it recorded")
	assert.Equal(t, accepted.Stdout, replayed.Stdout)
}

// TestARecordedSessionIsWrittenWhenItIsQuit: an abandoned session is the one
// somebody most wants to look at again.
func TestARecordedSessionIsWrittenWhenItIsQuit(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	recording := filepath.Join(t.TempDir(), "session.script")

	runFlowStdin(t, "step\nquit\n", "run", "local", path, "--debug", "--record", recording)

	got, err := os.ReadFile(recording)
	require.NoError(t, err, "no recording was written on the quit path")
	assert.Equal(t, "step\nquit\n", string(got))
}

func TestRecordNeedsADebugSession(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	recording := filepath.Join(t.TempDir(), "session.script")

	res := runFlow(t, "run", "local", path, "--record", recording)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--record")
	assert.NoFileExists(t, recording)
}

// TestARecordingNarrowsAFileThatWasAlreadyThere: a write keeps an existing
// file's mode, and the file holds typed expressions.
func TestARecordingNarrowsAFileThatWasAlreadyThere(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	recording := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(recording, []byte("stale\n"), 0o644))

	runFlowStdin(t, "step\nquit\n", "run", "local", path, "--debug", "--record", recording)

	info, err := os.Stat(recording)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

// TestReplayRefusesToRecordOverItsOwnScript: the script is read first and the
// recording written last, so a quit replay would truncate its source.
func TestReplayRefusesToRecordOverItsOwnScript(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	const script = "break second\ncontinue\ncontinue\n"
	scriptFile := writeDebugScript(t, script)

	res := runFlow(t, "debug", "replay", scriptFile, path, "--record", scriptFile)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--record")

	got, err := os.ReadFile(scriptFile)
	require.NoError(t, err)
	assert.Equal(t, script, string(got), "the script was written over")
}
