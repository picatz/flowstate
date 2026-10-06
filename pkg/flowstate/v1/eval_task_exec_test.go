package flowstatev1

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
)

// TestAPosixAbsoluteDirIsAbsoluteWhereverTheFlowfileIsAuthored: the literal is
// checked on the author's machine, the directory is on the worker's. A leading
// slash must not be refused because the author's OS says it is not absolute.
func TestAPosixAbsoluteDirIsAbsoluteWhereverTheFlowfileIsAuthored(t *testing.T) {
	t.Parallel()

	windows := func(p string) bool { return len(p) > 2 && p[1] == ':' }

	assert.True(t, isAbsoluteLiteral("/srv/work", windows), "a POSIX path was refused on a Windows author's machine")
	assert.True(t, isAbsoluteLiteral(`C:\work`, windows), "the author's own absolute path was refused")
	assert.False(t, isAbsoluteLiteral("work/dir", windows))
	assert.False(t, isAbsoluteLiteral(`.\work`, windows))
}

// TestAContextThatEndsBeforeTheStartIsNotRetryableAsDidNotStart: os/exec reports
// an expired context from Start as the context's own error. Classified as a
// program that never started it would be retried as an upstream failure; it is
// the step's deadline.
func TestAContextThatEndsBeforeTheStartIsNotRetryableAsDidNotStart(t *testing.T) {
	t.Parallel()

	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh is not installed")
	}
	sh, err = filepath.EvalSymlinks(sh)
	require.NoError(t, err)

	root := t.TempDir()
	policy, err := execpolicy.New(execpolicy.Config{
		Executables:    map[string]string{"sh": sh},
		Roots:          []string{root},
		Timeout:        time.Minute,
		MaxOutputBytes: 1 << 10,
	})
	require.NoError(t, err)

	command, err := policy.Check(context.Background(), execpolicy.Request{Argv: []string{"sh", "-c", "true"}, Dir: root})
	require.NoError(t, err)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err = command.Run(ctx)
	require.Error(t, err)

	failure := execFailure(err)
	assert.NotEqual(t, ErrorKindUpstream, ClassifyError(failure),
		"an expired deadline was classified as a program that did not start, which is retried: %v", failure)
	assert.Equal(t, ErrorKindTimeout, ClassifyError(failure), "%v", failure)
}

// TestExecOutputCeilingFitsTaskOutput holds the exec policy's output ceiling to
// the bound a task result must fit. The policy ceiling is per stream, a result
// carries both streams, and a result is measured as ProtoJSON, which spells a
// control character as six bytes. Both streams at the ceiling and made of the
// worst such byte are therefore the largest answer a policy can promise, and it
// must be accepted: a ceiling the history cannot hold is a limit that lies.
func TestExecOutputCeilingFitsTaskOutput(t *testing.T) {
	t.Parallel()

	worst := string(bytes.Repeat([]byte{0x01}, execpolicy.MaxOutputBytes))
	out, err := nodeOutputsFromProtoMessage(&Task_Exec_Outputs{Stdout: worst, Stderr: worst, ExitCode: 1})
	require.NoError(t, err)
	require.NoError(t, CheckTaskOutputSize(out),
		"two streams at the policy ceiling must fit a task result; lower execpolicy.MaxOutputBytes")

	// The same construction one byte over would not be a useful probe, so show
	// the ceiling is not vacuous: the bound is crossed by a modest multiple.
	big := string(bytes.Repeat([]byte{0x01}, 4*execpolicy.MaxOutputBytes))
	out, err = nodeOutputsFromProtoMessage(&Task_Exec_Outputs{Stdout: big, Stderr: big})
	require.NoError(t, err)
	assert.Error(t, CheckTaskOutputSize(out), "the probe must be able to fail, or it proves nothing")
}
