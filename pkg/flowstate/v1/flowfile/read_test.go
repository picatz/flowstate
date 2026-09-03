package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Issue #504: [flowfile.ParseFile], [flowfile.ValidateSourceFile], and the callee
// read in call.go each read a whole file with [os.ReadFile] and checked the
// parser's 1 MiB document limit only after the read returned, so the limit bounded
// what the parser would accept rather than what the process would allocate. These
// pin the fix at all three sites, in the same two directions
// pkg/flowstate/v1/flowtest's read_test.go pins for the identical shape: a stream
// that is bounded regardless of a file's reported size, and a path that names
// something other than a regular file.

// maxFlowfileBytes mirrors the unexported maxBytes these three reads are bounded
// by. Restated here, rather than exported from the package for one test file to
// read, because a test asserting a literal number pins the number itself: if the
// package's own bound ever moves, this and [flowfile]'s internal tests move
// together rather than one silently trusting a limit that changed underneath it.
const maxFlowfileBytes = 1 << 20

// TestParseFileRefusesANonRegularFileWithoutReadingIt is the half a size check
// could never carry: a symlink to /dev/zero stats as zero bytes and reads without
// end, so before the fix this would hang rather than refuse.
func TestParseFileRefusesANonRegularFileWithoutReadingIt(t *testing.T) {
	t.Parallel()

	if _, err := os.Stat("/dev/zero"); err != nil {
		t.Skip("no /dev/zero on this platform to point a fixture at")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "endless.yaml")
	require.NoError(t, os.Symlink("/dev/zero", path))

	_, _, err := flowfile.ParseFile(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a regular file")
}

// TestParseFileRefusesADirectoryWithAnActionableMessage pins the directory arm
// of the non-regular-file check: a directory is the common case (a trailing
// slash on a path), and the error names what to do rather than why the engine
// cannot proceed.
func TestParseFileRefusesADirectoryWithAnActionableMessage(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	_, _, err := flowfile.ParseFile(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is a directory")
	assert.Contains(t, err.Error(), "name the Flowfile inside it")
}

// TestParseFileRefusesAnOversizedFile is the size half: the file is written one
// byte past the limit, which only a stream capped at limit+1 can notice at
// exactly that point, rather than a size read afterward from a fully materialized
// buffer.
func TestParseFileRefusesAnOversizedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "huge.yaml")
	require.NoError(t, os.WriteFile(path, []byte("# "+strings.Repeat("a", maxFlowfileBytes)), 0o644))

	_, _, err := flowfile.ParseFile(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "larger than the")
}

// TestParseFileStillReadsAnOrdinaryFile is the other direction: the bound refuses
// what is too large or not a regular file, and nothing else.
func TestParseFileStillReadsAnOrdinaryFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "ok.yaml")
	require.NoError(t, os.WriteFile(path, []byte(simpleCalleeSource), 0o644))

	workflow, positions, err := flowfile.ParseFile(path)
	require.NoError(t, err)
	require.NotNil(t, positions)
	assert.Equal(t, "callee", workflow.GetName())
}

// TestValidateSourceFileRefusesANonRegularFileWithoutReadingIt is
// [TestParseFileRefusesANonRegularFileWithoutReadingIt] for the sibling entry
// point `flow validate` and the LSP call by path.
func TestValidateSourceFileRefusesANonRegularFileWithoutReadingIt(t *testing.T) {
	t.Parallel()

	if _, err := os.Stat("/dev/zero"); err != nil {
		t.Skip("no /dev/zero on this platform to point a fixture at")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "endless.yaml")
	require.NoError(t, os.Symlink("/dev/zero", path))

	_, err := flowfile.ValidateSourceFile(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a regular file")
}

// TestValidateSourceFileRefusesAnOversizedFile is the size half of the same bound.
func TestValidateSourceFileRefusesAnOversizedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "huge.yaml")
	require.NoError(t, os.WriteFile(path, []byte("# "+strings.Repeat("a", maxFlowfileBytes)), 0o644))

	_, err := flowfile.ValidateSourceFile(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "larger than the")
}

// TestValidateSourceFileStillValidatesAnOrdinaryFile is the other direction.
func TestValidateSourceFileStillValidatesAnOrdinaryFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "ok.yaml")
	require.NoError(t, os.WriteFile(path, []byte(simpleCalleeSource), 0o644))

	diagnostics, err := flowfile.ValidateSourceFile(path)
	require.NoError(t, err)
	assert.Empty(t, diagnostics)
}

// TestCallRefusesACalleeThatIsNotARegularFileWithoutReadingIt is the third of the
// three sites #504 named: a `call:` step resolves its callee out of the workflow
// document being parsed, so the file being read is named by the file being
// compiled, and the path an author never typed is exactly the one that reaches
// this bound.
//
// A directory rather than a symlink to /dev/zero, because a target outside the
// calling file's own directory — which is what /dev/zero is — is refused earlier,
// by [flowfile.ResolveCallTarget]'s own containment check, before the read this
// test means to reach. A directory sitting inside the caller's own directory
// passes that check and is what makes it to [readBoundedSource].
func TestCallRefusesACalleeThatIsNotARegularFileWithoutReadingIt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "callee.yaml"), 0o755))

	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: a
    call: ./callee.yaml
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is a directory")
}

// TestCallRefusesAnOversizedCallee is the size half for the callee read.
func TestCallRefusesAnOversizedCallee(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"),
		[]byte("# "+strings.Repeat("a", maxFlowfileBytes)), 0o644))

	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: a
    call: ./callee.yaml
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "larger than the")
}
