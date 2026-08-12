package flowtest_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A fixture is untrusted input, and the bound on reading one has to be on the
// stream rather than on a prior sizing of the path.
//
// Both readers here had the size-then-read shape: `os.Stat` decided whether the
// file was small enough and `os.ReadFile` then took whatever the path held. The
// number that was checked and the bytes that were consumed came from two separate
// observations, which is not a bound at all. These pin both halves of the fix —
// the cap on the read, and the refusal of anything that is not a regular file.

// TestADeliveryThatIsNotARegularFileIsRefused is the half a size check could
// never carry: a symlink to /dev/zero stats as zero bytes, comfortably under the
// cap, and reads without end. Before the fix this hung `flow test` until whatever
// killed it first.
func TestADeliveryThatIsNotARegularFileIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := os.Stat("/dev/zero"); err != nil {
		t.Skip("no /dev/zero on this platform to point a fixture at")
	}

	dir := writeDeliveryFixture(t)
	require.NoError(t, os.Symlink("/dev/zero", dir+"/endless.json"))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a delivery that is not a file
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./endless.json
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	assert.Contains(t, report.GetCases()[0].GetError(), "is not a regular file")
}

// TestADeliveryDirectoryIsRefused reaches the same refusal without a device file,
// so the rule is pinned wherever this builds rather than only where /dev/zero
// exists.
func TestADeliveryDirectoryIsRefused(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	require.NoError(t, os.Mkdir(dir+"/notafile.json", 0o755))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a delivery that is a directory
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./notafile.json
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	assert.Contains(t, report.GetCases()[0].GetError(), "is not a regular file")
}

// TestATestFileThatIsNotARegularFileIsRefused: `Load` had the identical shape,
// found by the sweep the delivery fix prompted. A `*.test.yaml` path is whatever
// `flow test` walked to, so the same symlink reaches it.
func TestATestFileThatIsNotARegularFileIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := os.Stat("/dev/zero"); err != nil {
		t.Skip("no /dev/zero on this platform to point a fixture at")
	}

	dir := t.TempDir()
	require.NoError(t, os.Symlink("/dev/zero", dir+"/endless.test.yaml"))

	_, err := flowtest.Load(dir + "/endless.test.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a regular file")
}

// TestATestFileTooLargeToReadIsRefused is the size half of the same bound, on the
// reader the sweep found rather than the one the review named, and it is the case
// that proves the cap is applied to the bytes read: the file is written one byte
// past the limit, which only a stream bound can notice at exactly that point.
func TestATestFileTooLargeToReadIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/huge.test.yaml", "# "+strings.Repeat("a", flowtest.MaxTestFileBytes))

	_, err := flowtest.Load(dir + "/huge.test.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than the")
}
