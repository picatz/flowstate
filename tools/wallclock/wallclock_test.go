package main

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wallClockSleeps is the ratchet: every `_test.go` file holding a sleep that
// spends real time, with how many, relative to the repository root.
//
// Adding a sleep fails the test below until it moves into a synctest bubble or
// is recorded here with the process it waits for; removing one fails it until
// the entry shrinks, so the table cannot keep an entry the tree no longer
// has. What each of the current entries waits for:
//
//   - cmd/flow/serverdev_test.go — the dev stack's addresses going quiet
//     *after* `flow server dev` returned, retried because a listener closing
//     and the kernel releasing the port are not the same instant. A teardown
//     assertion, not a startup wait.
//   - cmd/flow, the other three — windows, not waits, so all three are reason 3
//     below rather than reason 1. browser_test.go retries removing a profile
//     directory while the zygote, renderers and crash handler it did not launch
//     directly finish writing into it; workershutdown_test.go holds a window
//     open in which the worker must still be alive; and
//     workerinternallistener_test.go gives a binding that would happen
//     immediately room to appear before a *negative* assertion concludes it did
//     not — its subprocess is already polling by then, so nothing is awaited.
//   - internal/temporaltest — the supervised dev server process.
//   - engine/deadlock_budget_test.go — a sleep inside a workflow goroutine
//     that must *not* yield, since failing to yield is the thing the SDK's
//     deadlock detector is watching for; workflow_slice_test.go is the child
//     process that deliberately outlives its replay deadline.
//   - netpolicy — a hold in the *test body*, not in the handler, which parks on
//     a channel instead: it keeps the response body back so that the exported
//     span is measurably longer than the moment its headers arrived, which is
//     the bound the assertion reads. Unbubblable all the same, because the span
//     it measures is timed across a real loopback socket and neither end of one
//     is ever durably blocked.
//   - secrets/vault — a sleep inside the fake vault's login handler, reached
//     over a real loopback socket.
//   - plugin, waits — ten of helper_test.go's twelve sleep inside the fake
//     *plugin* subprocess, for reasons of their own: seven hold a handshake
//     open long enough to fail it, one counts a short-lived plugin down to its
//     own exit, one paces stdout writes and one holds a late stderr line back
//     until after the call returns, where the scrubber must still catch it. The other two are host-side polls (waitForProcessGone,
//     waitFor). host_test.go:102 and :153 both let a call reach the plugin
//     before it is interrupted, but not the same way: :102 then closes the
//     host, and :153 cancels the call the plugin must *survive* — a
//     distinction that file's own doc calls expensive to get wrong, so it is
//     spelled out here. launch_test.go:295 paces three health checks while the
//     stdout-noise plugin keeps writing, and :331 gives the stderr flood time
//     to run its course.
//   - plugin, one window — host_test.go:670 waits on nothing: it lets several
//     polls go by and then asserts *negatively* that the plugin was left alone
//     (pid unchanged, zero restarts). Reason 3, the same shape as
//     workerinternallistener_test.go, and filed here so the bullet does not
//     say "waits" of a sleep that does not.
//   - plugin/sdk/serve_test.go — a poll for the handshake line, which is not a
//     subprocess at all: that plugin runs in this process, and what the poll
//     waits on is a real pipe and the Unix socket `listen` creates. Real I/O
//     either way, so a bubble would never see it become ready.
//   - secrets/command_test.go — the hang a helper subprocess performs on
//     request, in that process rather than this one.
//   - server — the run's completion on a real dev server.
//
// What keeps these out of a bubble is not one rule but three, and telling them
// apart is the work when the ratchet fires on something new:
//
//  1. Something outside the bubble has to make progress first — a subprocess,
//     a dev server, a socket. A goroutine waiting on one of those is never
//     durably blocked, so the bubble's clock would never advance and the wait
//     would hang rather than return early.
//  2. The assertion measures a real-clock interval across such a boundary.
//     Netpolicy is the one: what the hold buys is a span, timed across a
//     loopback socket, that is measurably longer than the moment its headers
//     arrived.
//  3. The elapsed real time *is* the mechanism, and a bubble would defeat it
//     by spending it for free. deadlock_budget_test.go is the sharp case: it
//     sleeps inside a workflow goroutine precisely so that it does *not* yield,
//     and in a bubble that sleep becomes a yield, the SDK's detector never
//     fires, and the test passes having proved nothing. The retry backoffs and
//     observation windows in cmd/flow are the quiet case — a bubble would burn
//     all forty of browser_test.go's attempts, and workershutdown_test.go's
//     whole window, against a world that had not moved.
//
// So neither "the test is timing something" nor "there is a process somewhere
// nearby" settles it. The question to ask of a new entry is what the bubble
// would break. Three that answered "nothing" — a provider delay manufacturing a
// stampede, a wait's deadline and a parked reader — became bubbles, because
// each was paying real time for
// a *weaker* claim than a bubble makes: a stampede hoped for rather than
// guaranteed, a gate assumed reached after 100ms, a goroutine census carrying
// slack. Each is now an assertion the bubble makes exactly, and a new entry
// here is more likely to be a fourth of those than a fourth reason.
var wallClockSleeps = map[string]int{
	"cmd/flow/browser_test.go":                        1,
	"cmd/flow/serverdev_test.go":                      1,
	"cmd/flow/workerinternallistener_test.go":         1,
	"cmd/flow/workershutdown_test.go":                 1,
	"internal/temporaltest/supervisor_linux_test.go":  1,
	"pkg/flowstate/v1/engine/deadlock_budget_test.go": 1,
	"pkg/flowstate/v1/engine/workflow_slice_test.go":  1,
	"pkg/flowstate/v1/netpolicy/tracing_test.go":      1,
	"pkg/flowstate/v1/plugin/helper_test.go":          12,
	"pkg/flowstate/v1/plugin/host_test.go":            3,
	"pkg/flowstate/v1/plugin/launch_test.go":          2,
	"pkg/flowstate/v1/plugin/sdk/serve_test.go":       1,
	"pkg/flowstate/v1/secrets/command_test.go":        1,
	"pkg/flowstate/v1/secrets/vault/fake_test.go":     1,
	"pkg/flowstate/v1/server/server_test.go":          1,
}

// TestTheRepositoryWallClockSleepsOnlyGoDown holds the count in both
// directions, so the table is the tree's state rather than an allowlist.
func TestTheRepositoryWallClockSleepsOnlyGoDown(t *testing.T) {
	t.Parallel()

	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	sleeps, files, err := Analyze(root)
	require.NoError(t, err)
	require.Greater(t, files, 500,
		"the walk read %d test files, which is too few to have reached the tree", files)

	got := map[string]int{}
	for _, file := range CountByFile(sleeps) {
		got[relative(root, file.File)] = file.Count
	}

	added, removed := ratchet(got, wallClockSleeps)

	assert.Emptyf(t, added,
		"a test gained a sleep that spends real time. Move it into synctest.Test, where it "+
			"returns the instant the bubble is idle, or record the process it waits for in "+
			"wallClockSleeps beside the others:\n\n%s", sites(sleeps, root, added))
	assert.Emptyf(t, removed,
		"a test lost a sleep and wallClockSleeps still counts it; shrink the entry so the "+
			"table keeps saying where the tree stands: %s", strings.Join(removed, ", "))
}

// ratchet compares the tree's counts against the table and names the files
// that gained a sleep and the files whose entry counts one the tree lost.
func ratchet(got, want map[string]int) (added, removed []string) {
	for file, count := range got {
		if count > want[file] {
			added = append(added, file)
		}
	}
	for file, count := range want {
		if got[file] < count {
			removed = append(removed, file)
		}
	}
	slices.Sort(added)
	slices.Sort(removed)

	return added, removed
}

func TestTheRatchetHoldsInBothDirections(t *testing.T) {
	t.Parallel()

	want := map[string]int{"a_test.go": 2, "b_test.go": 1}

	added, removed := ratchet(map[string]int{"a_test.go": 2, "b_test.go": 1}, want)
	assert.Empty(t, added)
	assert.Empty(t, removed)

	added, removed = ratchet(map[string]int{"a_test.go": 3, "b_test.go": 1, "c_test.go": 1}, want)
	assert.Equal(t, []string{"a_test.go", "c_test.go"}, added, "a new sleep in a known file and a new file were not both reported")
	assert.Empty(t, removed)

	added, removed = ratchet(map[string]int{"a_test.go": 1}, want)
	assert.Empty(t, added)
	assert.Equal(t, []string{"a_test.go", "b_test.go"}, removed, "an entry the tree no longer supports was kept")
}

// sites lists the sleeps in the named files, for the message.
func sites(sleeps []Sleep, root string, files []string) string {
	var out []string
	for _, sleep := range sleeps {
		file := relative(root, sleep.File)
		for _, want := range files {
			if file == want {
				out = append(out, file+":"+strconv.Itoa(sleep.Line))
			}
		}
	}
	return strings.Join(out, "\n")
}

// analyzeSource writes the sources as test files in a fresh directory and
// analyzes it.
func analyzeSource(t *testing.T, sources map[string]string) []Sleep {
	t.Helper()

	dir := t.TempDir()
	for name, src := range sources {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(src), 0o600))
	}

	sleeps, files, err := Analyze(dir)
	require.NoError(t, err)
	require.Len(t, sources, files)

	return sleeps
}

func lines(sleeps []Sleep) []int {
	out := make([]int, 0, len(sleeps))
	for _, sleep := range sleeps {
		out = append(out, sleep.Line)
	}
	return out
}

func TestASleepInABubbleIsNotCounted(t *testing.T) {
	t.Parallel()

	sleeps := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestBubbled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(time.Second)
	})
}

func TestReal(t *testing.T) {
	time.Sleep(time.Second)
}
`})

	assert.Equal(t, []int{16}, lines(sleeps), "the sleep inside synctest.Test was counted, or the one outside was not")
}

func TestASleepInAHelperTheBubbleCallsIsCounted(t *testing.T) {
	t.Parallel()

	sleeps := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestBubbled(t *testing.T) {
	synctest.Test(t, helper)
}

func helper(t *testing.T) {
	time.Sleep(time.Second)
}
`})

	assert.Equal(t, []int{14}, lines(sleeps), "a sleep the analysis cannot see into a bubble is counted, so the table has to say why it is there")
}

func TestAnAliasedTimeImportIsFollowed(t *testing.T) {
	t.Parallel()

	sleeps := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	clock "time"
)

func TestReal(t *testing.T) {
	clock.Sleep(clock.Second)
}

func TestNotTime(t *testing.T) {
	var time struct{ Sleep func(int) }
	time.Sleep(1)
}
`})

	assert.Equal(t, []int{9}, lines(sleeps), "an aliased time import was missed, or a local named time was mistaken for it")
}

func TestADotImportedTimeAndSynctestAreFollowed(t *testing.T) {
	t.Parallel()

	sleeps := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	. "testing/synctest"
	. "time"
)

func TestBubbled(t *testing.T) {
	Test(t, func(t *testing.T) {
		Sleep(Second)
	})
}

func TestReal(t *testing.T) {
	Sleep(Second)
}
`})

	assert.Equal(t, []int{16}, lines(sleeps), "a dot-imported Sleep was missed, or a dot-imported Test was not seen as a bubble")
}

func TestABlankImportOfTimeCountsNothing(t *testing.T) {
	t.Parallel()

	sleeps := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	_ "time"
)

func TestNotTime(t *testing.T) {
	var time struct{ Sleep func(int) }
	time.Sleep(1)
}
`})

	assert.Empty(t, sleeps, "a file that blank-imports time has no time.Sleep to count")
}

func TestOnlyTestFilesAreRead(t *testing.T) {
	t.Parallel()

	sleeps := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	"time"
)

func TestReal(t *testing.T) {
	time.Sleep(time.Second)
	time.Sleep(time.Second)
}
`})

	dir := filepath.Dir(sleeps[0].File)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.go"), []byte("package a\n\nimport \"time\"\n\nfunc f() { time.Sleep(1) }\n"), 0o600))

	again, files, err := Analyze(dir)
	require.NoError(t, err)
	assert.Equal(t, 1, files, "a non-test file was read as a test file")
	assert.Equal(t, []int{9, 10}, lines(again), "a sleep in a non-test file was counted")
}
