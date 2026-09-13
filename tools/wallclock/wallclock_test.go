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
//   - cmd/flow — a worker or server subprocess, and a browser the test
//     never opens.
//   - internal/temporaltest — the supervised dev server process.
//   - engine/deadlock_budget_test.go — a workflow task deliberately running
//     past the deadlock budget on a real Temporal worker; workflow_slice_test.go
//     is the child process that deliberately outlives its replay deadline.
//   - flowdebug, netpolicy, secrets, wait_local — a wall-clock bound the test
//     is measuring (a span's duration, a command's runtime, a wait's
//     deadline); candidates for a bubble once the code under test takes its
//     clock from the bubble. secrets/cache_test.go is what that looks like
//     once done: its subtests are bubbled and its remaining entry is the fake
//     provider's own sleep, which the analysis cannot see is called from
//     inside one.
//   - plugin — the fake plugin subprocesses in helper_test.go sleep in the
//     *plugin* to stay alive for the host, and host_test.go and
//     launch_test.go wait on those processes.
//   - server — the run's completion on a real dev server.
var wallClockSleeps = map[string]int{
	"cmd/flow/browser_test.go":                        1,
	"cmd/flow/serverdev_test.go":                      1,
	"cmd/flow/workerinternallistener_test.go":         1,
	"cmd/flow/workershutdown_test.go":                 1,
	"internal/temporaltest/supervisor_linux_test.go":  1,
	"pkg/flowstate/v1/engine/deadlock_budget_test.go": 1,
	"pkg/flowstate/v1/engine/workflow_slice_test.go":  1,
	"pkg/flowstate/v1/flowdebug/session_test.go":      1,
	"pkg/flowstate/v1/netpolicy/tracing_test.go":      1,
	"pkg/flowstate/v1/plugin/helper_test.go":          12,
	"pkg/flowstate/v1/plugin/host_test.go":            3,
	"pkg/flowstate/v1/plugin/launch_test.go":          2,
	"pkg/flowstate/v1/plugin/sdk/serve_test.go":       1,
	"pkg/flowstate/v1/secrets/cache_test.go":          1,
	"pkg/flowstate/v1/secrets/command_test.go":        1,
	"pkg/flowstate/v1/secrets/vault/fake_test.go":     1,
	"pkg/flowstate/v1/server/server_test.go":          1,
	"pkg/flowstate/v1/wait_local_test.go":             1,
}

// TestTheRepositoryWallClockSleepsOnlyGoDown holds the count in both
// directions, so the table is the tree's state rather than an allowlist.
func TestTheRepositoryWallClockSleepsOnlyGoDown(t *testing.T) {
	t.Parallel()

	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	waits, files, err := Analyze(root)
	require.NoError(t, err)
	require.Greater(t, files, 500,
		"the walk read %d test files, which is too few to have reached the tree", files)

	sleeps := OfKind(waits, KindSleep)

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

// repositoryPolls is the second ratchet: every `_test.go` file holding a
// testify poll — Eventually, Never, and their WithT forms — outside a synctest
// bubble, with how many.
//
// A poll is the cheaper half of #1706 and the larger one. It usually costs no
// wall clock, because the first tick answers, so this table is not about
// seconds; it is about the two things a poll cannot do. Its ceiling is a guess
// about how slow a runner may be, which is the flake #1706 opens with, and its
// failure says "condition never satisfied" rather than which half of the
// condition was wrong.
//
// Where the goroutines that satisfy the condition are all inside the test,
// [synctest.Wait] replaces the poll outright: it returns when they are durably
// blocked, and the assertion that follows is an ordinary one.
// pkg/flowstate/v1/flowfile/lsp/testfile_test.go is the worked example, and is
// absent from this table for that reason.
//
// Where they are not — a real dev server, a plugin subprocess, a worker in
// another process — the poll stays, because a goroutine blocked on a socket is
// never durably blocked and the bubble would wait forever. Most of what
// remains below is that: `server` and `engine` talk to Temporal, `cmd/flow`
// and `plugin` to child processes. Sorting the rest into the two piles is the
// work #1706 still asks for, and this table is how the count is kept honest
// while it happens.
var repositoryPolls = map[string]int{
	"cmd/flow/main_test.go":                                              1,
	"cmd/flow/workerinternallistener_test.go":                            1,
	"cmd/flow/workershutdown_test.go":                                    2,
	"internal/temporaltest/supervisor_linux_test.go":                     3,
	"pkg/flowstate/v1/clock_test.go":                                     3,
	"pkg/flowstate/v1/debugger_test.go":                                  1,
	"pkg/flowstate/v1/engine/main_test.go":                               1,
	"pkg/flowstate/v1/engine/panicpolicy_test.go":                        1,
	"pkg/flowstate/v1/engine/replay_record_test.go":                      1,
	"pkg/flowstate/v1/engine/summary_test.go":                            1,
	"pkg/flowstate/v1/engine/versioning_test.go":                         5,
	"pkg/flowstate/v1/eval_test.go":                                      2,
	"pkg/flowstate/v1/flowdebug/completedisclosure_test.go":              1,
	"pkg/flowstate/v1/flowdebug/control_internal_test.go":                3,
	"pkg/flowstate/v1/flowdebug/control_test.go":                         4,
	"pkg/flowstate/v1/flowtest/flowtesting/flowtesting_internal_test.go": 1,
	"pkg/flowstate/v1/flowtest/regressions_test.go":                      1,
	"pkg/flowstate/v1/plugin/sdk/serve_test.go":                          1,
	"pkg/flowstate/v1/plugin/sdk/taskservice_test.go":                    2,
	"pkg/flowstate/v1/secrets/vault/auth_test.go":                        1,
	"pkg/flowstate/v1/server/chain_test.go":                              2,
	"pkg/flowstate/v1/server/concurrency_test.go":                        1,
	"pkg/flowstate/v1/server/durability_test.go":                         1,
	"pkg/flowstate/v1/server/eagerstart_test.go":                         1,
	"pkg/flowstate/v1/server/entity_test.go":                             4,
	"pkg/flowstate/v1/server/failure_test.go":                            5,
	"pkg/flowstate/v1/server/fairness_test.go":                           1,
	"pkg/flowstate/v1/server/inputs_test.go":                             2,
	"pkg/flowstate/v1/server/lifecycle_tenancy_test.go":                  6,
	"pkg/flowstate/v1/server/list_paging_test.go":                        1,
	"pkg/flowstate/v1/server/main_test.go":                               2,
	"pkg/flowstate/v1/server/manualtrigger_test.go":                      1,
	"pkg/flowstate/v1/server/progress_test.go":                           3,
	"pkg/flowstate/v1/server/requestid_test.go":                          1,
	"pkg/flowstate/v1/server/schedules_search_test.go":                   1,
	"pkg/flowstate/v1/server/schedules_test.go":                          7,
	"pkg/flowstate/v1/server/searchattributes_test.go":                   1,
	"pkg/flowstate/v1/server/signalauth_perrun_test.go":                  4,
	"pkg/flowstate/v1/server/signalauth_test.go":                         6,
	"pkg/flowstate/v1/server/staleruns_test.go":                          1,
	"pkg/flowstate/v1/server/starter_test.go":                            1,
	"pkg/flowstate/v1/server/tenancy_test.go":                            4,
	"pkg/flowstate/v1/server/timeline_test.go":                           1,
	"pkg/flowstate/v1/server/waits_test.go":                              3,
	"pkg/flowstate/v1/server/webhook_test.go":                            1,
	"pkg/flowstate/v1/temporalclient/main_test.go":                       1,
	"pkg/flowstate/v1/wait_local_test.go":                                1,
	"pkg/flowstate/v1/waits_local_test.go":                               4,
}

// TestTheRepositoryPollsOnlyGoDown is [TestTheRepositoryWallClockSleepsOnlyGoDown]
// for the Eventually family, and holds the count in both directions for the
// same reason: a table that only ever grew would be an allowlist.
func TestTheRepositoryPollsOnlyGoDown(t *testing.T) {
	t.Parallel()

	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	waits, files, err := Analyze(root)
	require.NoError(t, err)
	require.Greater(t, files, 500,
		"the walk read %d test files, which is too few to have reached the tree", files)

	polls := OfKind(waits, KindPoll)

	got := map[string]int{}
	for _, file := range CountByFile(polls) {
		got[relative(root, file.File)] = file.Count
	}

	added, removed := ratchet(got, repositoryPolls)

	assert.Emptyf(t, added,
		"a test gained a poll. Where the goroutines that satisfy the condition are inside "+
			"the test, move it into synctest.Test and replace the poll with synctest.Wait and "+
			"a plain assertion; where they are not, record it in repositoryPolls beside the "+
			"others:\n\n%s", sites(polls, root, added))
	assert.Emptyf(t, removed,
		"a test lost a poll and repositoryPolls still counts it; shrink the entry so the "+
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
func sites(waits []Wait, root string, files []string) string {
	var out []string
	for _, wait := range waits {
		file := relative(root, wait.File)
		for _, want := range files {
			if file == want {
				out = append(out, file+":"+strconv.Itoa(wait.Line))
			}
		}
	}
	return strings.Join(out, "\n")
}

// analyzeSource writes the sources as test files in a fresh directory and
// analyzes it.
func analyzeSource(t *testing.T, sources map[string]string) []Wait {
	t.Helper()

	dir := t.TempDir()
	for name, src := range sources {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(src), 0o600))
	}

	waits, files, err := Analyze(dir)
	require.NoError(t, err)
	require.Len(t, sources, files)

	return waits
}

func lines(waits []Wait) []int {
	out := make([]int, 0, len(waits))
	for _, wait := range waits {
		out = append(out, wait.Line)
	}
	return out
}

func TestASleepInABubbleIsNotCounted(t *testing.T) {
	t.Parallel()

	sleeps := OfKind(analyzeSource(t, map[string]string{"a_test.go": `package a

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
`}), KindSleep)

	assert.Equal(t, []int{16}, lines(sleeps), "the sleep inside synctest.Test was counted, or the one outside was not")
}

func TestASleepInAHelperTheBubbleCallsIsCounted(t *testing.T) {
	t.Parallel()

	sleeps := OfKind(analyzeSource(t, map[string]string{"a_test.go": `package a

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
`}), KindSleep)

	assert.Equal(t, []int{14}, lines(sleeps), "a sleep the analysis cannot see into a bubble is counted, so the table has to say why it is there")
}

func TestAnAliasedTimeImportIsFollowed(t *testing.T) {
	t.Parallel()

	sleeps := OfKind(analyzeSource(t, map[string]string{"a_test.go": `package a

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
`}), KindSleep)

	assert.Equal(t, []int{9}, lines(sleeps), "an aliased time import was missed, or a local named time was mistaken for it")
}

func TestADotImportedTimeAndSynctestAreFollowed(t *testing.T) {
	t.Parallel()

	sleeps := OfKind(analyzeSource(t, map[string]string{"a_test.go": `package a

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
`}), KindSleep)

	assert.Equal(t, []int{16}, lines(sleeps), "a dot-imported Sleep was missed, or a dot-imported Test was not seen as a bubble")
}

func TestABlankImportOfTimeCountsNothing(t *testing.T) {
	t.Parallel()

	sleeps := OfKind(analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	_ "time"
)

func TestNotTime(t *testing.T) {
	var time struct{ Sleep func(int) }
	time.Sleep(1)
}
`}), KindSleep)

	assert.Empty(t, sleeps, "a file that blank-imports time has no time.Sleep to count")
}

func TestOnlyTestFilesAreRead(t *testing.T) {
	t.Parallel()

	sleeps := OfKind(analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	"time"
)

func TestReal(t *testing.T) {
	time.Sleep(time.Second)
	time.Sleep(time.Second)
}
`}), KindSleep)

	dir := filepath.Dir(sleeps[0].File)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.go"), []byte("package a\n\nimport \"time\"\n\nfunc f() { time.Sleep(1) }\n"), 0o600))

	again, files, err := Analyze(dir)
	require.NoError(t, err)
	assert.Equal(t, 1, files, "a non-test file was read as a test file")
	assert.Equal(t, []int{9, 10}, lines(again), "a sleep in a non-test file was counted")
}

// pollLines is [lines] for the poll half of a result.
func pollLines(waits []Wait) []int { return lines(OfKind(waits, KindPoll)) }

func TestAPollInABubbleIsNotCounted(t *testing.T) {
	t.Parallel()

	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBubbled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		require.Eventually(t, func() bool { return true }, time.Second, time.Millisecond)
	})
}

func TestReal(t *testing.T) {
	require.Eventually(t, func() bool { return true }, time.Second, time.Millisecond)
}
`})

	assert.Equal(t, []int{18}, pollLines(waits),
		"the poll inside synctest.Test was counted, or the one outside was not")
}

func TestAPollIsFoundInAFileThatNeverImportsTime(t *testing.T) {
	t.Parallel()

	// The guard that skips a file with no `time` import must not skip one whose
	// only wait is a poll: a testify call takes its durations from constants a
	// helper supplies as often as from time's own.
	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReal(t *testing.T) {
	require.Eventually(t, func() bool { return true }, timeout, tick)
}
`})

	assert.Equal(t, []int{10}, pollLines(waits), "a poll in a file with no time import was missed")
}

func TestTheWholeEventuallyFamilyCounts(t *testing.T) {
	t.Parallel()

	// Never is the expensive one — it spends its whole timeout every run rather
	// than returning on the first true tick — so leaving it out would let the
	// count fall while the wall clock did not.
	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReal(t *testing.T) {
	require.Eventually(t, nil, 0, 0)
	require.EventuallyWithT(t, nil, 0, 0)
	require.Never(t, nil, 0, 0)
	require.NeverWithT(t, nil, 0, 0)
	assert.Eventually(t, nil, 0, 0)
	require.True(t, true)
}
`})

	assert.Equal(t, []int{11, 12, 13, 14, 15}, pollLines(waits),
		"a member of the Eventually family was missed, or an ordinary assertion was counted")
}

func TestAnAliasedAndADotImportedTestifyAreFollowed(t *testing.T) {
	t.Parallel()

	waits := analyzeSource(t, map[string]string{
		"a_test.go": `package a

import (
	"testing"

	req "github.com/stretchr/testify/require"
)

func TestReal(t *testing.T) {
	req.Eventually(t, nil, 0, 0)
}

func TestNotTestify(t *testing.T) {
	var require struct{ Eventually func() }
	require.Eventually()
}
`,
		"b_test.go": `package a

import (
	"testing"

	. "github.com/stretchr/testify/assert"
)

func TestDotted(t *testing.T) {
	Eventually(t, nil, 0, 0)
}
`,
	})

	assert.Equal(t, []int{10, 10}, pollLines(waits),
		"an aliased or dot-imported testify poll was missed, or a value named after the "+
			"package the file did not import under that name was mistaken for one")
}

// TestALocalShadowingTheImportedNameIsCounted records the limitation the
// analysis buys by parsing rather than building, so the next reader meets it
// here rather than in a puzzling table entry.
//
// [Analyze]'s doc states the tradeoff for bubbles; this is its other edge. A
// selector is matched by the name the file imports the package under, and
// nothing in a syntax tree says whether that name is in scope at the call.
// Resolving it would mean type-checking, which would mean building, which is
// what keeps the plugin modules' tests out of reach. The mistake is
// conservative in the direction that matters — it over-counts, so a wait is
// never missed — and a value named `require` or `time` holding a method of
// the same name is rare enough to record rather than to design around.
func TestALocalShadowingTheImportedNameIsCounted(t *testing.T) {
	t.Parallel()

	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShadowed(t *testing.T) {
	var require struct{ Eventually func() }
	require.Eventually()
}
`})

	assert.Equal(t, []int{11}, pollLines(waits),
		"the shadowing limitation changed; if it was fixed, delete this test and say so")
}
