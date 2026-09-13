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
//   - cmd/flow — serverdev and workerinternallistener wait on a server or
//     worker subprocess. The other two spend real time without waiting on
//     anything: browser_test.go retries removing a profile directory while the
//     zygote, renderers and crash handler it did not launch directly finish
//     writing into it, and workershutdown_test.go holds a window open in which
//     the worker must still be alive.
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
//   - plugin — the fake plugin subprocesses in helper_test.go sleep in the
//     *plugin* to stay alive for the host, and host_test.go and
//     launch_test.go wait on those processes.
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
// would break. Three that answered "nothing" — a cache TTL, a wait's deadline
// and a parked reader — became bubbles, because each was paying real time for
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
	// count fall while the wall clock did not. The formatted spellings are the
	// same calls with a message, and were the hole #1989's reviewer found.
	//
	// These six are the whole family in the pinned testify. There is no
	// NeverWithT, so the last two lines below are both "an ordinary assertion is
	// not counted" and "a plausible-sounding name that does not exist is not
	// quietly matched".
	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReal(t *testing.T) {
	require.Eventually(t, nil, 0, 0)
	require.Eventuallyf(t, nil, 0, 0, "")
	require.EventuallyWithT(t, nil, 0, 0)
	require.EventuallyWithTf(t, nil, 0, 0, "")
	require.Never(t, nil, 0, 0)
	require.Neverf(t, nil, 0, 0, "")
	assert.Eventually(t, nil, 0, 0)
	require.NeverWithT(t, nil, 0, 0)
	require.True(t, true)
}
`})

	assert.Equal(t, []int{11, 12, 13, 14, 15, 16, 17}, pollLines(waits),
		"a member of the Eventually family was missed, or an ordinary assertion was counted")
}

// TestAShadowedSynctestNameSuppressesNoBubble is the negative direction of the
// shadowing tradeoff, and the reason it is handled rather than merely recorded.
//
// A local that shadows `require` makes this analysis count a call that is not a
// poll — over-counting, which never hides a wait. A local that shadows
// `synctest` would do the opposite: its `Test` call would be read as a bubble
// and the real waits inside it would stop being counted, so the ratchet would
// go green because a test got worse. #1989's reviewer found that asymmetry, and
// the file-scoped answer is that a file declaring the import's own name gets no
// bubbles at all.
func TestAShadowedSynctestNameSuppressesNoBubble(t *testing.T) {
	t.Parallel()

	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type fake struct{}

func (fake) Test(f func(*testing.T)) {}

func TestReal(t *testing.T) {
	synctest := fake{}
	synctest.Test(func(t *testing.T) {
		time.Sleep(time.Second)
		require.Eventually(t, nil, 0, 0)
	})
}
`})

	assert.Equal(t, []int{18}, lines(OfKind(waits, KindSleep)),
		"a sleep inside a shadowed Test call was treated as bubbled")
	assert.Equal(t, []int{19}, pollLines(waits),
		"a poll inside a shadowed Test call was treated as bubbled")
}

// TestAnAliasedAndADotImportedTestifyAreFollowed also pins the over-count the
// name-based matcher accepts: in a file that imports testify, a call named
// Eventually on something that is not a testify object is counted anyway.
//
// That is the deliberate side of the trade. Recognising the receiver instead
// took three rounds of review on #1989 and was still missing shapes — a chained
// New, a field, a map entry — and every miss is a poll the ratchet lets through.
// Counting one extra call costs a table entry with a note; missing one costs the
// mechanism.
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

	assert.Equal(t, []int{10, 15, 10}, pollLines(waits),
		"an aliased or dot-imported testify poll was missed, or the deliberate over-count "+
			"on a non-testify Eventually in a testify-importing file has changed")
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

// TestAPollThroughATestifyObjectIsCounted is the regression for the hole an
// automated reviewer found on #1989: testify's object API binds the assertions
// to a value, so the receiver at the call is that value and not the package the
// file imported. A poll written that way used to be invisible here, which is
// the one defect a ratchet cannot survive — the count stays green while the
// tree gets worse, so it reports "no polls were added" about a poll that was.
//
// No test in this repository uses the object API today. That is exactly why the
// case is written down: the hole was not costing anything yet, and a ratchet is
// for the change nobody has made yet.
func TestAPollThroughATestifyObjectIsCounted(t *testing.T) {
	t.Parallel()

	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBound(t *testing.T) {
	r := require.New(t)
	r.Eventually(nil, 0, 0)

	var a = assert.New(t)
	a.Never(nil, 0, 0)
}

func TestReassigned(t *testing.T) {
	var r *require.Assertions
	r = require.New(t)
	r.EventuallyWithT(nil, 0, 0)
}

func TestChained(t *testing.T) {
	require.New(t).Eventually(nil, 0, 0)
}

func TestHeldInAField(t *testing.T) {
	h := struct{ r *require.Assertions }{r: require.New(t)}
	h.r.Neverf(nil, 0, 0, "")
}
`})

	assert.Equal(t, []int{12, 15, 21, 25, 30}, pollLines(waits),
		"a poll reached through a testify assertion object was missed: bound to a local, "+
			"reassigned, chained onto New, or held in a field")
}

// TestAShadowedDotImportedBubbleCallSuppressesNoBubble is the dot-import half
// of [TestAShadowedSynctestNameSuppressesNoBubble], and a regression for a
// mistake the first fix made.
//
// That fix exempted dot-imports from the shadow check, reasoning that a
// dot-import has no name for a local to shadow. True of the import and beside
// the point: what a local can displace is the name at the *call*, and for a
// dot-import that is the bare `Test` or `Run`. A local of either name therefore
// had its callback read as a bubble, which is the under-counting direction the
// first fix existed to close.
func TestAShadowedDotImportedBubbleCallSuppressesNoBubble(t *testing.T) {
	t.Parallel()

	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	. "testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReal(t *testing.T) {
	Test := func(_ *testing.T, f func(*testing.T)) { f(t) }
	Test(t, func(t *testing.T) {
		time.Sleep(time.Second)
		require.Eventually(t, nil, 0, 0)
	})
}
`})

	assert.Equal(t, []int{14}, lines(OfKind(waits, KindSleep)),
		"a sleep inside a shadowed dot-imported Test call was treated as bubbled")
	assert.Equal(t, []int{15}, pollLines(waits),
		"a poll inside a shadowed dot-imported Test call was treated as bubbled")
}

// TestARangeDeclaredSynctestShadowSuppressesNoBubble is the range-clause half of
// the shadow check, reported on #1989 after the first two halves were closed.
//
// A range clause is its own node rather than an assignment inside one, so the
// declaration scan walked straight past `for synctest := range …` and the loop
// body's `synctest.Test(…)` was read as a bubble. Every wait inside then stopped
// being counted, which is the direction this check exists to prevent.
func TestARangeDeclaredSynctestShadowSuppressesNoBubble(t *testing.T) {
	t.Parallel()

	waits := analyzeSource(t, map[string]string{"a_test.go": `package a

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type fake struct{}

func (fake) Test(f func(*testing.T)) {}

func TestReal(t *testing.T) {
	for synctest := range map[fake]struct{}{{}: {}} {
		synctest.Test(func(t *testing.T) {
			time.Sleep(time.Second)
			require.Eventually(t, nil, 0, 0)
		})
	}
}
`})

	assert.Equal(t, []int{18}, lines(OfKind(waits, KindSleep)),
		"a sleep inside a range-declared shadow was treated as bubbled")
	assert.Equal(t, []int{19}, pollLines(waits),
		"a poll inside a range-declared shadow was treated as bubbled")
}
