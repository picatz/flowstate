package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The size bound Go's own build cache does not have.
//
// `go help cache` offers two levers and neither is a budget: a sweep of
// entries unused for five days, run at most once a day, and `go clean -cache`,
// which discards everything. A machine that fills twenty-three gigabytes
// between breakfast and lunch is served by neither — nothing in that cache is
// five days old, so the sweep is a no-op, and the nuke charges a cold rebuild
// to every lane. These cover the middle.

func TestPruneForAsksForTheFloorAndALane(t *testing.T) {
	t.Parallel()

	// Freeing to the floor exactly earns the answer "dispatch zero lanes",
	// which is the state this exists to end — so the target is the floor plus
	// room to actually work.
	want := PruneFor(Machine{DiskFree: 1 * gib, CacheSizeBytes: 40 * gib})
	assert.Equal(t, uint64(DiskFloorBytes+LaneDiskBytes-1*gib), want.Bytes)
	assert.True(t, want.Enough)

	after := Machine{DiskFree: uint64(DiskFloorBytes) + LaneDiskBytes, CacheSizeBytes: 40 * gib}
	assert.Zero(t, PruneFor(after).Bytes, "a machine that already fits a lane has nothing to give back")
}

// TestPruneForWillNotPruneAFilesystemThatIsNotShort is the finding that a
// prune can be worse than doing nothing (Codex, #1112).
//
// DiskFree is the *tightest* mount a lane writes to, not the cache's. A
// machine whose GOTMPDIR is a small tmpfs while GOCACHE sits on a roomy volume
// is short on one and fine on the other, and a target computed from the
// aggregate would discard gigabytes of hot cache, free nothing where the bound
// is, and leave the machine fitting exactly as many lanes as before — with a
// cold rebuild now owed on top.
func TestPruneForWillNotPruneAFilesystemThatIsNotShort(t *testing.T) {
	t.Parallel()

	want := PruneFor(Machine{
		DiskFree:       64 << 20, // a small /tmp is the bound
		CacheDiskFree:  300 * gib,
		CacheDiskKnown: true,
		CacheSizeBytes: 40 * gib,
	})

	assert.Positive(t, want.Short, "the machine really is short — that much is true")
	assert.Zero(t, want.Bytes, "but not on the filesystem the cache could give bytes back to")
	assert.False(t, want.Enough)
}

// TestPruneForTreatsAnUnreadableCacheFilesystemAsTheSameMount: unknown must not
// mean "refuse". Most machines have one volume, and the cost of pruning where
// it does not help is a rebuild, where the cost of refusing to prune where it
// would have helped is the machine staying blocked — which is the outage the
// whole lever exists to end.
func TestPruneForTreatsAnUnreadableCacheFilesystemAsTheSameMount(t *testing.T) {
	t.Parallel()

	want := PruneFor(Machine{DiskFree: 1 * gib, CacheSizeBytes: 40 * gib})

	assert.Positive(t, want.Bytes, "an unreadable cache filesystem must not refuse the prune")
	assert.True(t, want.Enough)
}

// TestPruneForSeparatesNothingWrongFromNothingToBeDone: `Bytes == 0` carried
// both meanings, and the caller read it as the happy one — so an empty cache
// under a full disk reported that the disk was fine (Codex, #1112).
func TestPruneForSeparatesNothingWrongFromNothingToBeDone(t *testing.T) {
	t.Parallel()

	fine := PruneFor(Machine{DiskFree: 100 * gib, CacheSizeBytes: 40 * gib})
	assert.Zero(t, fine.Short, "a healthy machine is short of nothing")
	assert.Zero(t, fine.Bytes)

	empty := PruneFor(Machine{DiskFree: 1 * gib, CacheSizeBytes: 0})
	assert.Positive(t, empty.Short, "a full disk is short whether or not the cache can help")
	assert.Zero(t, empty.Bytes, "and an empty cache is nothing to reclaim")
	assert.False(t, empty.Enough)
}

// TestPruneAdviceNeverCallsAShortDiskHealthy is the finding itself rather than
// the type behind it (Codex, #1112): the defect was what got *printed*, so
// this holds the sentences to account the way TestAdviceIsGivenWhereItCanBeActedOn
// does for a plan.
func TestPruneAdviceNeverCallsAShortDiskHealthy(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		machine Machine
		says    string
	}{
		{
			name:    "an empty cache under a full disk",
			machine: Machine{DiskFree: 1 * gib, CacheSizeBytes: 0},
			says:    "the cache is empty or unreadable",
		},
		{
			name: "a cache on a filesystem that is not the short one",
			machine: Machine{
				DiskFree: 64 << 20, CacheDiskFree: 300 * gib, CacheDiskKnown: true,
				CacheSizeBytes: 40 * gib,
			},
			says: "GOCACHE is on a filesystem with",
		},
		{
			name:    "a cache too small to clear the floor",
			machine: Machine{DiskFree: 0, CacheSizeBytes: 1 * gib},
			says:    "the disk will still be short",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want := PruneFor(tc.machine)
			joined := strings.Join(want.Advice, "\n")

			require.NotEmpty(t, want.Advice,
				"a machine that fits no lanes must never be answered with silence")
			assert.Contains(t, joined, tc.says)
			assert.Contains(t, joined, "$GOMODCACHE",
				"and it has to say where to look next, since this lever cannot fix it")
			assert.NotContains(t, joined, "already past",
				"the whole defect was reporting a short disk as a healthy one")
		})
	}

	// And the other direction: a machine with no problem is told nothing,
	// because advice on a healthy machine is noise that trains readers to skip
	// it — the rule the plan's own advice test states.
	assert.Empty(t, PruneFor(Machine{DiskFree: 100 * gib, CacheSizeBytes: 40 * gib}).Advice)
}

func TestPruneForCannotPromiseMoreThanTheCacheHolds(t *testing.T) {
	t.Parallel()

	// The worktrees, the module cache and everything else on the volume are
	// somebody else's to remove. A target past the cache would delete every
	// entry and still report the disk short, so it says so first instead.
	want := PruneFor(Machine{DiskFree: 0, CacheSizeBytes: 1 * gib})
	assert.Equal(t, uint64(1*gib), want.Bytes, "it can only ever give back what it holds")
	assert.False(t, want.Enough, "and it has to admit when that is not enough")
}

// cacheDir builds a cache shaped the way cmd/go shapes one: hex shards of
// `<hash>-a` and `<hash>-d`, an executable cache entry that is a *directory*,
// a fuzz corpus, and the bookkeeping files that are not entries at all.
func cacheDir(t *testing.T) (dir string, execEntry string) {
	t.Helper()

	dir = t.TempDir()
	now := time.Now()

	write := func(path string, size int, age time.Duration) {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, make([]byte, size), 0o600))
		at := now.Add(-age)
		require.NoError(t, os.Chtimes(path, at, at))
	}

	// Not entries, and never removable.
	write(filepath.Join(dir, "trim.txt"), 8, 0)
	write(filepath.Join(dir, "README"), 8, 0)
	write(filepath.Join(dir, "fuzz", "corpus", "seed"), 4096, 500*time.Hour)

	// Forty plain entries, one MiB each, oldest last.
	for i := range 40 {
		write(filepath.Join(dir, fmt.Sprintf("%02x", i), fmt.Sprintf("%064x-d", i)),
			1<<20, time.Duration(i)*time.Hour)
	}

	// A non-entry file living inside a shard, which the suffix rule must skip.
	write(filepath.Join(dir, "00", "lock"), 8, 0)

	// The executable cache entry: a directory, and the oldest thing here.
	execEntry = filepath.Join(dir, "ff", strings.Repeat("f", 64)+"-d")
	write(filepath.Join(execEntry, "bin"), 2<<20, 100*time.Hour)
	old := now.Add(-100 * time.Hour)
	require.NoError(t, os.Chtimes(execEntry, old, old))

	return dir, execEntry
}

func TestPruneCacheTakesTheOldestAndStopsAtTheTarget(t *testing.T) {
	t.Parallel()

	dir, _ := cacheDir(t)

	freed, removed, err := pruneCache(dir, 10<<20)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, freed, uint64(10<<20), "it has to give back what was asked")
	assert.Positive(t, removed)

	// Stops at the target rather than emptying the cache: the whole point is
	// keeping the hot entries that `go clean -cache` throws away.
	assert.Less(t, freed, uint64(20<<20), "it took far more than it was asked for")

	assert.FileExists(t, filepath.Join(dir, "00", fmt.Sprintf("%064x-d", 0)),
		"the newest entry is the last thing that should go")
	assert.NoFileExists(t, filepath.Join(dir, "27", fmt.Sprintf("%064x-d", 39)),
		"the oldest plain entry should be the first")
}

func TestPruneCacheRemovesAnExecutableEntryWhole(t *testing.T) {
	t.Parallel()

	// An entry may be a directory — cmd/go's own trim calls RemoveAll on one —
	// and a prune that only knew about files would leave the largest, oldest
	// thing in the cache untouched while reporting success.
	dir, execEntry := cacheDir(t)

	_, _, err := pruneCache(dir, 1<<20)
	require.NoError(t, err)

	assert.NoDirExists(t, execEntry, "the oldest entry is a directory, and it should have gone first")
}

func TestPruneCacheLeavesWhatIsNotAnEntry(t *testing.T) {
	t.Parallel()

	dir, _ := cacheDir(t)

	// Ask for everything, so nothing survives by not being reached.
	_, _, err := pruneCache(dir, 1<<30)
	require.NoError(t, err)

	assert.FileExists(t, filepath.Join(dir, "fuzz", "corpus", "seed"),
		"a fuzz corpus is inputs that once expanded coverage — machine-hours to find again, "+
			"and `go help cache` says removing them makes fuzzing less effective")
	assert.FileExists(t, filepath.Join(dir, "trim.txt"),
		"cmd/go's own bookkeeping is not ours to delete")
	assert.FileExists(t, filepath.Join(dir, "00", "lock"),
		"only `-a` and `-d` names are entries, wherever they sit")
}

func TestPruneCacheDoesNothingWithoutATarget(t *testing.T) {
	t.Parallel()

	dir, _ := cacheDir(t)
	before := treeSize(dir)

	freed, removed, err := pruneCache(dir, 0)
	require.NoError(t, err)
	assert.Zero(t, freed)
	assert.Zero(t, removed)
	assert.Equal(t, before, treeSize(dir), "a zero target is a machine that needs nothing")

	freed, removed, err = pruneCache("", 1<<20)
	require.NoError(t, err)
	assert.Zero(t, freed)
	assert.Zero(t, removed, "no GOCACHE is not an error, it is nothing to do")
}
