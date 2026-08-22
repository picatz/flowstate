package fuzztargets

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot is this package's module root: tools/fuzztargets is two levels down.
const repoRoot = "../.."

// TestEveryFuzzTargetInTheTreeIsListed is the check that makes this file a
// source rather than a fifth copy. A list of targets maintained by hand is only
// as good as whoever remembered to add to it, and the drift #857 fixes started
// exactly there — a target written, added to one runner, and missing from
// another. So the list is compared against the tree it describes: every
// `func Fuzz…(f *testing.F)` in this module must appear here, in the directory
// it was declared in, and nothing may be listed that no longer exists.
func TestEveryFuzzTargetInTheTreeIsListed(t *testing.T) {
	found := fuzzTargetsInTree(t)

	listed := map[string]string{}
	for _, target := range All() {
		if prior, dup := listed[target.Name]; dup {
			t.Errorf("targets.txt lists %s twice (%s and %s)", target.Name, prior, target.Dir)
		}
		listed[target.Name] = target.Dir
	}

	for name, dir := range found {
		switch listedDir, ok := listed[name]; {
		case !ok:
			t.Errorf("%s is declared in %s and listed in no tier; add it to tools/fuzztargets/targets.txt (deep at least — that tier runs every target)", name, dir)
		case listedDir != dir:
			t.Errorf("%s is declared in %s but targets.txt says %s", name, dir, listedDir)
		}
	}
	for name, dir := range listed {
		if _, ok := found[name]; !ok {
			t.Errorf("targets.txt lists %s in %s, but no such fuzz target is declared in the tree", name, dir)
		}
	}
}

// TestEveryTargetRunsInTheDeepTier. The deep tier's whole claim, in its own
// comment, is that it runs every target in the repository for ten minutes each;
// before #857 it ran four of ten. Nothing about a target argues for excluding
// it from a weekly run nobody waits on, so the claim is asserted rather than
// left to a reader to re-check. The smoke tier deliberately carries a subset —
// that is a budget decision, written down in targets.txt beside the targets it
// excludes.
func TestEveryTargetRunsInTheDeepTier(t *testing.T) {
	for _, target := range All() {
		if !target.InTier(TierDeep) {
			t.Errorf("%s is not in the deep tier; the deep tier runs every target", target.Name)
		}
	}
	if len(InTier(TierSmoke)) == 0 {
		t.Error("no target is in the smoke tier, so the required per-push job would test nothing")
	}
}

// TestTheShellReaderAgreesWithTheGoReader. Two programs read targets.txt: this
// package, for tools/gate, and list.sh, for the Makefile and deep.yml. One file
// read two ways is the same drift risk one list written twice is, one level
// down — so the shell reader's output is compared with the Go reader's for
// every tier.
func TestTheShellReaderAgreesWithTheGoReader(t *testing.T) {
	for _, tier := range []string{TierSmoke, TierDeep} {
		t.Run(tier, func(t *testing.T) {
			cmd := exec.Command("./list.sh", tier)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("list.sh %s: %v\n%s", tier, err, stderr.String())
			}

			var want strings.Builder
			for _, target := range InTier(tier) {
				want.WriteString(target.Name + " " + target.Dir + "\n")
			}
			if got := string(out); got != want.String() {
				t.Errorf("list.sh %s printed:\n%s\nwant:\n%s", tier, got, want.String())
			}
		})
	}
}

// TestNoRunnerHoldsItsOwnCopyOfTheList. The fix is only a fix while the copies
// stay gone: a target name spelled into the Makefile or into either workflow is
// a fifth list starting over. Every runner reaches the targets through
// list.sh, so none of those files should name a target at all.
func TestNoRunnerHoldsItsOwnCopyOfTheList(t *testing.T) {
	for _, path := range []string{
		"Makefile",
		".github/workflows/ci.yml",
		".github/workflows/deep.yml",
	} {
		data, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range All() {
			// A prose mention in a comment is fine and often the
			// point (ci.yml explains why particular targets
			// exist); a `-fuzz <name>` invocation is the copy.
			for _, line := range strings.Split(string(data), "\n") {
				if strings.Contains(line, "-fuzz ") && strings.Contains(line, target.Name) {
					t.Errorf("%s runs %s by name: %q — read it from tools/fuzztargets/targets.txt instead", path, target.Name, strings.TrimSpace(line))
				}
			}
		}
	}
}

var fuzzFunc = regexp.MustCompile(`(?m)^func (Fuzz[A-Za-z0-9_]*)\(f \*testing\.F\)`)

// fuzzTargetsInTree returns every fuzz target declared in this module, mapped
// to the module-relative directory it is declared in. plugins/ is skipped: they
// are separate modules, outside this module's build graph, so neither tier can
// reach them (the same gap CLAUDE.md records for `make coverage`).
func fuzzTargetsInTree(t *testing.T) map[string]string {
	t.Helper()

	found := map[string]string{}
	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "plugins", "testdata", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		dir, err := filepath.Rel(repoRoot, filepath.Dir(path))
		if err != nil {
			return err
		}
		for _, m := range fuzzFunc.FindAllStringSubmatch(string(data), -1) {
			found[m[1]] = filepath.ToSlash(dir)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 {
		t.Fatal("walked the tree and found no fuzz targets at all, which means this test cannot fail for the reason it exists")
	}
	return found
}
