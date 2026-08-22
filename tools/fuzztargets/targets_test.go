package fuzztargets

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
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

// TestEveryRunnerReadsTheList is the other half of that: a runner that names no
// target and also reads no list is a runner that stopped fuzzing, which no test
// above would notice. The Makefile and the deep tier loop over list.sh; CI's
// fuzz-smoke job reaches the same list by running the Makefile target, which is
// the trade ci.yml makes for the `test` job too.
func TestEveryRunnerReadsTheList(t *testing.T) {
	for _, tc := range []struct{ path, want string }{
		{"Makefile", "tools/fuzztargets/list.sh smoke"},
		{".github/workflows/deep.yml", "tools/fuzztargets/list.sh deep"},
		{".github/workflows/ci.yml", "make fuzz-smoke"},
	} {
		data, err := os.ReadFile(filepath.Join(repoRoot, tc.path))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), tc.want) {
			t.Errorf("%s no longer contains %q, so it is not reading the target list any more", tc.path, tc.want)
		}
	}
}

// TestDiscoveryFindsTargetsWrittenTheOtherWays. The completeness check above is
// only as good as its idea of what a fuzz target looks like, and the shapes that
// break a text search are ordinary Go: a signature broken across lines because
// it grew a comment, a parameter named anything but `f`, `testing` imported
// under an alias. Each of those is a target `go test` runs, so each has to be
// one this finds — and the near-misses have to stay misses.
func TestDiscoveryFindsTargetsWrittenTheOtherWays(t *testing.T) {
	const src = `package p

import (
	"strings"
	tst "testing"
)

// The shape the tree uses today.
func FuzzOrdinary(f *tst.F) {}

// A signature broken across lines.
func FuzzMultiline(
	// the corpus seeds go in here
	f *tst.F,
) {
}

// A parameter named something else, and an unnamed one.
func FuzzOtherParamName(fuzzer *tst.F) {}
func FuzzUnnamedParam(*tst.F)          {}

// Not targets: a helper whose name only starts with the letters, a method, the
// wrong parameter type, a second parameter, a return value.
func Fuzzy(f *tst.F)                        {}
func (h helper) FuzzMethod(f *tst.F)        {}
func FuzzWrongParam(r *strings.Reader)      {}
func FuzzTwoParams(f *tst.F, n int)         {}
func FuzzReturnsSomething(f *tst.F) error   { return nil }
`
	file, err := parser.ParseFile(token.NewFileSet(), "fuzz_fixture_test.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}

	got := fuzzTargetsInFile(file)
	want := []string{"FuzzOrdinary", "FuzzMultiline", "FuzzOtherParamName", "FuzzUnnamedParam"}
	if !slices.Equal(got, want) {
		t.Errorf("discovered %v, want %v", got, want)
	}
}

// fuzzTargetsInTree returns every fuzz target declared in this module, mapped
// to the module-relative directory it is declared in. plugins/ is skipped: they
// are separate modules, outside this module's build graph, so neither tier can
// reach them (the same gap CLAUDE.md records for `make coverage`).
func fuzzTargetsInTree(t *testing.T) map[string]string {
	t.Helper()

	found := map[string]string{}
	fset := token.NewFileSet()
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
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		dir, err := filepath.Rel(repoRoot, filepath.Dir(path))
		if err != nil {
			return err
		}
		for _, name := range fuzzTargetsInFile(file) {
			found[name] = filepath.ToSlash(dir)
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

// fuzzTargetsInFile returns the fuzz targets one parsed test file declares.
//
// Parsed rather than pattern-matched, and matching the definition `go test`
// itself uses: a top-level func named FuzzXxx taking one *testing.F and
// returning nothing. A regexp over the source text was the first version of
// this and had a hole exactly where the guard is supposed to be tightest — a
// signature broken across lines, a parameter named something other than `f`, or
// `testing` imported under an alias is a target Go runs and the regexp misses,
// which is a target silently in no tier. That is the failure this whole file
// exists to prevent, so the discovery has to be the compiler's answer and not
// an approximation of it.
func fuzzTargetsInFile(file *ast.File) []string {
	testingName := ""
	for _, spec := range file.Imports {
		if spec.Path.Value != `"testing"` {
			continue
		}
		testingName = "testing"
		if spec.Name != nil {
			testingName = spec.Name.Name
		}
	}
	if testingName == "" || testingName == "_" || testingName == "." {
		// A dot-import of testing is legal and would need a different
		// check; nothing in this module does it, and guessing would be
		// worse than the honest answer that this file declares none.
		return nil
	}

	var names []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name == nil {
			continue
		}
		if !isFuzzTargetName(fn.Name.Name) {
			continue
		}
		if fn.Type.TypeParams != nil || fn.Type.Results != nil {
			continue
		}
		if params := fn.Type.Params; params == nil || len(params.List) != 1 || len(params.List[0].Names) > 1 {
			continue
		}
		star, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		sel, ok := star.X.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "F" {
			continue
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != testingName {
			continue
		}
		names = append(names, fn.Name.Name)
	}
	return names
}

// isFuzzTargetName applies `go test`'s own naming rule: FuzzXxx, where the
// character after the prefix does not begin a lower-case word. `Fuzzy` is a
// helper, not a target, and Go will not run it as one.
func isFuzzTargetName(name string) bool {
	rest, ok := strings.CutPrefix(name, "Fuzz")
	if !ok {
		return false
	}
	if rest == "" {
		return true
	}
	r, _ := utf8.DecodeRuneInString(rest)
	return !unicode.IsLower(r)
}
