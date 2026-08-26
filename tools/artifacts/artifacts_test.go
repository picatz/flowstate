// Package artifacts holds one check: this repository tracks no compiled
// executable.
//
// It exists because the mistake has now been made three times, by three
// different people, in exactly one way — `go build ./some/tool` writes its
// binary into the repository root, and the next `git add -A` commits it:
//
//   - `embedding`, 46 MB, in ff6131b, removed by #922.
//   - `gate`, five megabytes, on the branch that added its `.gitignore` line.
//   - `vacuity`, 3.3 MB, in #1125 — where it was removed from the index once
//     and came back, because a later build recreated it and a later `git add
//     -A` picked it up again.
//
// Each was answered with a `.gitignore` entry naming that one path, which
// prevents that one recurrence and nothing else. The entries are still right
// and still there; this is the part that generalises, in the mold the rest of
// `tools/` uses — walk the tree, fail on disagreement — so the fourth tool
// somebody builds in place is caught by a test rather than by a reviewer
// reading a diff that is one line of source and three megabytes of ELF.
//
// A committed binary is not merely untidy. It is in every clone forever, it is
// machine-specific so it is wrong for most people who get it, and nothing
// consumes it — every one of the three was built by hand while `make` ran the
// tool through `go run`.
package artifacts

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// executables are the leading bytes of a compiled program, by format.
//
// Matched on magic rather than on size or on a name pattern, because the
// question is "is this a program" and those two answer something else. The
// largest thing this repository legitimately tracks is a 412 KB protobuf
// descriptor set, which a size rule would have to be tuned around and which
// this does not look at twice.
//
// The list is *measured* rather than remembered, and the first version was
// remembered — ELF, Mach-O and PE, which is what a person thinks of and leaves
// five of Go's own targets able to commit a binary past a check named "no
// compiled executable" (Codex, #1126). Building a trivial program for every
// entry in `go tool dist list` gives eight distinct prefixes:
//
//	7f 45 4c 46  every Linux, BSD, Solaris and illumos target, and Android
//	cf fa ed fe  darwin/amd64, darwin/arm64
//	4d 5a 90 00  windows/386, windows/amd64, windows/arm64
//	00 61 73 6d  js/wasm, wasip1/wasm
//	01 f7 00 0a  aix/ppc64
//	00 00 8a 97  plan9/amd64
//	00 00 01 eb  plan9/386
//	00 00 06 47  plan9/arm
//
// Plan 9's magic is per-architecture, which is why three of them are here and
// why a fourth architecture would need a fourth line — the cost of a format
// that puts the machine in the header. Re-run the measurement after a toolchain
// bump that adds a port.
//
// The two Mach-O entries no current Go target emits are kept anyway: this asks
// whether a tracked file is a program, and a program can arrive from somewhere
// other than this repository's own `go build`.
// sweep asks for the cross-build calibration below, which `make check` sets.
const sweep = "FLOWSTATE_ARTIFACT_SWEEP"

var executables = map[string][]byte{
	"ELF":              {0x7f, 'E', 'L', 'F'},
	"Mach-O 64":        {0xcf, 0xfa, 0xed, 0xfe},
	"Mach-O 32":        {0xce, 0xfa, 0xed, 0xfe},
	"Mach-O universal": {0xca, 0xfe, 0xba, 0xbe},
	// Two bytes, not the four a Go build writes: "MZ" is the whole of what
	// makes a DOS or PE image, and matching it covers a program this
	// repository's toolchain did not produce.
	"PE":           {'M', 'Z'},
	"WebAssembly":  {0x00, 'a', 's', 'm'},
	"XCOFF (AIX)":  {0x01, 0xf7, 0x00, 0x0a},
	"Plan 9 amd64": {0x00, 0x00, 0x8a, 0x97},
	"Plan 9 386":   {0x00, 0x00, 0x01, 0xeb},
	"Plan 9 arm":   {0x00, 0x00, 0x06, 0x47},
}

// TestNoCompiledExecutableIsTracked is the check.
func TestNoCompiledExecutableIsTracked(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed, and the tracked set is what this asks about")
	}

	// The root, asked for rather than counted in `..` segments. The first
	// version of this walked from `tools/` and found 37 files — caught
	// immediately by the floor below, which is the whole reason that assertion
	// is there and not a formality.
	top, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("git rev-parse: %v (not a checkout, so there is no tracked set)", err)
	}
	root := strings.TrimSpace(string(top))

	// The *tracked* set, not the working tree: an ignored binary sitting in
	// somebody's checkout is the system working, and flagging it would make
	// this fail for everyone who has ever run `go build` here.
	listing := exec.Command("git", "ls-files", "-z")
	listing.Dir = root
	out, err := listing.Output()
	if err != nil {
		t.Skipf("git ls-files: %v (not a checkout, so there is no tracked set)", err)
	}

	paths := strings.Split(strings.TrimRight(string(out), "\x00"), "\x00")
	require.Greater(t, len(paths), 100,
		"the listing found %d tracked file(s), which is too few to be this repository — "+
			"a check that walks nothing and reports everything is fine is the failure it "+
			"exists to catch", len(paths))

	examined := 0

	// The paths on their own beside the annotated lines, because the message
	// below builds a command out of one. Deriving it by splitting the
	// annotation on spaces broke for any path containing one, which is a
	// command that looks right and removes the wrong file (Copilot, #1126).
	var (
		committed []string
		paths2    []string
	)
	for _, path := range paths {
		if path == "" {
			continue
		}

		file, err := os.Open(filepath.Join(root, path))
		if err != nil {
			// Tracked and absent from the working tree: a sparse checkout, or
			// a file this test's own run deleted. Nothing to read, nothing to
			// claim.
			continue
		}

		head := make([]byte, 4)
		n, _ := file.Read(head)
		_ = file.Close()
		examined++

		for kind, magic := range executables {
			if n >= len(magic) && bytes.HasPrefix(head[:n], magic) {
				committed = append(committed, path+" ("+kind+")")
				paths2 = append(paths2, path)

				break
			}
		}
	}

	require.Greater(t, examined, 100,
		"only %d tracked file(s) could be read, so this reported a clean tree without "+
			"looking at one", examined)

	assert.Empty(t, committed,
		"a compiled executable is tracked in this repository. It is in every clone "+
			"forever and is wrong for every machine but the one that built it. Remove it "+
			"and ignore the path it is built to:\n\n"+
			"%s\n\n"+
			"then add those paths to .gitignore beside /flow, /gate, /vacuity and "+
			"/embedding, which are there for this.\n\nFound:\n%s",
		removals(paths2), strings.Join(committed, "\n"))
}

// removals is the command to run, one line per path.
//
// `--` before the paths, so one beginning with a dash is a path rather than a
// flag; and quoted, so one containing a space survives a copy and paste. Both
// are properties of a *message*, which is the whole product of a failing check:
// a command that looks right and does something else is worse than no command.
func removals(paths []string) string {
	lines := make([]string, 0, len(paths))
	for _, path := range paths {
		lines = append(lines, "    git rm --cached -- "+strconv.Quote(path))
	}

	return strings.Join(lines, "\n")
}

// TestEveryGoTargetsExecutableIsRecognised keeps the list from being the three
// formats a person thinks of.
//
// The first version was ELF, Mach-O and PE, which left five of Go's own
// targets able to commit a binary straight past a check named "no compiled
// executable" (Codex, #1126). This builds a trivial program for every entry in
// `go tool dist list` and asserts that what comes out is recognised — so the
// list is checked against the toolchain rather than against my memory of it,
// and a port added by a future toolchain fails here rather than silently
// widening the hole.
//
// Cross-compilation without cgo needs nothing installed, but a target can still
// refuse to build in a given environment; those are skipped individually, and
// the count below is what keeps the whole test from passing by building
// nothing.
func TestEveryGoTargetsExecutableIsRecognised(t *testing.T) {
	t.Parallel()

	// Opt-in, and the number is why. Building a program for every entry in
	// `go tool dist list` takes about 107 seconds, and what it buys is notice
	// that Go has added a port with an executable format nobody has seen — a
	// thing that happens perhaps once in several years. Paying that on every
	// pull request would be the most expensive check in the suite guarding the
	// rarest event in it.
	//
	// So the *gate* is the tracked-file scan above, which costs about twenty
	// milliseconds and runs always; this is the calibration behind its list,
	// run when the toolchain moves. `make check` sets the variable, so the full
	// local rehearsal covers it.
	if os.Getenv(sweep) == "" {
		t.Skipf("set %s=1 to build one program per Go target (~2 minutes); the tracked-file "+
			"check above runs unconditionally", sweep)
	}

	targets, err := exec.Command("go", "tool", "dist", "list").Output()
	if err != nil {
		t.Skipf("go tool dist list: %v", err)
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.go"),
		[]byte("package main\n\nfunc main() {}\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"),
		[]byte("module probe\n\ngo 1.24\n"), 0o600))

	built := 0

	var unrecognised []string
	for _, target := range strings.Fields(string(targets)) {
		goos, goarch, ok := strings.Cut(target, "/")
		if !ok {
			continue
		}

		out := filepath.Join(dir, "out.bin")
		build := exec.Command("go", "build", "-o", out, ".")
		build.Dir = dir
		build.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
		if err := build.Run(); err != nil {
			// A target this environment cannot cross-build. Not a finding: the
			// count below is what says whether enough of them worked.
			continue
		}

		head, err := os.ReadFile(out)
		require.NoError(t, err)
		_ = os.Remove(out)
		built++

		found := false
		for _, magic := range executables {
			if bytes.HasPrefix(head, magic) {
				found = true

				break
			}
		}
		if !found {
			leading := head
			if len(leading) > 4 {
				leading = leading[:4]
			}
			unrecognised = append(unrecognised, fmt.Sprintf("%s (% x)", target, leading))
		}
	}

	require.Greater(t, built, 20,
		"only %d target(s) built, which is too few to say anything about the list — a "+
			"check that compiled nothing and reported every format covered is the failure "+
			"this file is about", built)

	assert.Empty(t, unrecognised,
		"a Go target produces an executable this check does not recognise, so committing "+
			"one passes a gate named for refusing exactly that. Add its magic to "+
			"`executables`:\n\n%s", strings.Join(unrecognised, "\n"))
}
