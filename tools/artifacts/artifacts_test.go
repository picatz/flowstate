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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// executables are the leading bytes of a compiled program, by platform.
//
// Matched on magic rather than on size or on a name pattern, because the
// question is "is this a program" and those two answer something else. The
// largest thing this repository legitimately tracks is a 412 KB protobuf
// descriptor set, which a size rule would have to be tuned around and which
// this does not look at twice.
var executables = map[string][]byte{
	"ELF":              {0x7f, 'E', 'L', 'F'},
	"Mach-O 64":        {0xcf, 0xfa, 0xed, 0xfe},
	"Mach-O 32":        {0xce, 0xfa, 0xed, 0xfe},
	"Mach-O universal": {0xca, 0xfe, 0xba, 0xbe},
	"PE":               {'M', 'Z'},
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

	var committed []string
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
			"    git rm --cached %s\n\n"+
			"then add that path to .gitignore beside /flow, /gate, /vacuity and "+
			"/embedding, which are there for this.\n\nFound: %s",
		strings.Split(strings.Join(committed, " "), " ")[0], strings.Join(committed, "\n"))
}
