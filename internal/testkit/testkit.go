// Package testkit holds the test helpers more than one package of this module
// needs, so that each package stops carrying its own copy.
//
// Every function here was found three or four times across `pkg` and `cmd`,
// byte for byte, because a test helper cannot be imported across a package
// boundary without a package to import it from (#1709). This is that package.
// It is `internal`, it is imported only by `_test.go` files, and it has no
// opinion about the code under test: what lives here is scaffolding a test
// needs before it can ask its question — a namespace name, the repository
// root, a recorded span. What a helper needs the engine's own types for lives
// beside the conformance cases instead (pkg/flowstate/v1/internal/conformance),
// since a package the engine imports cannot test itself against one that
// imports the engine.
//
// The rule for adding to it is the one AGENTS.md gives for any abstraction:
// it must remove real duplication. A helper one package needs stays in that
// package.
package testkit

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// namespaceOrdinal makes each namespace name unique, since two subtests of one
// parent share a sanitized name. One counter for the binary: the packages that
// register namespaces each had their own, which was fine while each was its
// own test binary and is fine now for the same reason.
var namespaceOrdinal atomic.Int64

// processToken tells this process's namespaces from another's on a server both
// reach. Six hex characters: short enough to keep a name readable, and the
// collision it has to avoid is between the handful of runs that share one
// server in one sitting, not between all runs ever.
var processToken = func() string {
	var b [3]byte
	// Never fails on a supported platform; the runtime aborts rather than
	// return a short read.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}()

// NamespaceNameFor derives a legal Temporal namespace name from a test's name.
//
// Named after the test so that a line in a server log, or a namespace left
// behind by a crash, says which test produced it. Numbered because two subtests
// of one parent sanitize to the same string, and because a name that collides
// would give one test another's runs — the exact isolation a namespace per test
// is there to provide. A per-process token is in it because a server can
// outlive the process (temporaltest.AddressEnv, #1738): a second run of the
// same package, or another package with a test of the same name, then
// registers against namespaces the first left behind, and the token is what
// keeps the name a new one. Random rather than the process id, since two runs
// in fresh containers can share a pid and still share a server (Codex, #1842).
func NamespaceNameFor(t testing.TB) string {
	t.Helper()

	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			return r
		default:
			return '-'
		}
	}, t.Name())

	// Long enough to identify a test, short enough to stay readable in a log line.
	const maxNameLength = 48
	if len(safe) > maxNameLength {
		safe = safe[:maxNameLength]
	}

	return fmt.Sprintf("%s-%s-%d", safe, processToken, namespaceOrdinal.Add(1))
}

// RepoRoot walks up from the test's working directory to the directory holding
// go.mod, for a test that reads a file the repository ships — an example, a
// document, a golden — by a path from the root rather than by counting `..`.
//
// Bounded at ten directories: nothing in this module is deeper, and a walk that
// reached the filesystem root would otherwise loop on its own parent.
func RepoRoot(t testing.TB) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "walked to the filesystem root without finding go.mod")
		dir = parent
	}

	t.Fatal("go.mod not found within ten directories of the test")

	return ""
}
