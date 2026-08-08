package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// defaultConverterWiring is the one line in this package allowed to name the
// SDK's default data converter: the default [New] gives a server that
// configured nothing.
const defaultConverterWiring = "dataConverter: converter.GetDefaultDataConverter(),"

// The one-constant rule, applied to a converter instead of a number.
//
// CLAUDE.md's "both drivers must agree" section names the shape every
// disagreement found so far has had: a value with one meaning, written down
// twice, in two places nothing imports together. The data converter is exactly
// that value. The write side is whatever converter the Temporal client was
// built with, so a read site that reaches for
// converter.GetDefaultDataConverter() on its own is not a shortcut, it is a
// second answer to a question with one answer, and on a deployment with a
// payload codec configured it is an outage: memos decode to nothing,
// [FlowstateServer.ownedBy] answers false, and every tenant is told "no such
// run" about runs it owns.
//
// So the rule is mechanical rather than a matter of review attention. There is
// one construction of the default, in [New], and every read goes through
// s.dataConverter. A grep is the right enforcement here precisely because the
// mistake is a *textual* reach for a package-level function: there is no type
// that can forbid it, and a test asserting one call site's behavior cannot see
// a new call site added next year.
//
// Test files are exempt: a test asserting what the default converter does with
// a codec-written payload is the point of
// TestCodecMemosAreReadThroughTheConfiguredConverter.
func TestNoReadSiteBuildsItsOwnDefaultDataConverter(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		source, err := os.ReadFile(filepath.Clean(name))
		require.NoError(t, err)
		checked++

		for _, line := range strings.Split(string(source), "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.Contains(trimmed, "converter.GetDefaultDataConverter()") {
				continue
			}
			// A comment explaining the rule is not a violation of it.
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if trimmed == defaultConverterWiring {
				continue
			}

			t.Errorf("%s: %s\n\nread this through s.dataConverter instead. The default converter is "+
				"constructed once, in New, and a second construction here decodes nothing on a "+
				"deployment that configured a payload codec. See WithDataConverter.", name, trimmed)
		}
	}

	require.Greater(t, checked, 0, "this guard read no source files, so it proves nothing")

	// And the one permitted construction has to still be there: a guard that
	// passes because New stopped setting a converter at all would be worse than
	// no guard, since every read would then panic on a nil interface.
	wiring, err := os.ReadFile("server.go")
	require.NoError(t, err)
	require.Contains(t, string(wiring), defaultConverterWiring,
		"New no longer defaults the data converter, so nothing sets one for an unconfigured deployment")
}
