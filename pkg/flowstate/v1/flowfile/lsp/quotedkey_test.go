package lsp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAQuotedKeyWithNothingToDecodeStartsNoParser is #1119's amplification.
//
// The tolerant line scanner hands every complete quoted key to the YAML scalar
// decoder, and the whole-document scans that feed it — workflow completion's
// outline, a test document's symbols — call it once per line. A megabyte holds
// a couple of hundred thousand lines of `"a":`, so one ordinary editor request
// started that many parsers, each building a document, a body node and a token
// stream to answer with the two characters between the quotes.
//
// Both quoting styles have exactly one escape, so a key containing neither is
// its own contents. The allocation assertion is the claim: a parser cannot be
// started without allocating, so a fast path that allocates nothing is a parser
// that did not run.
func TestAQuotedKeyWithNothingToDecodeStartsNoParser(t *testing.T) {
	allocations := testing.AllocsPerRun(2, func() {
		for range 100 {
			_, _ = yamlStringScalar(`"a"`)
			_, _ = yamlStringScalar(`'a'`)
		}
	})

	assert.Zero(t, allocations, "a quoted key with nothing to decode started a parser")
}

// TestQuotedKeysDecodeAsTheLoaderDoes keeps the fast path honest: it may only
// answer where it answers what the decoder would, and everything with an escape
// in it must still reach the decoder.
func TestQuotedKeysDecodeAsTheLoaderDoes(t *testing.T) {
	t.Parallel()

	for src, want := range map[string]string{
		// The fast path's own cases.
		`"a"`:          "a",
		`'a'`:          "a",
		`""`:           "",
		`''`:           "",
		`"with space"`: "with space",
		`"a:b"`:        "a:b",
		`'it"s'`:       `it"s`,
		// Escapes, which must go to the decoder and come back decoded.
		`"a\"b"`:   `a"b`,
		`"a\\b"`:   `a\b`,
		`"a\nb"`:   "a\nb",
		`'it''s'`:  "it's",
		`"\u00e9"`: "é",
	} {
		got, ok := yamlStringScalar(src)
		require.Truef(t, ok, "%s did not decode", src)
		assert.Equalf(t, want, got, "%s decoded differently from the loader", src)
	}

	// A key that is not a legal scalar at all is still refused rather than
	// waved through as its own contents.
	for _, src := range []string{`"unterminated`, `'unterminated`, `"a" trailing`} {
		_, ok := yamlStringScalar(src)
		assert.Falsef(t, ok, "%s was accepted as a scalar", src)
	}
}

// TestAWholeDocumentOfQuotedKeysScansOnce is the amplification at the scale a
// request actually meets: a scan of every line of a document made of quoted
// keys, which is what the outline and the symbol walk each do once per request.
func TestAWholeDocumentOfQuotedKeysScansOnce(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	for i := range 20_000 {
		fmt.Fprintf(&b, "%q:\n", fmt.Sprintf("k%06d", i))
	}

	ix := newLineIndex(b.String())

	scanned := 0
	for l := range ix.lineCount() {
		line := ix.line(l)
		if line == "" {
			// The empty line after the final newline.
			continue
		}
		m, ok := scanKeyLine(line)
		require.Truef(t, ok, "line %d (%q) did not scan as a key", l, line)
		require.NotEmptyf(t, m.key, "line %d decoded to an empty key", l)
		scanned++
	}

	require.Equal(t, 20_000, scanned, "the scan did not reach every key")
}
