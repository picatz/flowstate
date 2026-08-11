package flowfile

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTheEnvironmentCacheIsBounded is the rule this file's own cache has to follow.
//
// The key is the set of identifiers an expression mentions, which comes out of the
// document — so without a cap, how much a long-lived language server holds is a
// choice made by whoever wrote the file it was asked to check. Past the cap nothing
// is stored and the cost is the uncached one, which is the right way for a cache to
// fail: slower, not larger.
func TestTheEnvironmentCacheIsBounded(t *testing.T) {
	// Not parallel: it fills a package-level cache and then measures its size, which
	// another test validating anything at the same time would change.
	for i := range maxCachedEnvs * 2 {
		// A distinct name per file, so each one is a cache key nothing else uses.
		src := strings.Join([]string{
			"edition: v2026.3",
			"name: check",
			"steps:",
			"  - id: say",
			"    vars:",
			fmt.Sprintf("      n%d: hello", i),
			"    log:",
			fmt.Sprintf("      message: ${n%d}", i),
			"",
		}, "\n")

		ds, err := ValidateSource([]byte(src))
		require.NoError(t, err)
		require.Empty(t, ds, "the fixture itself is wrong, so this measures nothing")
	}

	envCacheMu.RLock()
	held := len(envCache)
	envCacheMu.RUnlock()

	assert.LessOrEqual(t, held, maxCachedEnvs,
		"the environment cache grew past its bound, so a document decides how much a language server holds")

	// And it was reached, rather than the test having quietly cached almost nothing
	// — `held <= 512` is also satisfied by a cache that never stores anything.
	assert.Equal(t, maxCachedEnvs, held,
		"the bound was never reached, so this does not test the bound")
}
