package flowtest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// FuzzLoadSource fuzzes the `*.test.yaml` parser on the path an outside party
// reaches (#932): the MCP `flowstate_test` tool hands submitted YAML straight
// to [flowtest.RunSourceContext] → [flowtest.LoadSource], below every
// argument-envelope check `FuzzMCPToolArguments` deliberately stops at. The
// bounds are careful and hand-checked — `MaxTestFileBytes` streamed, the
// count caps, the alias-expansion walk over the AST — but that walk is a
// deliberate re-implementation of flowfile's, and its own comment names the
// residual: a subtly different bound would only *look* like the real one.
// This target is the check that comment asks for.
//
// Invariants: no panic and no unbounded growth under the harness's GOMEMLIMIT
// (the run recipe in CLAUDE.md), and a document that loaded loads again — the
// idempotence shape the flowfile targets already use, since a parser that
// answers differently on the same bytes is a parser with hidden state.
func FuzzLoadSource(f *testing.F) {
	// Seeds: the shipped corpus (every example suite) plus this package's own
	// fixture suites, so the fuzzer starts from documents exercising every
	// stanza the format has rather than discovering YAML from nothing.
	for _, pattern := range []string{
		"../../../../examples/*/workflow.test.yaml",
		"testdata/*/*.test.yaml",
	} {
		seeds, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		for _, seed := range seeds {
			if data, err := os.ReadFile(seed); err == nil {
				f.Add(data)
			}
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		file, err := flowtest.LoadSource(data)
		if err != nil {
			return
		}
		if file == nil {
			t.Fatal("a load that reported success handed back nothing")
		}

		again, err := flowtest.LoadSource(data)
		if err != nil || again == nil {
			t.Fatalf("a document that loaded must load again: %v", err)
		}
	})
}
