package flowtest_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

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
// (the run recipe in CLAUDE.md), and a document that loaded loads again *to
// the same document* — the idempotence shape the flowfile targets already
// use, since a parser that answers differently on the same bytes is a parser
// with hidden state. The comparison is the invariant's teeth: two successful
// loads that disagree are exactly that hidden state, and checking only the
// second call's error would stay green through it.
func FuzzLoadSource(f *testing.F) {
	// Seeds: the shipped corpus (every example suite, walked recursively —
	// two live under examples/plugins/, one of them not named
	// workflow.test.yaml, so a fixed-depth glob misses real suites) plus this
	// package's own fixture suites, so the fuzzer starts from documents
	// exercising every stanza the format has rather than discovering YAML
	// from nothing.
	for _, root := range []string{"../../../../examples", "testdata"} {
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".test.yaml") {
				return nil
			}
			if data, err := os.ReadFile(path); err == nil {
				f.Add(data)
			}
			return nil
		})
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

		// EquateNaNs: YAML admits `.nan`, and NaN != NaN would otherwise
		// report a perfectly deterministic parse as a divergence. The
		// exporter compares unexported fields (today: Stub.fromDefaults,
		// stamped by mergeDefaults at load) instead of ignoring them —
		// hidden state that diverges in a field callers cannot see is
		// still hidden state, and skipping it would blind the invariant
		// exactly where it is hardest to notice.
		exporter := cmp.Exporter(func(reflect.Type) bool { return true })

		// The one unexported field deliberately not compared, and the reason is
		// not that it is awkward: File.doc is the parsed YAML kept so a run-time
		// failure can be placed in the file (#1558). It is a *derived index over
		// these very bytes*, so walking two copies of it re-asserts that the YAML
		// parser is deterministic — which is not this invariant — at the cost of
		// deep-comparing an entire AST per seed, which turned this target from
		// 0.2s into a timeout.
		//
		// Compared instead: that both loads agree on whether there is one at all,
		// so the field is not simply unchecked. What the field points *into* is
		// covered where it is actually used, by the position table test.
		ignoreDoc := cmpopts.IgnoreFields(flowtest.File{}, "doc")
		if (file.HasPositions()) != (again.HasPositions()) {
			t.Fatalf("the same bytes loaded once with positions and once without: %t then %t",
				file.HasPositions(), again.HasPositions())
		}

		if diff := cmp.Diff(file, again, cmpopts.EquateNaNs(), exporter, ignoreDoc); diff != "" {
			t.Fatalf("the same bytes loaded to two different documents (-first +again):\n%s", diff)
		}
	})
}
