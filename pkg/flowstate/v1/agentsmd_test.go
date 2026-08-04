package flowstatev1_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAgentsMdExistsAndStaysShort is the drift tripwire on AGENTS.md, the
// repo-root file OpenAI Codex and other agent tools read by convention.
//
// CLAUDE.md is the canonical guidance for this repository, and AGENTS.md
// exists only to point agent tools at it — never to duplicate it. A
// duplicate is a second copy that can silently disagree with the original
// the moment either one is edited without the other, which is exactly the
// class of bug "one constant cannot disagree with itself" (CLAUDE.md, "Both
// execution drivers must agree") describes for values instead of prose. So
// this test does not check AGENTS.md's content in depth; it checks the one
// property that keeps it a pointer rather than a fork: it exists, it tells
// a reader where the real guidance and the verification gate live, and it
// stays short enough that growing it into a second CLAUDE.md would be
// visibly wrong before it got there.
func TestAgentsMdExistsAndStaysShort(t *testing.T) {
	t.Parallel()

	root := repoRootDir(t)
	path := filepath.Join(root, "AGENTS.md")

	contents, err := os.ReadFile(path)
	require.NoError(t, err, "AGENTS.md must exist at the repository root for agent tools that read it by convention")

	require.Contains(t, string(contents), "CLAUDE.md",
		"AGENTS.md must point readers at CLAUDE.md, the canonical guidance — it must never duplicate it")
	require.Contains(t, string(contents), "make check",
		"AGENTS.md must name `make check` as the verification gate, the same one CLAUDE.md and CI use")

	const maxLines = 60
	lines := bytes.Count(contents, []byte("\n"))
	if len(contents) > 0 && !bytes.HasSuffix(contents, []byte("\n")) {
		lines++
	}
	require.LessOrEqual(t, lines, maxLines,
		"AGENTS.md has grown to %d lines; keep it under %d so nobody grows it into a fork of CLAUDE.md — "+
			"add detail to CLAUDE.md and point to it instead", lines, maxLines)

	// A single-source pointer should not carry whole invariant sections of its
	// own — those belong in CLAUDE.md exactly once. This is a light heuristic,
	// not a full duplication check: a heading that names an invariant CLAUDE.md
	// already owns is the shape a fork would start taking.
	for _, heading := range []string{
		"## Proto-first",
		"## Secrets never enter workflow history",
		"## Both execution drivers must agree",
	} {
		require.False(t, strings.Contains(string(contents), heading),
			"AGENTS.md contains %q, a CLAUDE.md section heading verbatim — that is the shape of a duplicate "+
				"starting to drift; keep the guidance in CLAUDE.md and link to it instead", heading)
	}
}
