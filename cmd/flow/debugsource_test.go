package main

import (
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestADebugSourceMapIsOfTheBytesThatWereCompiled: the program and its lines
// come from one read. A save afterwards that only adds a comment compiles to
// the same program with every step on another line, which no digest can tell
// apart; the map must still point at the lines of the bytes that were run.
func TestADebugSourceMapIsOfTheBytesThatWereCompiled(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "workflow.yaml")
	const text = "edition: v2026.3\nname: lines\nsteps:\n  - id: first\n    log:\n      message: one\noutputs: {}\n"
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	workflow, source, err := loadDebuggedWorkflow(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte("# moved\n# down\n"+text), 0o600); err != nil {
		t.Fatal(err)
	}
	sourceMap := source.sourceMap(workflow)
	if sourceMap == nil {
		t.Fatal("no source map for the compiled read")
	}

	for _, entry := range sourceMap.GetEntries() {
		if v1.DebugSiteKey(entry.GetSite()) == "lines:first" {
			if got := entry.GetLocation().GetRange().GetStartLine(); got != 4 {
				t.Fatalf("step first maps to line %d, want 4, where the compiled bytes put it", got)
			}
			return
		}
	}
	t.Fatal("the source map has no entry for step first")
}
