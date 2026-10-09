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
	const text = "edition: v2026.4\nname: lines\nsteps:\n  - id: first\n    log:\n      message: one\noutputs: {}\n"
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

// TestTheScreenIsHandedTheTextOfEveryDocumentTheMapNames: the Flowfile as it
// was compiled (not as it is on disk now) and each file it calls, in the map's
// own names, so the source pane can match each to the digest the map records.
func TestTheScreenIsHandedTheTextOfEveryDocumentTheMapNames(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root, child := filepath.Join(dir, "main.yaml"), filepath.Join(dir, "child.yaml")
	const mainText = "edition: v2026.4\nname: main\nsteps:\n  - id: nested\n    call: ./child.yaml\n"
	const childText = "edition: v2026.4\nname: child\nsteps:\n  - id: greet\n    log:\n      message: hi\n"
	for path, text := range map[string]string{root: mainText, child: childText} {
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	workflow, source, err := loadMappedWorkflow(root)
	if err != nil {
		t.Fatal(err)
	}
	sourceMap := source.sourceMap(workflow)

	// The root changes on disk after the read; the screen is given what was compiled.
	if err := os.WriteFile(root, []byte("# later\n"+mainText), 0o600); err != nil {
		t.Fatal(err)
	}
	documents := source.documents(sourceMap)
	if len(documents) != 2 {
		t.Fatalf("got %d documents, want the Flowfile and its callee", len(documents))
	}
	for _, document := range documents {
		want := map[string]string{root: mainText, child: childText}[document.URI]
		if string(document.Text) != want {
			t.Errorf("%s: got %q, want %q", document.URI, document.Text, want)
		}
		if got := v1.ContentDigest(document.Text); !slicesContainsDigest(sourceMap, got) {
			t.Errorf("%s: its digest %s is not one the map records", document.URI, got)
		}
	}

	var none *debugSource
	if got := none.documents(sourceMap); got != nil {
		t.Errorf("no program gave documents: %v", got)
	}
}

func slicesContainsDigest(sourceMap *v1.DebugSourceMap, digest string) bool {
	for _, document := range sourceMap.GetDocuments() {
		if document.GetDigest() == digest {
			return true
		}
	}

	return false
}
