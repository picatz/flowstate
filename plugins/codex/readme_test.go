package main

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestReadmeExamplesMatchTheFilesOnDisk is the corpus-test lesson from
// CLAUDE.md applied to prose instead of code, copied from
// plugins/github/readme_test.go (a different module, so not imported): an
// empty extraction must fail loudly rather than pass by accident, because a
// test that can pass by finding nothing proves nothing.
//
// The convention, documented in README.md's own "Examples, kept honest"
// section: an HTML comment naming a file, `<!-- example: <path> -->`, on the
// line immediately before a ```yaml fence. <path> is repository-root-relative.
// This test:
//
//  1. extracts every such fenced block from README.md and requires it to be
//     byte-identical to the file it names on disk;
//  2. requires every file under examples/plugins/codex/ to be named by some
//     block in README.md, so an example added there and never pasted in
//     fails too, not just one that drifts once pasted;
//  3. asserts a floor on the number of blocks found, so a convention
//     nobody's README follows cannot pass by extracting zero of anything.
func TestReadmeExamplesMatchTheFilesOnDisk(t *testing.T) {
	repoRoot := repoRootFromCodexPlugin(t)

	readmePath := filepath.Join(repoRoot, "plugins", "codex", "README.md")
	readme, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("reading %s: %v", readmePath, err)
	}

	blocks := extractExampleBlocks(t, string(readme))

	const minBlocks = 1
	if len(blocks) < minBlocks {
		t.Fatalf("extracted %d example block(s) from README.md, want at least %d — "+
			"the <!-- example: path --> convention may have broken", len(blocks), minBlocks)
	}

	seen := make(map[string]bool, len(blocks))
	for _, b := range blocks {
		if seen[b.path] {
			t.Errorf("README.md names %q more than once; each example file should appear exactly once", b.path)
		}
		seen[b.path] = true

		diskPath := filepath.Join(repoRoot, filepath.FromSlash(b.path))
		diskContents, err := os.ReadFile(diskPath)
		if err != nil {
			t.Errorf("README.md names %q, but reading it failed: %v", b.path, err)
			continue
		}

		if !bytes.Equal(diskContents, []byte(b.body)) {
			t.Errorf("README.md's fenced block for %q does not match the file on disk byte for byte — "+
				"the file is the truth (it's what CI runs); fix the README to match it, never the reverse", b.path)
		}
	}

	exampleDir := filepath.Join(repoRoot, "examples", "plugins", "codex")
	entries, err := os.ReadDir(exampleDir)
	if err != nil {
		t.Fatalf("reading %s: %v", exampleDir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		relPath := "examples/plugins/codex/" + entry.Name()
		if !seen[relPath] {
			t.Errorf("%s exists but no fenced block in plugins/codex/README.md names it via "+
				"<!-- example: %s -->", relPath, relPath)
		}
	}
}

type exampleBlock struct {
	path string
	body string
}

var exampleCommentPattern = regexp.MustCompile(`^<!--\s*example:\s*(\S+)\s*-->$`)

func extractExampleBlocks(t *testing.T, readme string) []exampleBlock {
	t.Helper()

	var blocks []exampleBlock

	scanner := bufio.NewScanner(strings.NewReader(readme))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var pendingPath string
	var inFence bool
	var fenceLines []string

	flush := func() {
		if pendingPath == "" {
			return
		}
		blocks = append(blocks, exampleBlock{
			path: pendingPath,
			body: strings.Join(fenceLines, "\n") + "\n",
		})
		pendingPath = ""
		fenceLines = nil
	}

	for scanner.Scan() {
		line := scanner.Text()

		if inFence {
			if strings.TrimSpace(line) == "```" {
				inFence = false
				flush()
				continue
			}
			fenceLines = append(fenceLines, line)
			continue
		}

		if match := exampleCommentPattern.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			pendingPath = match[1]
			continue
		}

		if strings.TrimSpace(line) == "```yaml" {
			if pendingPath != "" {
				inFence = true
				fenceLines = nil
				continue
			}
			for scanner.Scan() {
				if strings.TrimSpace(scanner.Text()) == "```" {
					break
				}
			}
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning README.md: %v", err)
	}

	return blocks
}

// repoRootFromCodexPlugin walks up from this test's directory to the
// repository root - the directory holding both CLAUDE.md and a go.mod,
// which is unique to the root: plugins/codex has its own go.mod but no
// CLAUDE.md, so checking for both together does not stop one level too
// early inside this plugin's own module.
func repoRootFromCodexPlugin(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}

	for range 10 {
		_, claudeErr := os.Stat(filepath.Join(dir, "CLAUDE.md"))
		_, modErr := os.Stat(filepath.Join(dir, "go.mod"))
		if claudeErr == nil && modErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	t.Fatal("walked to the filesystem root without finding the repository root (CLAUDE.md + go.mod together)")

	return ""
}
