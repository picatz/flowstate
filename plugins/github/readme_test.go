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
// CLAUDE.md applied to prose instead of code: an empty extraction must fail
// loudly rather than pass by accident, because a test that can pass by
// finding nothing proves nothing.
//
// The convention, documented in README.md's own "Examples, kept honest"
// section: an HTML comment naming a file, `<!-- example: <path> -->`, on the
// line immediately before a ```yaml fence. <path> is repository-root-relative
// (this plugin is its own module, but its examples live under the repo's
// top-level examples/ tree, per examples/plugins/github/README.md's own
// rule against examples/github/workflow.yaml). This test:
//
//  1. extracts every such fenced block from README.md and requires it to be
//     byte-identical to the file it names on disk;
//  2. requires every file under examples/plugins/github/ to be named by some
//     block in README.md, so an example added there and never pasted in
//     fails too, not just one that drifts once pasted;
//  3. asserts a floor on the number of blocks found, so a convention nobody's
//     README follows cannot pass by extracting zero of anything.
func TestReadmeExamplesMatchTheFilesOnDisk(t *testing.T) {
	repoRoot := repoRootFromGithubPlugin(t)

	readmePath := filepath.Join(repoRoot, "plugins", "github", "README.md")
	readme, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("reading %s: %v", readmePath, err)
	}

	blocks := extractExampleBlocks(t, string(readme))

	// The floor: three example files exist today (workflow.yaml,
	// issue-comment.yaml, triage.yaml), so finding fewer than three means the
	// extraction convention itself broke, not that an example is briefly
	// missing.
	const minBlocks = 3
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

	// Direction two: every file the examples directory actually has must be
	// named by some block above, so an example added and never pasted into
	// the README fails here instead of silently going undocumented.
	exampleDir := filepath.Join(repoRoot, "examples", "plugins", "github")
	entries, err := os.ReadDir(exampleDir)
	if err != nil {
		t.Fatalf("reading %s: %v", exampleDir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		relPath := "examples/plugins/github/" + entry.Name()
		if !seen[relPath] {
			t.Errorf("%s exists but no fenced block in plugins/github/README.md names it via "+
				"<!-- example: %s -->", relPath, relPath)
		}
	}
}

// exampleBlock is one fenced ```yaml block README.md pastes in whole, keyed
// by the repository-root-relative path the preceding HTML comment names.
type exampleBlock struct {
	path string
	body string
}

// exampleCommentPattern matches the convention's HTML comment line:
// "<!-- example: <path> -->", allowing the ordinary whitespace variance a
// human editing markdown by hand produces.
var exampleCommentPattern = regexp.MustCompile(`^<!--\s*example:\s*(\S+)\s*-->$`)

// extractExampleBlocks walks README.md's lines, pairing each
// "<!-- example: path -->" comment with the ```yaml fence immediately after
// it and returning the fence's body verbatim (no trailing newline, matching
// what a fenced block "contains" between its opening and closing lines).
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
			// A ```yaml fence with no preceding "example:" comment is not part
			// of this convention (e.g. a short inline snippet); skip past it
			// without collecting it, rather than misattributing its contents.
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

// repoRootFromGithubPlugin walks up from this test's directory to the
// repository root — the directory holding both CLAUDE.md and a go.mod,
// which is unique to the root: plugins/github has its own go.mod but no
// CLAUDE.md, so checking for both together does not stop one level too
// early inside this plugin's own module.
func repoRootFromGithubPlugin(t *testing.T) string {
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
