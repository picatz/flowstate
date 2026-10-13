package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestEveryPhonyNameIsADefinedTarget keeps the Makefile's .PHONY list honest:
// a name run together with its neighbour (docsdocs-preview) names no target,
// and leaves the real one up to date whenever a directory of that name exists.
func TestEveryPhonyNameIsADefinedTarget(t *testing.T) {
	b, err := os.ReadFile("../../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	line := ""
	for l := range strings.SplitSeq(text, "\n") {
		if rest, ok := strings.CutPrefix(l, ".PHONY:"); ok {
			line = rest
			break
		}
	}
	names := strings.Fields(line)
	if len(names) == 0 {
		t.Fatal("Makefile has no .PHONY line")
	}
	listed := map[string]bool{}
	for _, n := range names {
		listed[n] = true
		if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(n) + `(:| [^:\n]*:)`).MatchString(text) {
			t.Errorf(".PHONY lists %q, which no target defines", n)
		}
	}
	for _, want := range []string{"docs", "docs-preview", "temporal-cli"} {
		if !listed[want] {
			t.Errorf(".PHONY does not list %q", want)
		}
	}
}
