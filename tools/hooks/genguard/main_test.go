package main

import (
	"strings"
	"testing"
)

// TestGenerated checks both directions: every generated surface refuses
// with a message naming its source and regenerate command, and the source
// files that live beside generated ones stay editable. The second half is
// the one that keeps the guard trusted; refusing an edit to sync.go would
// teach people the guard is wrong.
func TestGenerated(t *testing.T) {
	t.Parallel()

	refuse := []struct {
		rel      string
		mentions []string // the source and command the message must name
	}{
		{"pkg/flowstate/v1/flowstate.pb.go", []string{"proto/flowstate/v1/flowstate.proto", "buf", "generate"}},
		{"pkg/flowstate/v1/plugin/protocol.pb.go", []string{"buf", "generate"}},
		{"docs/reference/tasks.md", []string{"make docs"}},
		{"docs/reference/envvars.md", []string{"cmd/flow/internal/docsgen/envvars.go"}},
		{"cmd/flow/internal/reference/mirror/DSL.md", []string{"docs/DSL.md", "go generate ./cmd/flow/internal/reference"}},
		{"cmd/flow/internal/reference/mirror/examples/hello.yaml", []string{"go generate ./cmd/flow/internal/reference"}},
		{"pkg/flowstate/v1/protodoc/flowstate.descriptorset.binpb", []string{"buf", "build"}},
	}
	for _, tt := range refuse {
		msg := generated(tt.rel)
		if msg == "" {
			t.Errorf("generated(%q) allowed a generated file", tt.rel)
			continue
		}
		for _, want := range tt.mentions {
			if !strings.Contains(msg, want) {
				t.Errorf("generated(%q) message %q does not name %q", tt.rel, msg, want)
			}
		}
	}

	allow := []string{
		"pkg/flowstate/v1/eval.go",
		"proto/flowstate/v1/flowstate.proto",
		"docs/DSL.md",
		"docs/ARCHITECTURE.md",
		// The reference package's own source, beside the generated mirror.
		"cmd/flow/internal/reference/reference.go",
		"cmd/flow/internal/reference/reference_test.go",
		"cmd/flow/internal/reference/sync.go",
		// Sibling of the descriptor set that is not the descriptor set.
		"pkg/flowstate/v1/protodoc/protodoc.go",
		// A name that merely contains a guarded substring.
		"pkg/flowstate/v1/pbgo_helpers.go",
	}
	for _, rel := range allow {
		if msg := generated(rel); msg != "" {
			t.Errorf("generated(%q) refused a source file: %s", rel, msg)
		}
	}
}

// TestProjectRel pins the resolution rules: absolute paths resolve against
// the project root, relative paths against the session cwd, and anything
// outside the project maps to nothing.
func TestProjectRel(t *testing.T) {
	t.Setenv("CLAUDE_PROJECT_DIR", "/repo")

	tests := []struct {
		path, cwd, want string
	}{
		{"/repo/docs/reference/tasks.md", "/repo", "docs/reference/tasks.md"},
		{"docs/reference/tasks.md", "/repo", "docs/reference/tasks.md"},
		{"reference/tasks.md", "/repo/docs", "docs/reference/tasks.md"},
		{"/elsewhere/file.pb.go", "/repo", ""},
		{"../outside.pb.go", "/repo", ""},
		{"", "/repo", ""},
	}
	for _, tt := range tests {
		if got := projectRel(tt.path, tt.cwd); got != tt.want {
			t.Errorf("projectRel(%q, %q) = %q, want %q", tt.path, tt.cwd, got, tt.want)
		}
	}
}
