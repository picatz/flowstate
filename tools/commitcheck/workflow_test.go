package main

import (
	"os"
	"strings"
	"testing"

	yaml "github.com/goccy/go-yaml"
)

// commitcheckWorkflow is the slice of .github/workflows/commitcheck.yml this
// test reads.
type commitcheckWorkflow struct {
	Jobs map[string]struct {
		Steps []struct {
			Name string `yaml:"name"`
			Run  string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

// TestTheWorkflowPassesStrict is the check #2024 found missing: the package
// doc and this workflow both described a flip that no code ever made, and
// nothing failed when neither passed -strict. Removing the flag now makes
// "Commit conventions" advisory again — [decision]'s own tests cannot see
// that, since they never read this file — so this pins the one line that
// decides it.
func TestTheWorkflowPassesStrict(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/commitcheck.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow commitcheckWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse commitcheck workflow: %v", err)
	}
	job, ok := workflow.Jobs["commitcheck"]
	if !ok {
		t.Fatal("commitcheck.yml has no commitcheck job")
	}
	var run string
	for _, step := range job.Steps {
		if strings.Contains(step.Run, "tools/commitcheck") {
			run = step.Run
		}
	}
	if want := "go run ./tools/commitcheck -strict"; run != want {
		t.Fatalf("commitcheck step runs %q, want %q", run, want)
	}
}
