package auth_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestExamplePoliciesParse walks every trust-policy document under examples/
// and parses it through the same entry point a deployment uses.
//
// The example Flowfiles are validated in CI and the policy files beside them
// were not: a policy that drifts from the schema, or that names a claim shape
// the grammar refuses, would sit in a walkthrough looking authoritative while
// failing for whoever copies it. Parsing is the half a test can hold; whether
// a claim an issuer actually mints satisfies the namespace grammar is a
// property of the issuer, characterized in ci_federation_test.go.
func TestExamplePoliciesParse(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "..", "..", "examples")
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		switch d.Name() {
		case "auth-policy.yaml", "trust.yaml":
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if len(files) == 0 {
		t.Fatalf("no policy files found under %s; the walk or the layout changed", root)
	}

	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		if _, err := auth.ParsePolicy(data); err != nil {
			t.Errorf("%s does not parse: %v", path, err)
		}
	}
}
