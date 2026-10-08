// Package linguist_test checks the prepared GitHub Linguist entry in this
// directory against the repository it describes. Nothing here is submitted.
package linguist_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestSamplesParse fails a sample the Flowfile parser refuses: a Linguist sample
// that is not a valid Flowfile teaches the classifier the wrong thing.
func TestSamplesParse(t *testing.T) {
	paths, err := filepath.Glob("samples/Flowfile/*")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no samples found (err %v)", err)
	}
	for _, p := range paths {
		if !strings.HasSuffix(p, ".flow.yaml") && !strings.HasSuffix(p, ".flow.yml") {
			t.Errorf("%s: a sample must use an extension the entry claims", p)
		}
		if _, _, err := flowfile.ParseFile(p); err != nil {
			t.Errorf("%s does not parse: %v", p, err)
		}
	}
}

// TestEveryExtensionHasASample holds Linguist's rule that each declared
// extension needs a sample of its own.
func TestEveryExtensionHasASample(t *testing.T) {
	entry := readFile(t, "languages.yml")
	for _, ext := range []string{".flow.yaml", ".flow.yml"} {
		if !strings.Contains(entry, `"`+ext+`"`) {
			t.Fatalf("languages.yml does not declare %s", ext)
		}
		if m, _ := filepath.Glob("samples/Flowfile/*" + ext); len(m) == 0 {
			t.Errorf("no sample for %s", ext)
		}
	}
}

// TestEntryMatchesEditors keeps the Linguist names equal to the ones the editor
// configs detect, so a rename in one place cannot leave the other behind.
func TestEntryMatchesEditors(t *testing.T) {
	entry := readFile(t, "languages.yml")
	for _, want := range []string{`".flow.yaml"`, `".flow.yml"`, "- Flowfile", "tm_scope: source.flowfile"} {
		if !strings.Contains(entry, want) {
			t.Errorf("languages.yml lacks %s", want)
		}
	}
	var manifest struct {
		Contributes struct {
			Languages []struct {
				ID           string   `json:"id"`
				FilenamePats []string `json:"filenamePatterns"`
				Filenames    []string `json:"filenames"`
			} `json:"languages"`
		} `json:"contributes"`
	}
	if err := json.Unmarshal([]byte(readFile(t, "../vscode/package.json")), &manifest); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, l := range manifest.Contributes.Languages {
		if l.ID != "flowfile" {
			continue
		}
		found = true
		for _, g := range []string{"**/*.flow.yaml", "**/*.flow.yml"} {
			if !slices.Contains(l.FilenamePats, g) {
				t.Errorf("VS Code flowfile language lacks %s", g)
			}
		}
		if !slices.Contains(l.Filenames, "Flowfile") {
			t.Errorf("VS Code flowfile language lacks the Flowfile filename")
		}
	}
	if !found {
		t.Fatal("VS Code manifest has no flowfile language")
	}
}

// TestGrammarScopes checks that grammars.yml registers the scope the bundled
// TextMate grammar declares, and not source.cel: Linguist already vendors that
// scope and rejects a duplicate.
func TestGrammarScopes(t *testing.T) {
	var g struct {
		ScopeName string `json:"scopeName"`
	}
	if err := json.Unmarshal([]byte(readFile(t, "../vscode/syntaxes/flowfile.tmLanguage.json")), &g); err != nil {
		t.Fatal(err)
	}
	grammars := readFile(t, "grammars.yml")
	if !strings.Contains(grammars, "\n- "+g.ScopeName+"\n") {
		t.Errorf("grammars.yml does not register %s", g.ScopeName)
	}
	if strings.Contains(grammars, "\n- source.cel") {
		t.Error("grammars.yml registers source.cel, which Linguist already vendors")
	}
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
