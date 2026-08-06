package main

import (
	"strings"
	"testing"
)

func TestValidateRepositoryURLAcceptsHTTPS(t *testing.T) {
	u, err := validateRepositoryURL("https://example.com/owner/repo.git")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.Scheme != "https" {
		t.Fatalf("scheme = %q, want https", u.Scheme)
	}
}

// TestValidateRepositoryURLRefusesEverySchemeButHTTPS is the allowlist
// direction the security review asked for: refuse by default, not by
// enumerating dangerous schemes one at a time. file:// (arbitrary local
// read), ssh:// (no credential story yet - see doc.go), git:// (no auth at
// all), and the two CLI-git remote-helper schemes that are code execution by
// design (ext::, fd::) all land in the same refusal, for the same reason.
func TestValidateRepositoryURLRefusesEverySchemeButHTTPS(t *testing.T) {
	for _, raw := range []string{
		"http://example.com/owner/repo.git",
		"ssh://git@example.com/owner/repo.git",
		"git://example.com/owner/repo.git",
		"file:///etc/passwd",
		"ext::sh -c touch%20/tmp/pwned",
		"fd::0",
	} {
		if _, err := validateRepositoryURL(raw); err == nil {
			t.Errorf("validateRepositoryURL(%q): got no error, want one - only https:// is allowed", raw)
		}
	}
}

func TestValidateRepositoryURLRefusesUserinfo(t *testing.T) {
	if _, err := validateRepositoryURL("https://token@example.com/owner/repo.git"); err == nil {
		t.Fatal("a url with embedded userinfo was accepted; a credential must travel as a secret reference, never in the url")
	}
}

// full1, full2, full3 are three distinct, syntactically valid full commit
// shas used across this file's cursor-shape tests - not real objects in
// any repository, since validateCursor's own job is checking shape, never
// resolving anything.
const (
	full1 = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
	full2 = "e41dae53697dee0228afc70ea30f32ceeeac4e26"
	full3 = "0e8a687e0ac423825196d7c8dcd7d02fe3f96f83"
)

// TestValidateCursorAcceptsTheFrontierEmittedShape proves the one shape
// LogInputs.cursor accepts: exactly what encodeCursor (cursor.go) produces
// - two "|"-separated, comma-joined lists of full lowercase hex shas.
func TestValidateCursorAcceptsTheFrontierEmittedShape(t *testing.T) {
	raw := full1 + "," + full2 + "|" + full3
	got, err := validateCursor(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != raw {
		t.Fatalf("validateCursor(%q) = %q, want unchanged", raw, got)
	}
}

func TestValidateCursorAcceptsEmpty(t *testing.T) {
	got, err := validateCursor("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Fatalf("validateCursor(\"\") = %q, want empty", got)
	}
}

// TestValidateCursorRefusesAnythingNotShapedLikeAnEncodedCursor is
// LogInputs.cursor's own narrowness, checked directly: unlike ref (which
// accepts anything go-git's revision parser does), a bare single sha (this
// field's own first version's own shape), a branch name, a revision
// expression, an uppercase sha, a sha of the wrong length, and a
// single-section or empty-section value are all refused - a cursor is
// never something a caller composes, only ever something this task itself
// previously emitted via encodeCursor.
func TestValidateCursorRefusesAnythingNotShapedLikeAnEncodedCursor(t *testing.T) {
	for _, tt := range []struct {
		name   string
		cursor string
	}{
		{"bare single sha - this field's own first version's shape", full1},
		{"short sha", full1[:7]},
		{"branch name", "main"},
		{"revision expression", "HEAD~1"},
		{"uppercase sha", strings.ToUpper(full1) + "|" + full2},
		{"one character too long", full1 + "a|" + full2},
		{"one character too short", full1[:39] + "|" + full2},
		{"non-hex characters at full length", strings.Repeat("g", 40) + "|" + full2},
		{"only one section", full1 + "," + full2},
		{"three sections", full1 + "|" + full2 + "|" + full3},
		{"empty frontier section", "|" + full2},
		{"empty emitted section", full1 + "|"},
		{"trailing comma in frontier", full1 + ",|" + full2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := validateCursor(tt.cursor); err == nil {
				t.Fatalf("validateCursor(%q): got nil error, want a refusal", tt.cursor)
			}
		})
	}
}

// TestValidateCursorRefusesMoreEntriesThanMaxCursorEntries proves
// maxCursorEntries is actually enforced at validation time, not merely
// documented - an incoming cursor naming more commits, total, than this
// task will ever track is refused the same way one shaped wrong in any
// other way is, since this task never emits one that does.
func TestValidateCursorRefusesMoreEntriesThanMaxCursorEntries(t *testing.T) {
	frontier := make([]string, maxCursorEntries+1)
	for i := range frontier {
		frontier[i] = full1
	}
	raw := strings.Join(frontier, ",") + "|" + full2
	if _, err := validateCursor(raw); err == nil {
		t.Fatal("validateCursor with more entries than maxCursorEntries: got nil error, want a refusal")
	}
}

func TestValidateBranchNameAcceptsOrdinaryNames(t *testing.T) {
	for _, name := range []string{"main", "feature/x", "release-1.0"} {
		if _, err := validateBranchName(name); err != nil {
			t.Errorf("validateBranchName(%q): unexpected error: %v", name, err)
		}
	}
}

// TestValidateBranchNameRefusesALeadingDash is the ref-name-as-ref check the
// security review asked for even though nothing here ever builds an argv: a
// name a naive downstream consumer of this task's own output could mistake
// for a flag.
func TestValidateBranchNameRefusesALeadingDash(t *testing.T) {
	for _, name := range []string{"-x", "--force", "-"} {
		if _, err := validateBranchName(name); err == nil {
			t.Errorf("validateBranchName(%q): got no error, want one", name)
		}
	}
}

func TestValidateBranchNameRefusesAFullRef(t *testing.T) {
	if _, err := validateBranchName("refs/heads/main"); err == nil {
		t.Fatal("a full ref was accepted as a branch name; this task supplies refs/heads/ itself")
	}
}

func TestValidateBranchNameRefusesGitsOwnInvalidNames(t *testing.T) {
	for _, name := range []string{"", "a..b", "a.lock", "a b", "a~b", "a^b", "a:b", "a?b", "a*b", "a[b", strings.Repeat("x", 300)} {
		if _, err := validateBranchName(name); err == nil {
			t.Errorf("validateBranchName(%q): got no error, want one", name)
		}
	}
}

func TestValidateTreePathAcceptsOrdinaryPaths(t *testing.T) {
	for _, p := range []string{"a.txt", "dir/a.txt", "a/b/c.txt", ".hidden"} {
		if _, err := validateTreePath("files", p); err != nil {
			t.Errorf("validateTreePath(%q): unexpected error: %v", p, err)
		}
	}
}

// TestValidateTreePathRefusesEscapesAndGitWrites is the direct, single-path
// version of the escape and .git/ refusals; commit_push_test.go's
// TestCommitPushRefusesAPathEscapingTheTreeViaPatch and
// TestCommitPushRefusesAWriteUnderDotGit prove the same thing bites at the
// real call site, not only here in isolation.
func TestValidateTreePathRefusesEscapesAndGitWrites(t *testing.T) {
	for _, p := range []string{
		"/etc/passwd",
		"../outside.txt",
		"a/../../outside.txt",
		"a/../b",
		".git/hooks/pre-commit",
		"a/.git/hooks/pre-commit",
		".git",
		"a\\b",
		"",
		"a//b",
		"/a",
		"a/",
	} {
		if _, err := validateTreePath("files", p); err == nil {
			t.Errorf("validateTreePath(%q): got no error, want one", p)
		}
	}
}
