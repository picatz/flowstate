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
