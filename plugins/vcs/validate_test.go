package main

import "testing"

// A malformed scheme must be refused, not the closest-looking safe one
// substituted for it: an author who typed the wrong URL deserves a
// diagnostic naming what they wrote, not a task that silently reached
// somewhere else.
func TestValidateRepositoryURLRefusesEveryNonHTTPSScheme(t *testing.T) {
	for _, raw := range []string{
		"http://example.com/owner/repo.git",
		"file:///etc/passwd",
		"git://example.com/owner/repo.git",
		"ssh://git@example.com/owner/repo.git",
		"ext::sh -c 'touch pwned'",
		"",
	} {
		if _, err := validateRepositoryURL(raw); err == nil {
			t.Errorf("validateRepositoryURL(%q): got no error, want one - a non-https URL must never reach go-git's transport", raw)
		}
	}
}

// A URL carrying a userinfo component is a way to smuggle a credential into
// a Flowfile as a literal, bypassing the token input's secret-reference
// requirement entirely. It must be refused outright, not stripped: silently
// dropping the credential would run the request unauthenticated instead of
// telling the author their file does something other than what they wrote.
func TestValidateRepositoryURLRefusesEmbeddedUserinfo(t *testing.T) {
	if _, err := validateRepositoryURL("https://x-access-token:ghp_secret@example.com/o/r.git"); err == nil {
		t.Fatal("validateRepositoryURL with an embedded credential: got no error, want one")
	}
}

func TestValidateRepositoryURLAcceptsAnOrdinaryHTTPSURL(t *testing.T) {
	u, err := validateRepositoryURL("https://example.com/owner/repo.git")
	if err != nil {
		t.Fatalf("validateRepositoryURL: unexpected error: %v", err)
	}
	if u.Host != "example.com" {
		t.Fatalf("host: got %q, want %q", u.Host, "example.com")
	}
}

func TestValidateRepositoryURLBoundsLength(t *testing.T) {
	huge := "https://example.com/" + string(make([]byte, maxURLBytes))
	if _, err := validateRepositoryURL(huge); err == nil {
		t.Fatal("validateRepositoryURL with an oversized url: got no error, want one")
	}
}

func TestValidateRevisionRefusesControlCharacters(t *testing.T) {
	for _, raw := range []string{"main\x00", "main\nrm -rf /", "\x1b[31mmain"} {
		if _, err := validateRevision("ref", raw); err == nil {
			t.Errorf("validateRevision(%q): got no error, want one", raw)
		}
	}
}

func TestValidateRevisionAcceptsOrdinaryNames(t *testing.T) {
	for _, raw := range []string{"", "main", "v1.2.3", "HEAD~3", "refs/heads/feature/x"} {
		if _, err := validateRevision("ref", raw); err != nil {
			t.Errorf("validateRevision(%q): unexpected error: %v", raw, err)
		}
	}
}

// fetchDepthForMaxCommits (log.go) fetches maxCommits+1 without a clamp of
// its own, on the assumption that the ceiling clampMaxCommits already
// enforces (maxMaxCommits) leaves room below maxCloneDepth for the extra
// commit. This test exists so that changing either constant without
// checking the other trips here, in a name that says why, rather than
// quietly letting fetchDepthForMaxCommits ask cloneBounded for a depth
// cloneBounded's own bound then refuses.
func TestMaxMaxCommitsFetchDepthNeverExceedsMaxCloneDepth(t *testing.T) {
	if got := fetchDepthForMaxCommits(maxMaxCommits); got > maxCloneDepth {
		t.Fatalf("fetchDepthForMaxCommits(maxMaxCommits) = %d, exceeds maxCloneDepth (%d)", got, maxCloneDepth)
	}
}

func TestClampMaxCommitsRefusesRatherThanSilentlyClamps(t *testing.T) {
	if _, err := clampMaxCommits(maxMaxCommits + 1); err == nil {
		t.Fatal("clampMaxCommits over the ceiling: got no error, want one - " +
			"a silently clamped bound would look like a working request that quietly returned less than asked for")
	}
	if got, err := clampMaxCommits(0); err != nil || got != defaultMaxCommits {
		t.Fatalf("clampMaxCommits(0): got (%d, %v), want (%d, nil)", got, err, defaultMaxCommits)
	}
	if _, err := clampMaxCommits(-1); err == nil {
		t.Fatal("clampMaxCommits(-1): got no error, want one")
	}
}
