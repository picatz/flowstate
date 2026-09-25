package main

import (
	"context"
	"strings"
	"testing"
)

// The tests in this file cover #694: every paginated list task fingerprints
// the API base its cursor was issued against, and that base has to be the
// one the request actually goes to. Since #663 an authenticated call
// ignores its own base_url input and uses GITHUB_API_BASE_URL, so a
// fingerprint over the input describes nothing at all for those calls - it
// is the empty string whichever instance the walk ran against.
//
// They are written in the negative direction CLAUDE.md asks for: not "a
// cursor resumes its own walk" (which passed throughout the defect) but
// "a cursor from one instance is refused by another."

// apiBaseFor is what a list task does in production: hand newClient the
// token and the task's own base_url input, and fingerprint whatever base it
// reports pointing the client at.
func apiBaseFor(t *testing.T, token, baseURL string) string {
	t.Helper()
	_, base, err := newClient(token, baseURL)
	if err != nil {
		t.Fatalf("newClient(%q, %q): %v", token, baseURL, err)
	}
	return base
}

// TestCursorFingerprintSeparatesTwoAuthenticatedInstances is #694's own
// collision, asserted for all three list tasks at once: one query, two
// GitHub instances selected by the operator, and no difference at all in
// what the caller passed - the base_url input is empty on both sides,
// exactly as an authenticated call always leaves it.
//
// Before the fix every one of these pairs was equal, which is what let a
// cursor issued against one instance resume a walk against the other.
func TestCursorFingerprintSeparatesTwoAuthenticatedInstances(t *testing.T) {
	const (
		instanceA = "https://github.example.com/api/v3"
		instanceB = "https://github.other-example.com/api/v3"
	)

	baseFor := func(configured string) string {
		t.Setenv(envAPIBaseURL, configured)
		// The empty string is the base_url input: a task's own input is
		// never what an authenticated call reaches, and passing it here is
		// what makes this a test of the effective base rather than of the
		// input.
		return apiBaseFor(t, "credential", "")
	}

	baseA, baseB := baseFor(instanceA), baseFor(instanceB)
	if baseA == baseB {
		t.Fatalf("effective API base for two configured instances is the same %q", baseA)
	}
	if baseA != instanceA || baseB != instanceB {
		t.Fatalf("effective API bases = %q, %q; want %q, %q", baseA, baseB, instanceA, instanceB)
	}

	issueA, issueB := stableIssueListParams(5), stableIssueListParams(5)
	issueA.apiBase, issueB.apiBase = baseA, baseB
	if issueListFingerprint("o", "r", issueA) == issueListFingerprint("o", "r", issueB) {
		t.Error("issue_list fingerprints collide across two API bases")
	}

	prA, prB := stablePullRequestListParams(5), stablePullRequestListParams(5)
	prA.apiBase, prB.apiBase = baseA, baseB
	if pullRequestListFingerprint("o", "r", prA) == pullRequestListFingerprint("o", "r", prB) {
		t.Error("pull_request_list fingerprints collide across two API bases")
	}

	if pullRequestFilesFingerprint("o", "r", 1, 5, baseA) == pullRequestFilesFingerprint("o", "r", 1, 5, baseB) {
		t.Error("pull_request_files fingerprints collide across two API bases")
	}
}

// TestIssueListCursorRefusesAnotherInstancesCursor walks the whole path the
// fingerprint exists to protect: a real cursor, minted while the operator
// had one instance configured, replayed after the configuration moved to
// another. The refusal - not a page of that other instance's issues
// presented as a continuation - is the fix.
func TestIssueListCursorRefusesAnotherInstancesCursor(t *testing.T) {
	client := newPagedTestServer(t, 20, 4, issueJSON)

	t.Setenv(envAPIBaseURL, "https://github.example.com/api/v3")
	issued := stableIssueListParams(5)
	issued.apiBase = apiBaseFor(t, "credential", "")
	_, _, cursor, err := doIssueList(context.Background(), client, "o", "r", issued)
	if err != nil {
		t.Fatalf("producing a cursor: unexpected error: %v", err)
	}
	if cursor == "" {
		t.Fatal("expected a next_cursor")
	}

	t.Setenv(envAPIBaseURL, "https://github.other-example.com/api/v3")
	replayed := stableIssueListParams(5)
	replayed.apiBase = apiBaseFor(t, "credential", "")
	replayed.cursor = cursor
	_, _, _, err = doIssueList(context.Background(), client, "o", "r", replayed)
	if err == nil {
		t.Fatal("a cursor from another instance was accepted, want a refusal")
	}
	if !strings.Contains(err.Error(), "different filters") {
		t.Fatalf("error = %q, want it to name the mismatch", err)
	}
}

// TestCursorFingerprintAcceptsEquivalentSpellingsOfOneAPIBase is the other
// direction, and the reason canonicalAPIBase exists: three ways of naming
// api.github.com - an unset base_url, an explicit one, and one with a
// trailing slash - are one endpoint, so a cursor issued under any of them
// resumes under any other rather than being refused for a difference that
// is not one.
func TestCursorFingerprintAcceptsEquivalentSpellingsOfOneAPIBase(t *testing.T) {
	t.Setenv(envAPIBaseURL, "")

	unset := apiBaseFor(t, "", "")
	explicit := apiBaseFor(t, "", defaultAPIBaseURL)
	trailing := apiBaseFor(t, "", defaultAPIBaseURL+"/")

	for _, got := range []string{unset, explicit, trailing} {
		if got != defaultAPIBaseURL {
			t.Fatalf("effective API base = %q, want %q for every spelling of github.com's", got, defaultAPIBaseURL)
		}
	}

	fingerprintFor := func(base string) fingerprint {
		p := stableIssueListParams(5)
		p.apiBase = base
		return issueListFingerprint("o", "r", p)
	}
	if fingerprintFor(unset) != fingerprintFor(explicit) || fingerprintFor(explicit) != fingerprintFor(trailing) {
		t.Fatal("three spellings of one API base fingerprinted differently")
	}
}

// TestUnauthenticatedCursorFingerprintFollowsTheTaskBaseURL keeps the
// unauthenticated half honest: nothing here is operator-configured, so the
// task's own base_url IS the effective base, and two of them must still
// separate.
func TestUnauthenticatedCursorFingerprintFollowsTheTaskBaseURL(t *testing.T) {
	t.Setenv(envAPIBaseURL, "https://github.example.com/api/v3")

	a := apiBaseFor(t, "", "https://ghes-a.example.com/api/v3")
	b := apiBaseFor(t, "", "https://ghes-b.example.com/api/v3")

	if a != "https://ghes-a.example.com/api/v3" {
		t.Fatalf("unauthenticated effective base = %q, want the task's own base_url - the operator's base "+
			"configures where a CREDENTIAL may go, not where an unauthenticated call goes", a)
	}

	pa, pb := stableIssueListParams(5), stableIssueListParams(5)
	pa.apiBase, pb.apiBase = a, b
	if issueListFingerprint("o", "r", pa) == issueListFingerprint("o", "r", pb) {
		t.Fatal("issue_list fingerprints collide across two unauthenticated base_url values")
	}
}

// TestIssueListFingerprintSeparatesLabelSets is the encoding-ambiguity half
// of the same class: labels used to be joined with a comma before hashing,
// so ["a", "b"] and ["a,b"] - different filters, different result sets -
// produced the same bytes and therefore the same fingerprint, and a cursor
// from one walk resumed the other.
func TestIssueListFingerprintSeparatesLabelSets(t *testing.T) {
	two, one := stableIssueListParams(5), stableIssueListParams(5)
	two.labels = []string{"a", "b"}
	one.labels = []string{"a,b"}
	if issueListFingerprint("o", "r", two) == issueListFingerprint("o", "r", one) {
		t.Fatal(`labels ["a", "b"] and ["a,b"] fingerprinted identically`)
	}

	empty, none := stableIssueListParams(5), stableIssueListParams(5)
	empty.labels = []string{""}
	none.labels = nil
	if issueListFingerprint("o", "r", empty) == issueListFingerprint("o", "r", none) {
		t.Fatal("one empty label and no labels at all fingerprinted identically")
	}
}
