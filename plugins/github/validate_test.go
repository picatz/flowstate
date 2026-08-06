package main

import "testing"

// Owner rules and repository rules differ on GitHub, so they get separate
// tests. An owner (a user or org login) never contains a dot or an
// underscore and never starts with, ends with, or doubles up a hyphen.
func TestValidateOwnerRefusesEmptyAndMalformedNames(t *testing.T) {
	for _, v := range []string{
		"", "-leading-dash", "trailing-dash-", "double--hyphen", "has a space",
		"has/slash", "..", ".", "my.repo_name-2", string(make([]byte, 200)),
	} {
		if err := validateOwner("owner", v); err == nil {
			t.Errorf("validateOwner(%q): got no error, want one", v)
		}
	}
}

func TestValidateOwnerAcceptsOrdinaryNames(t *testing.T) {
	for _, v := range []string{"octocat", "hello-world", "a", "a1-b2"} {
		if err := validateOwner("owner", v); err != nil {
			t.Errorf("validateOwner(%q): unexpected error: %v", v, err)
		}
	}
}

// `.github` is a real, commonly-used repository - GitHub's own
// community-health-file repository is named exactly that - and a
// repository name permits a leading dot or underscore where an owner login
// does not. `.` and `..` are refused outright even though they would
// otherwise match the character class, because they are path-meaningful
// rather than ordinary names.
func TestValidateRepoAcceptsLeadingDotAndUnderscore(t *testing.T) {
	for _, v := range []string{".github", "_internal-tools", "my.repo_name-2"} {
		if err := validateRepo("repo", v); err != nil {
			t.Errorf("validateRepo(%q): unexpected error: %v", v, err)
		}
	}
}

func TestValidateRepoRefusesEmptyAndPathMeaningfulNames(t *testing.T) {
	for _, v := range []string{"", ".", "..", "has a space", "has/slash", string(make([]byte, 200))} {
		if err := validateRepo("repo", v); err == nil {
			t.Errorf("validateRepo(%q): got no error, want one", v)
		}
	}
}

func TestValidateNumberRefusesNonPositive(t *testing.T) {
	for _, n := range []int64{0, -1, -100} {
		if err := validateNumber("number", n); err == nil {
			t.Errorf("validateNumber(%d): got no error, want one", n)
		}
	}
}

func TestValidateCommentBodyBoundsLength(t *testing.T) {
	if err := validateCommentBody(""); err == nil {
		t.Fatal("validateCommentBody(\"\"): got no error, want one")
	}
	huge := make([]byte, maxCommentBodyBytes+1)
	for i := range huge {
		huge[i] = 'a'
	}
	if err := validateCommentBody(string(huge)); err == nil {
		t.Fatal("validateCommentBody(oversized): got no error, want one")
	}
	if err := validateCommentBody("looks good, shipping it"); err != nil {
		t.Fatalf("validateCommentBody(ordinary text): unexpected error: %v", err)
	}
}

func TestValidateStateDefaultsToOpenAndAcceptsGithubsThreeValues(t *testing.T) {
	got, err := validateState("state", "")
	if err != nil || got != "open" {
		t.Fatalf("validateState(\"\"): got (%q, %v), want (\"open\", nil)", got, err)
	}
	for _, v := range []string{"open", "closed", "all"} {
		got, err := validateState("state", v)
		if err != nil || got != v {
			t.Errorf("validateState(%q): got (%q, %v), want (%q, nil)", v, got, err, v)
		}
	}
}

func TestValidateStateRefusesAnythingElse(t *testing.T) {
	for _, v := range []string{"OPEN", "merged", "draft", " open"} {
		if _, err := validateState("state", v); err == nil {
			t.Errorf("validateState(%q): got no error, want one", v)
		}
	}
}

func TestClampMaxResultsAppliesDefaultAndCeiling(t *testing.T) {
	got, err := clampMaxResults(0)
	if err != nil || got != defaultMaxResults {
		t.Fatalf("clampMaxResults(0): got (%d, %v), want (%d, nil)", got, err, defaultMaxResults)
	}
	if _, err := clampMaxResults(-1); err == nil {
		t.Fatal("clampMaxResults(-1): got no error, want one")
	}
	if _, err := clampMaxResults(maxMaxResults + 1); err == nil {
		t.Fatalf("clampMaxResults(%d): got no error, want one - over the ceiling and refused, not silently clamped", maxMaxResults+1)
	}
	got, err = clampMaxResults(maxMaxResults)
	if err != nil || got != maxMaxResults {
		t.Fatalf("clampMaxResults(%d): got (%d, %v), want (%d, nil)", maxMaxResults, got, err, maxMaxResults)
	}
}

func TestValidateLabelsBoundsCountAndLength(t *testing.T) {
	if err := validateLabels(nil); err != nil {
		t.Fatalf("validateLabels(nil): unexpected error: %v", err)
	}
	if err := validateLabels([]string{"bug", "security"}); err != nil {
		t.Fatalf("validateLabels(ordinary): unexpected error: %v", err)
	}
	if err := validateLabels([]string{""}); err == nil {
		t.Fatal("validateLabels([\"\"]): got no error, want one")
	}
	tooMany := make([]string, maxLabels+1)
	for i := range tooMany {
		tooMany[i] = "l"
	}
	if err := validateLabels(tooMany); err == nil {
		t.Fatal("validateLabels(too many): got no error, want one")
	}
	huge := make([]byte, maxLabelBytes+1)
	for i := range huge {
		huge[i] = 'a'
	}
	if err := validateLabels([]string{string(huge)}); err == nil {
		t.Fatal("validateLabels(oversized entry): got no error, want one")
	}
}

func TestParseSinceRefusesNonRFC3339(t *testing.T) {
	if _, err := parseSince(""); err != nil {
		t.Fatalf("parseSince(\"\"): unexpected error: %v", err)
	}
	if _, err := parseSince("2026-01-02T15:04:05Z"); err != nil {
		t.Fatalf("parseSince(valid RFC 3339): unexpected error: %v", err)
	}
	if _, err := parseSince("not a time"); err == nil {
		t.Fatal("parseSince(\"not a time\"): got no error, want one")
	}
	if _, err := parseSince("2026-01-02"); err == nil {
		t.Fatal("parseSince(date only, no time): got no error, want one")
	}
}
