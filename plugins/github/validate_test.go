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
