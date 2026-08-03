package main

import "testing"

func TestValidateOwnerRepoRefusesEmptyAndMalformedNames(t *testing.T) {
	for _, v := range []string{"", "-leading-dash", "trailing-dash-", "has a space", "has/slash", "..", string(make([]byte, 200))} {
		if err := validateOwnerRepo("owner", v); err == nil {
			t.Errorf("validateOwnerRepo(%q): got no error, want one", v)
		}
	}
}

func TestValidateOwnerRepoAcceptsOrdinaryNames(t *testing.T) {
	for _, v := range []string{"octocat", "hello-world", "a", "my.repo_name-2"} {
		if err := validateOwnerRepo("owner", v); err != nil {
			t.Errorf("validateOwnerRepo(%q): unexpected error: %v", v, err)
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
