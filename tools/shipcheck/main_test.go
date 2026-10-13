package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/picatz/flowstate/internal/shipcheck"
)

const current = "0123456789abcdef0123456789abcdef01234567"

func runWith(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	origCheck, origHead := check, head
	t.Cleanup(func() { check, head = origCheck, origHead })
	check = func(string, int) (shipcheck.Result, error) {
		t.Fatal("the attest path must not run the check")
		return shipcheck.Result{}, nil
	}
	head = func(string, int) (string, error) { return current, nil }
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// TestAttestPrintsOnlyForTheReviewedCurrentHead: an attestation records a
// reviewer's verdict on one head, so --attest without --head is a usage
// error, a --head that is not the current head prints nothing, and the
// current head prints the body the check accepts.
func TestAttestPrintsOnlyForTheReviewedCurrentHead(t *testing.T) {
	if code, stdout, _ := runWith(t, "--pr", "7", "--attest", "flowstate-reviewer"); code != 2 || stdout != "" {
		t.Fatalf("--attest without --head: exit %d, stdout %q; want usage and no body", code, stdout)
	}
	if code, stdout, _ := runWith(t, "--pr", "7", "--head", current); code != 2 || stdout != "" {
		t.Fatalf("--head without --attest: exit %d, stdout %q; want usage", code, stdout)
	}
	code, stdout, stderr := runWith(t, "--pr", "7", "--attest", "flowstate-reviewer", "--head", strings.Repeat("f", 40))
	if code != 1 || stdout != "" || !strings.Contains(stderr, "review the current head") {
		t.Fatalf("stale --head: exit %d, stdout %q, stderr %q; want a refusal and no body", code, stdout, stderr)
	}
	code, stdout, stderr = runWith(t, "--pr", "7", "--attest", "flowstate-reviewer", "--head", current)
	want, err := shipcheck.Attestation("flowstate-reviewer", current)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 || stdout != want {
		t.Fatalf("current --head: exit %d, stdout %q (stderr %q), want %q", code, stdout, stderr, want)
	}
}
