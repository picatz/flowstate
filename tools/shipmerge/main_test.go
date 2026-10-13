package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/picatz/flowstate/internal/shipcheck"
)

const head = "0123456789abcdef0123456789abcdef01234567"

// fakes replaces the evidence check and gh for one test and records every gh
// call, so a test can prove the merge was or was not attempted and with what.
type fakes struct {
	result   shipcheck.Result
	checkErr error
	checked  bool
	response string
	ghErr    error
	calls    [][]string
	// activity is what the re-read before the merge returns; nil means the
	// same digest the verdict carried.
	activity    *string
	activityErr error
}

func install(t *testing.T, f *fakes) {
	t.Helper()
	origCheck, origActivity, origGH := check, activity, gh
	t.Cleanup(func() { check, activity, gh = origCheck, origActivity, origGH })
	check = func(repo string, number int) (shipcheck.Result, error) {
		f.checked = true
		return f.result, f.checkErr
	}
	activity = func(repo string, number int) (string, error) {
		if f.activity != nil {
			return *f.activity, f.activityErr
		}
		return f.result.Activity, f.activityErr
	}
	gh = func(args ...string) ([]byte, error) {
		f.calls = append(f.calls, args)
		return []byte(f.response), f.ghErr
	}
}

func runWith(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestMergesThePassingHeadWithTheGivenMessage(t *testing.T) {
	f := &fakes{result: shipcheck.Result{Head: head}, response: `{"sha":"feedface","merged":true,"message":"Pull Request successfully merged"}`}
	install(t, f)
	code, stdout, stderr := runWith(t, "--pr", "7", "--subject", "tools: add a merge command", "--body", "Why it exists.")
	if code != 0 {
		t.Fatalf("exit %d, stderr:\n%s", code, stderr)
	}
	if len(f.calls) != 1 {
		t.Fatalf("gh calls = %v, want one merge", f.calls)
	}
	call := f.calls[0]
	for _, want := range []string{"PUT", "repos/picatz/flowstate/pulls/7/merge", "merge_method=squash", "sha=" + head,
		"commit_title=tools: add a merge command (#7)", "commit_message=Why it exists."} {
		if !slices.Contains(call, want) {
			t.Errorf("merge call %q lacks %q", call, want)
		}
	}
	if !strings.Contains(stdout, "as feedface") {
		t.Errorf("stdout = %q, want the merge commit named", stdout)
	}
}

func TestNothingMergesUnlessShipcheckPasses(t *testing.T) {
	for name, f := range map[string]*fakes{
		"problems": {result: shipcheck.Result{Head: head, Problems: []string{"2 review thread(s) remain unresolved"}}},
		"error":    {checkErr: errors.New("GraphQL: 403; REST: 502")},
	} {
		t.Run(name, func(t *testing.T) {
			install(t, f)
			code, _, stderr := runWith(t, "--pr", "7", "--subject", "tools: add a merge command")
			if code == 0 || len(f.calls) != 0 {
				t.Fatalf("exit %d with gh calls %v; a failed check must merge nothing", code, f.calls)
			}
			if !strings.Contains(stderr, "nothing merged") {
				t.Errorf("stderr = %q, want it to say nothing was merged", stderr)
			}
		})
	}
}

// TestActivityAfterTheVerdictBlocksTheMerge: over REST a review edit has no
// timestamp, so the re-read before the merge is what sees it. An edited
// review (a different digest) or a failed re-read merges nothing; an
// unchanged digest merges.
func TestActivityAfterTheVerdictBlocksTheMerge(t *testing.T) {
	edited := "digest-after-edit"
	for name, f := range map[string]*fakes{
		"review edited": {result: shipcheck.Result{Head: head, REST: true, Activity: "digest-at-verdict"}, activity: &edited},
		"re-read fails": {result: shipcheck.Result{Head: head, REST: true, Activity: "digest-at-verdict"}, activityErr: errors.New("REST: 502")},
	} {
		t.Run(name, func(t *testing.T) {
			install(t, f)
			code, _, stderr := runWith(t, "--pr", "7", "--subject", "tools: add a merge command")
			if code == 0 || len(f.calls) != 0 {
				t.Fatalf("exit %d with gh calls %v; changed activity must merge nothing", code, f.calls)
			}
			if !strings.Contains(stderr, "nothing merged") {
				t.Errorf("stderr = %q", stderr)
			}
		})
	}
	t.Run("unchanged", func(t *testing.T) {
		f := &fakes{result: shipcheck.Result{Head: head, REST: true, Activity: "digest-at-verdict"}, response: `{"sha":"feedface","merged":true}`}
		install(t, f)
		if code, _, stderr := runWith(t, "--pr", "7", "--subject", "tools: add a merge command"); code != 0 || len(f.calls) != 1 {
			t.Fatalf("exit %d, gh calls %v: %s", code, f.calls, stderr)
		}
	})
}

// TestTheMessageIsCheckedBeforeTheEvidence: a squash subject that would fail
// the conventions stops the run before the evidence is read or a merge is
// attempted, since it is the cheapest thing to get wrong and to fix.
func TestTheMessageIsCheckedBeforeTheEvidence(t *testing.T) {
	f := &fakes{result: shipcheck.Result{Head: head}}
	install(t, f)
	code, _, stderr := runWith(t, "--pr", "7", "--subject", "Add a merge command")
	if code == 0 || f.checked || len(f.calls) != 0 {
		t.Fatalf("exit %d, checked %v, gh calls %v; a malformed subject must stop the run", code, f.checked, f.calls)
	}
	if !strings.Contains(stderr, "squash message") {
		t.Errorf("stderr = %q, want the convention finding", stderr)
	}
}

func TestAMergeGitHubDoesNotConfirmFails(t *testing.T) {
	for name, f := range map[string]*fakes{
		"refused":     {result: shipcheck.Result{Head: head}, ghErr: errors.New("exit status 1"), response: `{"message":"Head branch was modified. Review and try the merge again."}`},
		"not merged":  {result: shipcheck.Result{Head: head}, response: `{"merged":false,"message":"Pull Request is not mergeable"}`},
		"no document": {result: shipcheck.Result{Head: head}, response: `not json`},
	} {
		t.Run(name, func(t *testing.T) {
			install(t, f)
			code, stdout, stderr := runWith(t, "--pr", "7", "--subject", "tools: add a merge command")
			if code == 0 || stdout != "" {
				t.Fatalf("exit %d, stdout %q; an unconfirmed merge must fail", code, stdout)
			}
			if !strings.Contains(stderr, "was not merged") {
				t.Errorf("stderr = %q", stderr)
			}
		})
	}
}

func TestTheMessageFileIsACommitMessage(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	f := &fakes{result: shipcheck.Result{Head: head}, response: `{"sha":"feedface","merged":true}`}
	install(t, f)
	good := write("good.txt", "tools: add a merge command (#7)\n\nFirst paragraph.\n\nNo-Issue: example\n")
	if code, _, stderr := runWith(t, "--pr", "7", "-F", good); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	call := f.calls[0]
	if !slices.Contains(call, "commit_title=tools: add a merge command (#7)") {
		t.Errorf("a subject already carrying its number was changed: %q", call)
	}
	if !slices.Contains(call, "commit_message=First paragraph.\n\nNo-Issue: example") {
		t.Errorf("the body was not passed whole: %q", call)
	}

	for name, text := range map[string]string{
		"no blank line": "tools: add a merge command\nBody without a gap.\n",
		"no subject":    "\n\nBody only.\n",
		"too large":     "tools: x\n\n" + strings.Repeat("a", maxMessageBytes),
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakes{result: shipcheck.Result{Head: head}}
			install(t, f)
			if code, _, _ := runWith(t, "--pr", "7", "-F", write(name, text)); code != 2 || f.checked {
				t.Fatalf("exit %d, checked %v; a malformed message file must stop the run", code, f.checked)
			}
		})
	}
}

func TestUsageNeedsOneMessageSource(t *testing.T) {
	for _, args := range [][]string{
		{"--pr", "7"},
		{"--subject", "tools: x"},
		{"--pr", "7", "--subject", "tools: x", "-F", "m.txt"},
		{"--pr", "7", "-F", "m.txt", "--body", "b"},
		{"--pr", "7", "--repo", "flowstate", "--subject", "tools: x"},
		{"--pr", "7", "--subject", "tools: x", "extra"},
	} {
		f := &fakes{}
		install(t, f)
		if code, _, _ := runWith(t, args...); code != 2 || f.checked {
			t.Errorf("args %q: exit %d, checked %v; want usage", args, code, f.checked)
		}
	}
}
