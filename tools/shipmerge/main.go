// Command shipmerge squash-merges a Flowstate pull request once the shipping
// evidence passes, pinned to the head that evidence was read for.
//
//	go run ./tools/shipmerge --pr NUMBER -F message.txt
//	go run ./tools/shipmerge --pr NUMBER --subject 'scope: do the thing' [--body 'why']
//
// It runs the check `go run ./tools/shipcheck` runs, in this process, and
// merges only when that check passes, so no pipe or shell status stands
// between the verdict and the merge. The merge is GitHub's REST squash with
// `sha` set to the head the check read: a push after the check makes GitHub
// refuse the merge rather than land a head nobody attested. REST is the
// transport a Claude Code session can reach, where `gh pr merge` (GraphQL)
// is refused, so this is one route for every host.
//
// The message file is a commit message: the subject on the first line, a
// blank line, then the body. It is held to tools/commitcheck's commit
// conventions before anything else runs, and " (#NUMBER)" is appended to a
// subject that does not end with it, as GitHub's default squash subject does.
//
// Merging is an action, not a check: unlike shipcheck, this command is not on
// the Claude Code allow list, so each run is the operator's decision.
package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/picatz/flowstate/internal/commitcheck"
	"github.com/picatz/flowstate/internal/shipcheck"
)

// maxMessageBytes bounds the message file read; a squash message is a few
// paragraphs, and GitHub's own limit is far below anything that would need
// more.
const maxMessageBytes = 64 << 10

// check and gh are the evidence check and the GitHub CLI, replaced in tests.
var (
	check = shipcheck.Check
	gh    = shipcheck.GH
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("shipmerge", flag.ContinueOnError)
	flags.SetOutput(stderr)
	repo := flags.String("repo", "picatz/flowstate", "GitHub owner/repository")
	number := flags.Int("pr", 0, "pull request number")
	file := flags.String("F", "", "file holding the squash message: subject, blank line, body")
	subject := flags.String("subject", "", "squash subject, instead of -F")
	body := flags.String("body", "", "squash body, with --subject")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *number <= 0 || !shipcheck.ValidRepo(*repo) || flags.NArg() != 0 ||
		(*file == "") == (*subject == "") || (*file != "" && *body != "") {
		fmt.Fprintln(stderr, "usage: go run ./tools/shipmerge [--repo owner/repo] --pr NUMBER (-F FILE | --subject TEXT [--body TEXT])")
		return 2
	}

	title, message := *subject, *body
	if *file != "" {
		var err error
		if title, message, err = readMessage(*file); err != nil {
			fmt.Fprintln(stderr, "shipmerge:", err)
			return 2
		}
	}
	title = withNumber(strings.TrimSpace(title), *number)
	message = strings.TrimSpace(message)
	if findings := commitcheck.Check(title, message, commitcheck.SurfaceCommit); len(findings) != 0 {
		for _, f := range findings {
			fmt.Fprintln(stderr, "shipmerge: squash message:", f)
		}
		return 1
	}

	result, err := check(*repo, *number)
	if err != nil {
		fmt.Fprintln(stderr, "shipmerge: shipcheck did not complete, nothing merged:", err)
		return 1
	}
	if len(result.Problems) != 0 {
		for _, problem := range result.Problems {
			fmt.Fprintln(stderr, "shipcheck:", problem)
		}
		fmt.Fprintln(stderr, "shipmerge: shipcheck did not pass, nothing merged")
		return 1
	}

	merged, err := merge(*repo, *number, result.Head, title, message)
	if err != nil {
		fmt.Fprintf(stderr, "shipmerge: %s#%d at %s was not merged: %v\n", *repo, *number, result.Head, err)
		return 1
	}
	fmt.Fprintf(stdout, "shipmerge: merged %s#%d at %s as %s; fetch origin/main and confirm it is reachable\n", *repo, *number, result.Head, merged)
	return 0
}

// readMessage splits a commit-message file into its subject and body. A
// second line that is not blank is refused rather than folded into either:
// git would read it as part of the subject paragraph, GitHub would not.
func readMessage(path string) (subject, body string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxMessageBytes+1))
	if err != nil {
		return "", "", err
	}
	if len(data) > maxMessageBytes {
		return "", "", fmt.Errorf("%s is larger than %d bytes", path, maxMessageBytes)
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	subject, rest, _ := strings.Cut(text, "\n")
	if strings.TrimSpace(subject) == "" {
		return "", "", fmt.Errorf("%s has no subject on its first line", path)
	}
	if second, _, _ := strings.Cut(rest, "\n"); strings.TrimSpace(second) != "" {
		return "", "", fmt.Errorf("%s: the line after the subject must be blank", path)
	}
	return subject, rest, nil
}

// withNumber appends " (#number)" to subject unless it already ends so.
func withNumber(subject string, number int) string {
	suffix := fmt.Sprintf(" (#%d)", number)
	if strings.HasSuffix(subject, suffix) {
		return subject
	}
	return subject + suffix
}

// merge squash-merges repo#number pinned to head and returns the merge
// commit's SHA. GitHub answers a moved head with an error, which is returned
// as is: the right response is to check the new head, not to retry.
func merge(repo string, number int, head, title, message string) (string, error) {
	out, err := gh("api", "--method", "PUT", fmt.Sprintf("repos/%s/pulls/%d/merge", repo, number),
		"-f", "merge_method=squash", "-f", "sha="+head,
		"-f", "commit_title="+title, "-f", "commit_message="+message)
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, shipcheck.ErrorSnippet(out))
	}
	var response struct {
		SHA     string `json:"sha"`
		Merged  bool   `json:"merged"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(out, &response); err != nil {
		return "", fmt.Errorf("decode merge response: %w", err)
	}
	if !response.Merged || response.SHA == "" {
		return "", errors.New("GitHub did not report a merge: " + cmp.Or(response.Message, "no message"))
	}
	return response.SHA, nil
}
