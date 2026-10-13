// Command shipcheck verifies the remote final-head evidence required before an
// autonomous Flowstate merge. It is read-only: passing this command never
// merges or enables auto-merge; tools/shipmerge does the merge after the same
// verdict passes.
//
// With --attest REVIEWER --head SHA it prints, instead, the exact owner
// comment that attests REVIEWER's verdict on SHA, so the attestation is
// copied rather than composed. --head is required and must be the pull
// request's current head: the attestation records a verdict, so it names the
// head the reviewer's verdict names, and an attestation of any other head does
// not count.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/picatz/flowstate/internal/shipcheck"
)

// check and head are the evidence check and the head lookup, replaced in
// tests.
var (
	check = shipcheck.Check
	head  = shipcheck.Head
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("shipcheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	repo := flags.String("repo", "picatz/flowstate", "GitHub owner/repository")
	prNumber := flags.Int("pr", 0, "pull request number")
	reviewer := flags.String("attest", "", "print the attestation comment for this reviewer instead of checking")
	reviewed := flags.String("head", "", "with --attest, the full head SHA the reviewer's verdict names (required)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *prNumber <= 0 || !shipcheck.ValidRepo(*repo) || flags.NArg() != 0 || (*reviewed == "") != (*reviewer == "") {
		fmt.Fprintln(stderr, "usage: go run ./tools/shipcheck --repo owner/repo --pr NUMBER [--attest REVIEWER --head SHA]")
		return 2
	}
	if *reviewer != "" {
		return attest(*repo, *prNumber, *reviewer, *reviewed, stdout, stderr)
	}

	result, err := check(*repo, *prNumber)
	if err != nil {
		fmt.Fprintln(stderr, "shipcheck:", err)
		return 1
	}
	if len(result.Problems) != 0 {
		for _, problem := range result.Problems {
			fmt.Fprintln(stderr, "shipcheck:", problem)
		}
		return 1
	}
	caveat := ""
	if result.REST {
		caveat = "; read over REST, which cannot report a review body edited after the attestation"
	}
	fmt.Fprintf(stdout, "shipcheck: PASS PR %s#%d at %s; auto-merge disabled, every check terminal and acceptable, independent exact-head code/security review passed, zero unresolved review threads%s\n", *repo, *prNumber, result.Head, caveat)
	return 0
}

// attest prints the attestation body for the reviewed head on stdout, once
// that head is the pull request's current one, and how to post it on stderr,
// so the body can be redirected to a file whole.
func attest(repo string, number int, reviewer, reviewed string, stdout, stderr io.Writer) int {
	current, err := head(repo, number)
	if err != nil {
		fmt.Fprintln(stderr, "shipcheck:", err)
		return 1
	}
	if reviewed != current {
		fmt.Fprintf(stderr, "shipcheck: the reviewed head %s is not the head of %s#%d, %s; review the current head before attesting\n", reviewed, repo, number, current)
		return 1
	}
	body, err := shipcheck.Attestation(reviewer, current)
	if err != nil {
		fmt.Fprintln(stderr, "shipcheck:", err)
		return 2
	}
	fmt.Fprint(stdout, body)
	fmt.Fprintf(stderr, "shipcheck: post this unchanged as a new comment on %s#%d from the owner's identity, after every other comment, review and thread change, and never edit it\n", repo, number)
	return 0
}
