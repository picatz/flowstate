// Command shipcheck verifies the remote final-head evidence required before an
// autonomous Flowstate merge. It is read-only: passing this command never
// merges or enables auto-merge; tools/shipmerge does the merge after the same
// verdict passes.
//
// With --attest REVIEWER it prints, instead, the exact owner comment that
// attests an independent review of the pull request's current head, so the
// attestation is copied rather than composed. --head SHA, with --attest, first
// requires the pull request's head to be SHA: a reviewer's verdict names the
// head it reviewed, and an attestation of any other head does not count.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/picatz/flowstate/internal/shipcheck"
)

func main() {
	repo := flag.String("repo", "picatz/flowstate", "GitHub owner/repository")
	prNumber := flag.Int("pr", 0, "pull request number")
	reviewer := flag.String("attest", "", "print the attestation comment for this reviewer instead of checking")
	head := flag.String("head", "", "with --attest, the full head SHA the reviewer's verdict names")
	flag.Parse()
	if *prNumber <= 0 || !shipcheck.ValidRepo(*repo) || flag.NArg() != 0 || (*head != "" && *reviewer == "") {
		fmt.Fprintln(os.Stderr, "usage: go run ./tools/shipcheck --repo owner/repo --pr NUMBER [--attest REVIEWER [--head SHA]]")
		os.Exit(2)
	}
	if *reviewer != "" {
		os.Exit(attest(*repo, *prNumber, *reviewer, *head))
	}

	result, err := shipcheck.Check(*repo, *prNumber)
	if err != nil {
		fmt.Fprintln(os.Stderr, "shipcheck:", err)
		os.Exit(1)
	}
	if len(result.Problems) != 0 {
		for _, problem := range result.Problems {
			fmt.Fprintln(os.Stderr, "shipcheck:", problem)
		}
		os.Exit(1)
	}
	caveat := ""
	if result.REST {
		caveat = "; read over REST, which cannot report a review body edited after the attestation"
	}
	fmt.Printf("shipcheck: PASS PR %s#%d at %s; auto-merge disabled, every check terminal and acceptable, independent exact-head code/security review passed, zero unresolved review threads%s\n", *repo, *prNumber, result.Head, caveat)
}

// attest prints the attestation body for the pull request's current head on
// stdout, and how to post it on stderr, so the body can be redirected to a
// file whole.
func attest(repo string, number int, reviewer, want string) int {
	current, err := shipcheck.Head(repo, number)
	if err != nil {
		fmt.Fprintln(os.Stderr, "shipcheck:", err)
		return 1
	}
	if want != "" && want != current {
		fmt.Fprintf(os.Stderr, "shipcheck: the reviewed head %s is not the head of %s#%d, %s; review the current head before attesting\n", want, repo, number, current)
		return 1
	}
	body, err := shipcheck.Attestation(reviewer, current)
	if err != nil {
		fmt.Fprintln(os.Stderr, "shipcheck:", err)
		return 2
	}
	fmt.Print(body)
	fmt.Fprintf(os.Stderr, "shipcheck: post this unchanged as a new comment on %s#%d from the owner's identity, after every other comment, review and thread change, and never edit it\n", repo, number)
	return 0
}
