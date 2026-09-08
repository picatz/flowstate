// Command shipcheck verifies the remote final-head evidence required before an
// autonomous Flowstate merge. It is intentionally read-only: passing this
// command never merges or enables auto-merge.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	maxThreadPages = 20
)

var (
	ghCommandTimeout = 30 * time.Second
	ghWaitDelay      = time.Second
)

type pullRequest struct {
	State            string          `json:"state"`
	BaseRefName      string          `json:"baseRefName"`
	IsDraft          bool            `json:"isDraft"`
	HeadRefOID       string          `json:"headRefOid"`
	AutoMergeRequest json.RawMessage `json:"autoMergeRequest"`
	StatusChecks     []statusCheck   `json:"statusCheckRollup"`
	Reviews          []review        `json:"reviews"`
	Comments         []comment       `json:"comments"`
}

type statusCheck struct {
	Type       string `json:"__typename"`
	Name       string `json:"name"`
	Context    string `json:"context"`
	Workflow   string `json:"workflowName"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	State      string `json:"state"`
}

type actor struct {
	Login string `json:"login"`
}

type commit struct {
	OID string `json:"oid"`
}

type review struct {
	Author actor   `json:"author"`
	Commit *commit `json:"commit"`
	Body   string  `json:"body"`
}

type comment struct {
	Author actor  `json:"author"`
	Body   string `json:"body"`
}

func main() {
	repo := flag.String("repo", "picatz/flowstate", "GitHub owner/repository")
	prNumber := flag.Int("pr", 0, "pull request number")
	flag.Parse()
	if *prNumber <= 0 || !validRepo(*repo) {
		fmt.Fprintln(os.Stderr, "usage: go run ./tools/shipcheck --repo owner/repo --pr NUMBER")
		os.Exit(2)
	}

	pr, err := loadPullRequest(*repo, *prNumber)
	if err != nil {
		fmt.Fprintln(os.Stderr, "shipcheck:", err)
		os.Exit(1)
	}
	unresolved, err := unresolvedReviewThreads(*repo, *prNumber)
	if err != nil {
		fmt.Fprintln(os.Stderr, "shipcheck: review-thread check did not complete:", err)
		os.Exit(1)
	}
	problems := evaluate(pr, unresolved)
	if len(problems) != 0 {
		for _, problem := range problems {
			fmt.Fprintln(os.Stderr, "shipcheck:", problem)
		}
		os.Exit(1)
	}
	fmt.Printf("shipcheck: PASS PR %s#%d at %s; auto-merge disabled, every check terminal and acceptable, Codex code/security and Copilot reviewed the final head, zero unresolved review threads\n", *repo, *prNumber, pr.HeadRefOID)
}

func validRepo(repo string) bool {
	parts := strings.Split(repo, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}

func loadPullRequest(repo string, number int) (pullRequest, error) {
	fields := "state,baseRefName,isDraft,headRefOid,autoMergeRequest,statusCheckRollup,reviews,comments"
	out, err := runGH("pr", "view", strconv.Itoa(number), "--repo", repo, "--json", fields)
	if err != nil {
		return pullRequest{}, fmt.Errorf("query pull request: %w: %s", err, strings.TrimSpace(string(out)))
	}
	var pr pullRequest
	if err := json.Unmarshal(out, &pr); err != nil {
		return pullRequest{}, fmt.Errorf("decode pull request: %w", err)
	}
	return pr, nil
}

func runGH(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ghCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", args...)
	// A credential helper or child process may inherit CombinedOutput's pipe
	// after gh is killed. WaitDelay closes that pipe rather than waiting on an
	// uncooperative descendant forever.
	cmd.WaitDelay = ghWaitDelay
	return cmd.CombinedOutput()
}

func evaluate(pr pullRequest, unresolved int) []string {
	var problems []string
	if pr.State != "OPEN" {
		problems = append(problems, fmt.Sprintf("pull request state is %q, want OPEN", pr.State))
	}
	if pr.BaseRefName != "main" {
		problems = append(problems, fmt.Sprintf("pull request base is %q, want main", pr.BaseRefName))
	}
	if pr.IsDraft {
		problems = append(problems, "pull request is still a draft")
	}
	if pr.HeadRefOID == "" {
		problems = append(problems, "pull request has no head commit")
	}
	if raw := bytes.TrimSpace(pr.AutoMergeRequest); len(raw) != 0 && !bytes.Equal(raw, []byte("null")) {
		problems = append(problems, "auto-merge is enabled; disable it before shipping")
	}
	if len(pr.StatusChecks) == 0 {
		problems = append(problems, "no check runs or status contexts were reported")
	}
	problems = append(problems, checkProblems(pr.StatusChecks)...)

	if !hasExactHeadReview(pr.Reviews, pr.HeadRefOID, "copilot-pull-request-reviewer") {
		problems = append(problems, "Copilot has not reviewed the exact final head")
	} else if copilotExactHeadHasFindings(pr.Reviews, pr.HeadRefOID) {
		problems = append(problems, "Copilot exact-final-head review still contains findings")
	}
	if !hasCodexReview(pr, false) {
		problems = append(problems, "Codex code review has not completed on the exact final head")
	}
	if !hasCodexReview(pr, true) {
		problems = append(problems, "Codex security review has not completed on the exact final head")
	}
	if unresolved != 0 {
		problems = append(problems, fmt.Sprintf("%d review thread(s) remain unresolved", unresolved))
	}
	return problems
}

func checkProblems(checks []statusCheck) []string {
	groups := make(map[string][]statusCheck, len(checks))
	var order []string
	for _, check := range checks {
		name := check.Name
		if name == "" {
			name = check.Context
		}
		key := check.Type + "\x00" + check.Workflow + "\x00" + name
		if len(groups[key]) == 0 {
			order = append(order, key)
		}
		groups[key] = append(groups[key], check)
	}
	var problems []string
	for _, key := range order {
		group := groups[key]
		name := strings.SplitN(key, "\x00", 3)[2]
		acceptable := false
		for _, check := range group {
			switch check.Type {
			case "CheckRun":
				if check.Status != "COMPLETED" {
					problems = append(problems, fmt.Sprintf("check %q is %s/%s", name, check.Status, check.Conclusion))
				}
				acceptable = acceptable || check.Status == "COMPLETED" && acceptableConclusion(check.Conclusion)
			case "StatusContext":
				if check.State != "SUCCESS" && check.State != "FAILURE" && check.State != "ERROR" {
					problems = append(problems, fmt.Sprintf("status %q is %s", name, check.State))
				}
				acceptable = acceptable || check.State == "SUCCESS"
			default:
				problems = append(problems, fmt.Sprintf("check %q has unsupported type %q", name, check.Type))
			}
		}
		if !acceptable {
			last := group[len(group)-1]
			if last.Type == "StatusContext" {
				problems = append(problems, fmt.Sprintf("status %q has no successful result (latest %s)", name, last.State))
			} else {
				problems = append(problems, fmt.Sprintf("check %q has no acceptable completed result (latest %s/%s)", name, last.Status, last.Conclusion))
			}
		}
	}
	return problems
}

func copilotExactHeadHasFindings(reviews []review, head string) bool {
	for _, review := range reviews {
		if review.Author.Login != "copilot-pull-request-reviewer" || review.Commit == nil || review.Commit.OID != head {
			continue
		}
		return strings.Contains(review.Body, "Changes recommended") || strings.Contains(review.Body, "Suppressed comments (")
	}
	return false
}

func acceptableConclusion(conclusion string) bool {
	switch conclusion {
	case "SUCCESS", "SKIPPED", "NEUTRAL":
		return true
	default:
		return false
	}
}

func hasExactHeadReview(reviews []review, head, login string) bool {
	for _, review := range reviews {
		if review.Author.Login == login && review.Commit != nil && review.Commit.OID == head {
			return true
		}
	}
	return false
}

func hasCodexReview(pr pullRequest, security bool) bool {
	for _, review := range pr.Reviews {
		if !security && review.Author.Login == "chatgpt-codex-connector" && review.Commit != nil && review.Commit.OID == pr.HeadRefOID {
			return true
		}
	}
	for _, comment := range pr.Comments {
		if comment.Author.Login != "chatgpt-codex-connector" {
			continue
		}
		if security && completedSecuritySummary(comment.Body, pr.HeadRefOID) {
			return true
		}
		if !mentionsCommit(comment.Body, pr.HeadRefOID) {
			continue
		}
		isSecurity := strings.Contains(comment.Body, "Security review completed")
		if security == isSecurity && (isSecurity || strings.Contains(comment.Body, "Codex Review")) {
			return true
		}
	}
	return false
}

func completedSecuritySummary(body, head string) bool {
	return strings.Contains(body, "codex-security-review:v1") &&
		strings.Contains(body, `"headSha":"`+head+`"`) &&
		strings.Contains(body, `"status":"completed"`)
}

func mentionsCommit(body, head string) bool {
	for n := 7; n <= len(head); n++ {
		if strings.Contains(body, "`"+head[:n]+"`") {
			return true
		}
	}
	return false
}

type threadPage struct {
	Data struct {
		Repository struct {
			PullRequest *struct {
				ReviewThreads struct {
					Nodes []struct {
						IsResolved bool `json:"isResolved"`
					} `json:"nodes"`
					PageInfo struct {
						HasNextPage bool   `json:"hasNextPage"`
						EndCursor   string `json:"endCursor"`
					} `json:"pageInfo"`
				} `json:"reviewThreads"`
			} `json:"pullRequest"`
		} `json:"repository"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func unresolvedReviewThreads(repo string, number int) (int, error) {
	owner, name, _ := strings.Cut(repo, "/")
	const query = `query($owner:String!,$repo:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$repo){pullRequest(number:$number){reviewThreads(first:100,after:$cursor){nodes{isResolved}pageInfo{hasNextPage endCursor}}}}}`
	cursor := ""
	unresolved := 0
	for page := 0; page < maxThreadPages; page++ {
		args := []string{"api", "graphql", "-f", "query=" + query, "-F", "owner=" + owner, "-F", "repo=" + name, "-F", "number=" + strconv.Itoa(number)}
		if cursor != "" {
			args = append(args, "-F", "cursor="+cursor)
		}
		out, err := runGH(args...)
		if err != nil {
			return 0, fmt.Errorf("query GraphQL: %w: %s", err, strings.TrimSpace(string(out)))
		}
		var response threadPage
		if err := json.Unmarshal(out, &response); err != nil {
			return 0, fmt.Errorf("decode GraphQL response: %w", err)
		}
		if len(response.Errors) != 0 {
			return 0, errors.New(response.Errors[0].Message)
		}
		if response.Data.Repository.PullRequest == nil {
			return 0, errors.New("pull request was not found")
		}
		threads := response.Data.Repository.PullRequest.ReviewThreads
		for _, thread := range threads.Nodes {
			if !thread.IsResolved {
				unresolved++
			}
		}
		if !threads.PageInfo.HasNextPage {
			return unresolved, nil
		}
		if threads.PageInfo.EndCursor == "" {
			return 0, errors.New("review-thread pagination has no end cursor")
		}
		cursor = threads.PageInfo.EndCursor
	}
	return 0, fmt.Errorf("review-thread query exceeded %d pages", maxThreadPages)
}
