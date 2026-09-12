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

	"github.com/picatz/flowstate/internal/textbound"
)

const (
	maxThreadPages    = 20
	errorSnippetLimit = 4 << 10
)

type requiredCheck struct {
	workflow string
	name     string
}

var requiredChecks = []requiredCheck{
	{"CodeQL", "Analyze Go"},
	{"CI", "plan"},
	{"CI", "verdict"},
	{"Commit conventions", "commitcheck"},
	{"Dependency review", "Review dependency changes"},
}

var (
	ghCommandTimeout = 30 * time.Second
	ghWaitDelay      = time.Second
	ghOutputLimit    = 16 << 20
)

const ghErrorOutputLimit = 64 << 10

var errGHOutputLimit = errors.New("gh output exceeded the byte limit")

type boundedBuffer struct {
	buffer   bytes.Buffer
	limit    int
	exceeded bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := b.limit - b.buffer.Len()
	if remaining < len(p) {
		b.exceeded = true
		if remaining <= 0 {
			return n, nil
		}
		p = p[:remaining]
	}
	_, _ = b.buffer.Write(p)
	return n, nil
}

func (b *boundedBuffer) Bytes() []byte  { return b.buffer.Bytes() }
func (b *boundedBuffer) String() string { return b.buffer.String() }

type pullRequest struct {
	State                        string        `json:"state"`
	BaseRefName                  string        `json:"baseRefName"`
	IsDraft                      bool          `json:"isDraft"`
	HeadRefOID                   string        `json:"headRefOid"`
	AutoMergeRequest             presentJSON   `json:"autoMergeRequest"`
	ReviewDecision               string        `json:"reviewDecision"`
	StatusChecks                 []statusCheck `json:"statusCheckRollup"`
	Reviews                      []review      `json:"reviews"`
	Comments                     []comment     `json:"comments"`
	Files                        []changedFile `json:"files"`
	ChangedFiles                 int           `json:"changedFiles"`
	LatestReviewCommentUpdatedAt string
}

type changedFile struct {
	Path string `json:"path"`
}

type presentJSON struct {
	Present bool
	Value   json.RawMessage
}

func (p *presentJSON) UnmarshalJSON(data []byte) error {
	p.Present = true
	p.Value = append(p.Value[:0], data...)
	return nil
}

type statusCheck struct {
	Type        string `json:"__typename"`
	Name        string `json:"name"`
	Context     string `json:"context"`
	Workflow    string `json:"workflowName"`
	Status      string `json:"status"`
	Conclusion  string `json:"conclusion"`
	State       string `json:"state"`
	StartedAt   string `json:"startedAt"`
	CreatedAt   string `json:"createdAt"`
	CompletedAt string `json:"completedAt"`
}

type actor struct {
	Login string `json:"login"`
}

type review struct {
	SubmittedAt string `json:"submittedAt"`
	UpdatedAt   string `json:"updatedAt"`
}

type comment struct {
	Author            actor  `json:"author"`
	AuthorAssociation string `json:"authorAssociation"`
	Body              string `json:"body"`
	CreatedAt         string `json:"createdAt"`
	UpdatedAt         string `json:"updatedAt"`
	URL               string `json:"url"`
}

type independentReview struct {
	HeadSHA  string `json:"headSha"`
	Reviewer string `json:"reviewer"`
	Scope    string `json:"scope"`
	Status   string `json:"status"`
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
	caveat := ""
	if fallbackNoted {
		caveat = "; read over REST, which cannot report a review body edited after the attestation"
	}
	fmt.Printf("shipcheck: PASS PR %s#%d at %s; auto-merge disabled, every check terminal and acceptable, independent exact-head code/security review passed, zero unresolved review threads%s\n", *repo, *prNumber, pr.HeadRefOID, caveat)
}

func validRepo(repo string) bool {
	parts := strings.Split(repo, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}

func loadPullRequest(repo string, number int) (pullRequest, error) {
	pr, err := loadPullRequestSummary(repo, number)
	if err != nil {
		return pullRequest{}, err
	}
	comments, err := loadComments(repo, number)
	if err != nil {
		return pullRequest{}, err
	}
	pr.Comments = comments
	reviews, err := loadReviews(repo, number)
	if err != nil {
		return pullRequest{}, err
	}
	pr.Reviews = reviews
	latestReviewComment, err := loadLatestReviewCommentUpdate(repo, number)
	if err != nil {
		return pullRequest{}, err
	}
	pr.LatestReviewCommentUpdatedAt = latestReviewComment
	return pr, nil
}

// loadPullRequestGraphQL reads the pull request's state, head, auto-merge
// request, review decision, check rollup and files through `gh pr view`,
// which is GraphQL underneath; rest.go holds the REST spelling.
func loadPullRequestGraphQL(repo string, number int) (pullRequest, error) {
	fields := "state,baseRefName,isDraft,headRefOid,autoMergeRequest,reviewDecision,statusCheckRollup,files,changedFiles"
	out, err := runGH("pr", "view", strconv.Itoa(number), "--repo", repo, "--json", fields)
	if err != nil {
		return pullRequest{}, fmt.Errorf("query pull request: %w: %s", err, errorSnippet(out))
	}
	var pr pullRequest
	if err := json.Unmarshal(out, &pr); err != nil {
		return pullRequest{}, fmt.Errorf("decode pull request: %w", err)
	}
	return pr, nil
}

func loadLatestReviewCommentUpdate(repo string, number int) (string, error) {
	endpoint := fmt.Sprintf("repos/%s/pulls/%d/comments", repo, number)
	out, err := runGH("api", "--method", "GET", endpoint, "-f", "sort=updated", "-f", "direction=desc", "-F", "per_page=1")
	if err != nil {
		return "", fmt.Errorf("query latest inline review comment: %w: %s", err, errorSnippet(out))
	}
	var comments []struct {
		UpdatedAt string `json:"updated_at"`
	}
	if err := json.Unmarshal(out, &comments); err != nil {
		return "", fmt.Errorf("decode latest inline review comment: %w", err)
	}
	// A JSON null decodes into a nil slice without error; it is a missing
	// list, not proof that no inline comment was edited.
	if comments == nil {
		return "", fmt.Errorf("decode latest inline review comment: the list is null, not an array")
	}
	if len(comments) == 0 {
		return "", nil
	}
	return comments[0].UpdatedAt, nil
}

const commentsQuery = `query($owner:String!,$repo:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$repo){pullRequest(number:$number){comments(first:100,after:$cursor){pageInfo{hasNextPage endCursor} nodes{author{login} authorAssociation body createdAt updatedAt url}}}}}`

func loadCommentsGraphQL(repo string, number int) ([]comment, error) {
	parts := strings.Split(repo, "/")
	var comments []comment
	var cursor string
	for page := 0; page < maxThreadPages; page++ {
		args := []string{"api", "graphql", "-f", "query=" + commentsQuery, "-F", "owner=" + parts[0], "-F", "repo=" + parts[1], "-F", "number=" + strconv.Itoa(number)}
		if cursor != "" {
			args = append(args, "-f", "cursor="+cursor)
		}
		out, err := runGH(args...)
		if err != nil {
			return nil, fmt.Errorf("query pull-request comments: %w: %s", err, errorSnippet(out))
		}
		var response struct {
			Data struct {
				Repository struct {
					PullRequest struct {
						Comments struct {
							PageInfo struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
							Nodes []comment `json:"nodes"`
						} `json:"comments"`
					} `json:"pullRequest"`
				} `json:"repository"`
			} `json:"data"`
		}
		if err := json.Unmarshal(out, &response); err != nil {
			return nil, fmt.Errorf("decode pull-request comments: %w", err)
		}
		pageData := response.Data.Repository.PullRequest.Comments
		comments = append(comments, pageData.Nodes...)
		if !pageData.PageInfo.HasNextPage {
			return comments, nil
		}
		if pageData.PageInfo.EndCursor == "" {
			return nil, errors.New("pull-request comment pagination has no end cursor")
		}
		cursor = pageData.PageInfo.EndCursor
	}
	return nil, fmt.Errorf("pull-request comment query exceeded %d pages", maxThreadPages)
}

const reviewsQuery = `query($owner:String!,$repo:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$repo){pullRequest(number:$number){reviews(first:100,after:$cursor){pageInfo{hasNextPage endCursor} nodes{submittedAt updatedAt}}}}}`

func loadReviewsGraphQL(repo string, number int) ([]review, error) {
	parts := strings.Split(repo, "/")
	var reviews []review
	var cursor string
	for page := 0; page < maxThreadPages; page++ {
		args := []string{"api", "graphql", "-f", "query=" + reviewsQuery, "-F", "owner=" + parts[0], "-F", "repo=" + parts[1], "-F", "number=" + strconv.Itoa(number)}
		if cursor != "" {
			args = append(args, "-f", "cursor="+cursor)
		}
		out, err := runGH(args...)
		if err != nil {
			return nil, fmt.Errorf("query pull-request reviews: %w: %s", err, errorSnippet(out))
		}
		var response struct {
			Data struct {
				Repository struct {
					PullRequest struct {
						Reviews struct {
							PageInfo struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
							Nodes []review `json:"nodes"`
						} `json:"reviews"`
					} `json:"pullRequest"`
				} `json:"repository"`
			} `json:"data"`
		}
		if err := json.Unmarshal(out, &response); err != nil {
			return nil, fmt.Errorf("decode pull-request reviews: %w", err)
		}
		pageData := response.Data.Repository.PullRequest.Reviews
		reviews = append(reviews, pageData.Nodes...)
		if !pageData.PageInfo.HasNextPage {
			return reviews, nil
		}
		if pageData.PageInfo.EndCursor == "" {
			return nil, errors.New("pull-request review pagination has no end cursor")
		}
		cursor = pageData.PageInfo.EndCursor
	}
	return nil, fmt.Errorf("pull-request review query exceeded %d pages", maxThreadPages)
}

func runGH(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ghCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", args...)
	// A credential helper or child process may inherit an output pipe
	// after gh is killed. WaitDelay closes that pipe rather than waiting on an
	// uncooperative descendant forever.
	cmd.WaitDelay = ghWaitDelay
	stdout := &boundedBuffer{limit: ghOutputLimit - ghErrorOutputLimit}
	stderr := &boundedBuffer{limit: ghErrorOutputLimit}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if stdout.exceeded || stderr.exceeded {
		return append(stdout.Bytes(), stderr.Bytes()...), errGHOutputLimit
	}
	if err != nil {
		return append(stdout.Bytes(), stderr.Bytes()...), err
	}
	return stdout.Bytes(), nil
}

func errorSnippet(out []byte) string {
	truncated := len(out) > errorSnippetLimit
	if truncated {
		out = out[:errorSnippetLimit]
	}
	snippet := strings.TrimSpace(textbound.Cut(string(out), errorSnippetLimit))
	if truncated {
		snippet += "..."
	}
	return snippet
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
	if !pr.AutoMergeRequest.Present {
		problems = append(problems, "auto-merge state is missing")
	} else if raw := bytes.TrimSpace(pr.AutoMergeRequest.Value); !bytes.Equal(raw, []byte("null")) {
		problems = append(problems, "auto-merge is enabled; disable it before shipping")
	}
	if pr.ReviewDecision != "" && pr.ReviewDecision != "APPROVED" {
		problems = append(problems, fmt.Sprintf("review decision is %q", pr.ReviewDecision))
	}
	if len(pr.StatusChecks) == 0 {
		problems = append(problems, "no check runs or status contexts were reported")
	}
	if pr.ChangedFiles != len(pr.Files) {
		problems = append(problems, fmt.Sprintf("changed-file evidence is incomplete: GitHub reports %d but returned %d", pr.ChangedFiles, len(pr.Files)))
	}
	problems = append(problems, checkProblems(pr.StatusChecks, requiredChecksFor(pr.Files))...)

	if !hasIndependentReview(pr) {
		problems = append(problems, "independent code/security review has not passed on the exact final head")
	}
	if requests := ownerCodexRequests(pr); requests > 1 {
		problems = append(problems, fmt.Sprintf("Codex was requested %d times on this pull request; request an optional provider at most once", requests))
	}
	if unresolved != 0 {
		problems = append(problems, fmt.Sprintf("%d review thread(s) remain unresolved", unresolved))
	}
	return problems
}

func ownerCodexRequests(pr pullRequest) int {
	requests := 0
	for _, comment := range pr.Comments {
		if comment.AuthorAssociation != "OWNER" {
			continue
		}
		for _, line := range strings.Split(strings.ToLower(comment.Body), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "@codex review") || strings.HasPrefix(line, "@codex security review") {
				requests++
				break
			}
		}
	}
	return requests
}

func requiredChecksFor(files []changedFile) []requiredCheck {
	required := append([]requiredCheck(nil), requiredChecks...)
	for _, file := range files {
		if file.Path == "docs/EDITORS.md" || file.Path == ".github/workflows/editors.yml" ||
			strings.HasPrefix(file.Path, "tools/editorsmoke/") || strings.HasPrefix(file.Path, "pkg/flowstate/v1/flowfile/") ||
			strings.HasPrefix(file.Path, "cmd/flow/") || strings.HasPrefix(file.Path, "editors/vscode/") {
			return append(required,
				requiredCheck{"Editors", "Neovim LSP smoke"},
				requiredCheck{"Editors", "VS Code extension"},
			)
		}
	}
	return required
}

func checkProblems(checks []statusCheck, requiredChecks []requiredCheck) []string {
	groups := make(map[string][]statusCheck, len(checks))
	reported := make(map[string][]statusCheck, len(checks))
	var order []string
	for _, check := range checks {
		name := check.Name
		if name == "" {
			name = check.Context
		}
		key := check.Type + "\x00" + check.Workflow + "\x00" + name
		identity := check.Workflow + "\x00" + name
		reported[identity] = append(reported[identity], check)
		if len(groups[key]) == 0 {
			order = append(order, key)
		}
		groups[key] = append(groups[key], check)
	}
	var problems []string
	for _, required := range requiredChecks {
		results := reported[required.workflow+"\x00"+required.name]
		if len(results) == 0 {
			problems = append(problems, fmt.Sprintf("required check %q from workflow %q was not reported", required.name, required.workflow))
			continue
		}
		latest := latestStatusCheck(results)
		if latest.Type != "CheckRun" || latest.Status != "COMPLETED" || latest.Conclusion != "SUCCESS" {
			problems = append(problems, fmt.Sprintf("required check %q from workflow %q latest result is %s/%s", required.name, required.workflow, latest.Status, latest.Conclusion))
		}
	}
	for _, key := range order {
		group := groups[key]
		name := strings.SplitN(key, "\x00", 3)[2]
		if len(group) > 1 {
			for _, check := range group {
				if checkTimestamp(check) == "" {
					problems = append(problems, fmt.Sprintf("check %q has duplicate results without enough timestamp evidence to order them", name))
					break
				}
			}
			if conflictingLatestChecks(group) {
				problems = append(problems, fmt.Sprintf("check %q has conflicting results at the latest timestamp", name))
			}
		}
		for _, check := range group {
			if check.Type == "CheckRun" && check.Status != "COMPLETED" {
				problems = append(problems, fmt.Sprintf("check %q is %s/%s", name, check.Status, check.Conclusion))
			}
			if check.Type == "StatusContext" && check.State != "SUCCESS" && check.State != "FAILURE" && check.State != "ERROR" {
				problems = append(problems, fmt.Sprintf("status %q is %s", name, check.State))
			}
		}
		latest := latestStatusCheck(group)
		switch latest.Type {
		case "CheckRun":
			if latest.Status != "COMPLETED" || !acceptableConclusion(latest.Conclusion) {
				problems = append(problems, fmt.Sprintf("check %q latest result is %s/%s", name, latest.Status, latest.Conclusion))
			}
		case "StatusContext":
			if latest.State != "SUCCESS" {
				problems = append(problems, fmt.Sprintf("status %q latest result is %s", name, latest.State))
			}
		default:
			problems = append(problems, fmt.Sprintf("check %q has unsupported type %q", name, latest.Type))
		}
	}
	return problems
}

func conflictingLatestChecks(checks []statusCheck) bool {
	latestTimestamp := ""
	for _, check := range checks {
		if timestamp := checkTimestamp(check); timestamp > latestTimestamp {
			latestTimestamp = timestamp
		}
	}
	latestResult := ""
	for _, check := range checks {
		if checkTimestamp(check) != latestTimestamp {
			continue
		}
		result := check.Type + "\x00" + check.Status + "\x00" + check.Conclusion + "\x00" + check.State
		if latestResult != "" && latestResult != result {
			return true
		}
		latestResult = result
	}
	return false
}

func latestStatusCheck(checks []statusCheck) statusCheck {
	latest := checks[0]
	for _, check := range checks[1:] {
		if checkTimestamp(check) >= checkTimestamp(latest) {
			latest = check
		}
	}
	return latest
}

func checkTimestamp(check statusCheck) string {
	if check.StartedAt != "" {
		return check.StartedAt
	}
	if check.CreatedAt != "" {
		return check.CreatedAt
	}
	return check.CompletedAt
}

func acceptableConclusion(conclusion string) bool {
	switch conclusion {
	case "SUCCESS", "SKIPPED", "NEUTRAL":
		return true
	default:
		return false
	}
}

const independentReviewMarker = "<!-- flowstate-independent-review:v1 "

func hasIndependentReview(pr pullRequest) bool {
	for _, comment := range pr.Comments {
		if comment.AuthorAssociation != "OWNER" {
			continue
		}
		for _, line := range strings.Split(comment.Body, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, independentReviewMarker) || !strings.HasSuffix(line, " -->") {
				continue
			}
			var evidence independentReview
			raw := strings.TrimSuffix(strings.TrimPrefix(line, independentReviewMarker), " -->")
			if json.Unmarshal([]byte(raw), &evidence) != nil || evidence.HeadSHA != pr.HeadRefOID ||
				evidence.Reviewer == "" || evidence.Scope != "code-security" || evidence.Status != "pass" {
				continue
			}
			body := strings.ToLower(comment.Body)
			if !strings.Contains(body, "pass") || !strings.Contains(body, "no actionable") {
				continue
			}
			if comment.UpdatedAt > comment.CreatedAt {
				continue
			}
			if pr.LatestReviewCommentUpdatedAt != "" && pr.LatestReviewCommentUpdatedAt >= comment.CreatedAt {
				continue
			}
			stale := false
			for _, other := range pr.Comments {
				if other.URL != comment.URL && (other.CreatedAt >= comment.CreatedAt || other.UpdatedAt >= comment.CreatedAt) {
					stale = true
					break
				}
			}
			for _, review := range pr.Reviews {
				if review.SubmittedAt >= comment.CreatedAt || review.UpdatedAt >= comment.CreatedAt {
					stale = true
					break
				}
			}
			if !stale {
				return true
			}
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

func unresolvedReviewThreadsGraphQL(repo string, number int) (int, error) {
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
			return 0, fmt.Errorf("query GraphQL: %w: %s", err, errorSnippet(out))
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
