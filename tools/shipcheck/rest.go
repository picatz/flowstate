package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
)

// GraphQL is not reachable from every environment that runs this command.
// A Claude Code session reaches GitHub through a proxy that answers every
// GraphQL request with 403 and serves REST only, and `gh pr view --json` is
// GraphQL underneath. Each loader in main.go therefore has a REST spelling
// here, and the wrappers below ask GraphQL first and REST second, erroring
// only when both fail: the fallback adds a way for a check to run without
// adding a way for a failed check to read as clean.
//
// Two facts REST spells differently are handled explicitly rather than
// zero-filled. GitHub's computed reviewDecision is replaced by the decision
// each reviewer's latest decisive review implies, checked against the base
// branch's rulesets and classic branch protection so a required-approval
// rule still reads as REVIEW_REQUIRED. A review's updatedAt is not exposed by REST at all, so an
// edited review body after the attestation is not detected on this path;
// noteFallback says so, so the operator reads the reviews once more before
// merging instead of trusting a check that could not look.

// restPageSize is the page size every paged REST walk asks for; a page
// shorter than this ends the walk.
const restPageSize = 100

// fallbackNoted keeps the transport note to one line per run.
var fallbackNoted bool

func noteFallback(what string, graphQLErr error) {
	if fallbackNoted {
		return
	}
	fallbackNoted = true
	fmt.Fprintf(os.Stderr, "shipcheck: GraphQL unavailable for %s (%v); reading the evidence over REST, which cannot report a review body edited after the attestation: read the reviews once more before merging\n", what, graphQLErr)
}

func fallbackError(what string, graphQLErr, restErr error) error {
	return fmt.Errorf("%s: GraphQL: %v; REST: %v", what, graphQLErr, restErr)
}

func loadPullRequestSummary(repo string, number int) (pullRequest, error) {
	pr, graphQLErr := loadPullRequestGraphQL(repo, number)
	if graphQLErr == nil {
		return pr, nil
	}
	pr, restErr := loadPullRequestREST(repo, number)
	if restErr != nil {
		return pullRequest{}, fallbackError("pull request", graphQLErr, restErr)
	}
	noteFallback("the pull request", graphQLErr)
	return pr, nil
}

func loadComments(repo string, number int) ([]comment, error) {
	comments, graphQLErr := loadCommentsGraphQL(repo, number)
	if graphQLErr == nil {
		return comments, nil
	}
	comments, restErr := loadCommentsREST(repo, number)
	if restErr != nil {
		return nil, fallbackError("comments", graphQLErr, restErr)
	}
	noteFallback("comments", graphQLErr)
	return comments, nil
}

func loadReviews(repo string, number int) ([]review, error) {
	reviews, graphQLErr := loadReviewsGraphQL(repo, number)
	if graphQLErr == nil {
		return reviews, nil
	}
	reviews, restErr := loadReviewsREST(repo, number)
	if restErr != nil {
		return nil, fallbackError("reviews", graphQLErr, restErr)
	}
	noteFallback("reviews", graphQLErr)
	return reviews, nil
}

func unresolvedReviewThreads(repo string, number int) (int, error) {
	unresolved, graphQLErr := unresolvedReviewThreadsGraphQL(repo, number)
	if graphQLErr == nil {
		return unresolved, nil
	}
	unresolved, restErr := unresolvedReviewThreadsREST(repo, number)
	if restErr != nil {
		return 0, fallbackError("review threads", graphQLErr, restErr)
	}
	noteFallback("review threads", graphQLErr)
	return unresolved, nil
}

// restGet runs `gh api --method GET endpoint` and decodes the document.
func restGet(endpoint string, out any) error {
	raw, err := runGH("api", "--method", "GET", endpoint)
	if err != nil {
		return fmt.Errorf("GET %s: %w: %s", endpoint, err, errorSnippet(raw))
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode %s: %w", endpoint, err)
	}
	return nil
}

// restPages walks a list endpoint page by page and returns its raw items.
// A page shorter than restPageSize ends the walk; running past
// maxThreadPages is an error rather than a partial list, the same bound
// the GraphQL walks apply. key names the array inside an envelope document
// (`check_runs`, `workflow_runs`, `statuses`); "" reads a bare array.
func restPages(endpoint, key string) ([]json.RawMessage, error) {
	var items []json.RawMessage
	for page := 1; page <= maxThreadPages; page++ {
		paged := fmt.Sprintf("%s%sper_page=%d&page=%d", endpoint, querySeparator(endpoint), restPageSize, page)
		var batch []json.RawMessage
		if key == "" {
			if err := restGet(paged, &batch); err != nil {
				return nil, err
			}
		} else {
			var envelope map[string]json.RawMessage
			if err := restGet(paged, &envelope); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(envelope[key], &batch); err != nil {
				return nil, fmt.Errorf("decode %s.%s: %w", paged, key, err)
			}
		}
		// A JSON null decodes into a nil slice without error; it is a
		// missing list, not a complete empty page.
		if batch == nil {
			return nil, fmt.Errorf("decode %s: the list is null, not an array", paged)
		}
		items = append(items, batch...)
		if len(batch) < restPageSize {
			return items, nil
		}
	}
	return nil, fmt.Errorf("%s ran past %d pages", endpoint, maxThreadPages)
}

func querySeparator(endpoint string) string {
	if strings.Contains(endpoint, "?") {
		return "&"
	}
	return "?"
}

func decodeEach[T any](items []json.RawMessage, what string) ([]T, error) {
	out := make([]T, 0, len(items))
	for _, item := range items {
		var v T
		if err := json.Unmarshal(item, &v); err != nil {
			return nil, fmt.Errorf("decode %s: %w", what, err)
		}
		out = append(out, v)
	}
	return out, nil
}

// loadPullRequestREST assembles the summary `gh pr view --json` would have
// returned from the pull request, its files, the head commit's check runs
// joined to their workflow runs for the workflow name, its status contexts,
// and its reviews and base-branch rules for the decision.
func loadPullRequestREST(repo string, number int) (pullRequest, error) {
	base := fmt.Sprintf("repos/%s/pulls/%d", repo, number)
	var raw struct {
		State string `json:"state"`
		Draft bool   `json:"draft"`
		Base  struct {
			Ref string `json:"ref"`
		} `json:"base"`
		Head struct {
			SHA string `json:"sha"`
		} `json:"head"`
		AutoMerge    json.RawMessage `json:"auto_merge"`
		ChangedFiles int             `json:"changed_files"`
	}
	if err := restGet(base, &raw); err != nil {
		return pullRequest{}, err
	}
	pr := pullRequest{
		State:        strings.ToUpper(raw.State),
		BaseRefName:  raw.Base.Ref,
		IsDraft:      raw.Draft,
		HeadRefOID:   raw.Head.SHA,
		ChangedFiles: raw.ChangedFiles,
	}
	// REST always carries the auto_merge key, null when none is queued; a
	// document without it leaves Present false, which evaluate rejects.
	if raw.AutoMerge != nil {
		pr.AutoMergeRequest = presentJSON{Present: true, Value: raw.AutoMerge}
	}

	fileItems, err := restPages(base+"/files", "")
	if err != nil {
		return pullRequest{}, err
	}
	files, err := decodeEach[struct {
		Filename string `json:"filename"`
	}](fileItems, "pull request files")
	if err != nil {
		return pullRequest{}, err
	}
	for _, file := range files {
		pr.Files = append(pr.Files, changedFile{Path: file.Filename})
	}

	checks, err := loadStatusChecksREST(repo, pr.HeadRefOID)
	if err != nil {
		return pullRequest{}, err
	}
	pr.StatusChecks = checks

	decision, err := reviewDecisionREST(repo, number, pr.BaseRefName)
	if err != nil {
		return pullRequest{}, err
	}
	pr.ReviewDecision = decision
	return pr, nil
}

// loadStatusChecksREST is the head commit's check rollup: every check run
// (`filter=all`, so a cancelled duplicate stays visible for the duplicate
// rules in evaluate, as it is in the GraphQL rollup) with the workflow name
// its check suite belongs to, plus the latest status per context from the
// combined-status document, paged like every other list.
func loadStatusChecksREST(repo, sha string) ([]statusCheck, error) {
	if sha == "" {
		return nil, fmt.Errorf("pull request has no head commit to load checks for")
	}
	runItems, err := restPages(fmt.Sprintf("repos/%s/actions/runs?head_sha=%s", repo, sha), "workflow_runs")
	if err != nil {
		return nil, err
	}
	runs, err := decodeEach[struct {
		Name         string `json:"name"`
		CheckSuiteID int64  `json:"check_suite_id"`
	}](runItems, "workflow runs")
	if err != nil {
		return nil, err
	}
	workflows := make(map[int64]string, len(runs))
	for _, run := range runs {
		workflows[run.CheckSuiteID] = run.Name
	}

	checkItems, err := restPages(fmt.Sprintf("repos/%s/commits/%s/check-runs?filter=all", repo, sha), "check_runs")
	if err != nil {
		return nil, err
	}
	checkRuns, err := decodeEach[struct {
		Name        string `json:"name"`
		Status      string `json:"status"`
		Conclusion  string `json:"conclusion"`
		StartedAt   string `json:"started_at"`
		CompletedAt string `json:"completed_at"`
		CheckSuite  struct {
			ID int64 `json:"id"`
		} `json:"check_suite"`
	}](checkItems, "check runs")
	if err != nil {
		return nil, err
	}
	checks := make([]statusCheck, 0, len(checkRuns))
	for _, run := range checkRuns {
		checks = append(checks, statusCheck{
			Type:        "CheckRun",
			Name:        run.Name,
			Workflow:    workflows[run.CheckSuite.ID],
			Status:      strings.ToUpper(run.Status),
			Conclusion:  strings.ToUpper(run.Conclusion),
			StartedAt:   run.StartedAt,
			CompletedAt: run.CompletedAt,
		})
	}

	statusItems, err := restPages(fmt.Sprintf("repos/%s/commits/%s/status", repo, sha), "statuses")
	if err != nil {
		return nil, err
	}
	statuses, err := decodeEach[struct {
		Context   string `json:"context"`
		State     string `json:"state"`
		CreatedAt string `json:"created_at"`
	}](statusItems, "status contexts")
	if err != nil {
		return nil, err
	}
	for _, status := range statuses {
		checks = append(checks, statusCheck{
			Type:      "StatusContext",
			Context:   status.Context,
			State:     strings.ToUpper(status.State),
			CreatedAt: status.CreatedAt,
		})
	}
	return checks, nil
}

type restReview struct {
	User struct {
		Login string `json:"login"`
	} `json:"user"`
	AuthorAssociation string `json:"author_association"`
	State             string `json:"state"`
	SubmittedAt       string `json:"submitted_at"`
}

// approvalCounts reports whether an approving review from an author with
// this association counts toward a required review, as GitHub counts only
// approvals from people with write access. A request for changes is
// honored from anyone: blocking on it is the safe direction.
func approvalCounts(association string) bool {
	switch association {
	case "OWNER", "MEMBER", "COLLABORATOR":
		return true
	default:
		return false
	}
}

func restReviews(repo string, number int) ([]restReview, error) {
	items, err := restPages(fmt.Sprintf("repos/%s/pulls/%d/reviews", repo, number), "")
	if err != nil {
		return nil, err
	}
	return decodeEach[restReview](items, "reviews")
}

// loadReviewsREST carries only submitted_at: REST does not expose a
// review's updatedAt, which is the gap noteFallback names.
func loadReviewsREST(repo string, number int) ([]review, error) {
	raw, err := restReviews(repo, number)
	if err != nil {
		return nil, err
	}
	reviews := make([]review, 0, len(raw))
	for _, r := range raw {
		reviews = append(reviews, review{SubmittedAt: r.SubmittedAt})
	}
	return reviews, nil
}

// reviewDecisionREST is the decision the reviews imply, in the vocabulary
// evaluate reads: CHANGES_REQUESTED when any reviewer's latest decisive
// review asks for changes, REVIEW_REQUIRED when the base branch's rulesets
// or classic protection require more distinct counting approvals than the
// reviewers have given or a review predicate REST cannot evaluate (a
// code-owner review, a last-push approval), APPROVED when at least one
// counting reviewer approves and every requirement is met, and "" when
// nothing decides. A dismissed review has state DISMISSED and decides
// nothing; a decisive review without a reviewer login is an error, since
// the latest review per reviewer cannot be established without the
// reviewer.
func reviewDecisionREST(repo string, number int, base string) (string, error) {
	raw, err := restReviews(repo, number)
	if err != nil {
		return "", err
	}
	// "Latest per reviewer" needs submission order; sort rather than trust
	// the list's order, which GitHub documents but this tool need not rely on.
	slices.SortStableFunc(raw, func(a, b restReview) int {
		return strings.Compare(a.SubmittedAt, b.SubmittedAt)
	})
	latest := map[string]string{}
	for _, r := range raw {
		switch r.State {
		case "CHANGES_REQUESTED", "APPROVED":
		default:
			continue
		}
		if r.User.Login == "" {
			return "", fmt.Errorf("a %s review carries no reviewer login, so the review decision cannot be established", r.State)
		}
		switch {
		case r.State == "CHANGES_REQUESTED":
			latest[r.User.Login] = r.State
		case approvalCounts(r.AuthorAssociation):
			latest[r.User.Login] = r.State
		default:
			delete(latest, r.User.Login)
		}
	}
	approvals := 0
	for _, state := range latest {
		if state == "CHANGES_REQUESTED" {
			return state, nil
		}
		approvals++
	}
	rules, err := reviewRulesREST(repo, base)
	if err != nil {
		return "", err
	}
	switch {
	case rules.opaque != "":
		fmt.Fprintf(os.Stderr, "shipcheck: the base branch requires %s, which REST cannot evaluate; treating the review decision as REVIEW_REQUIRED\n", rules.opaque)
		return "REVIEW_REQUIRED", nil
	case approvals < rules.required:
		return "REVIEW_REQUIRED", nil
	case approvals > 0:
		return "APPROVED", nil
	default:
		return "", nil
	}
}

// reviewRules is what the base branch asks of reviews, from its rulesets
// and its classic branch protection together: the largest number of
// approving reviews either requires, and the name of a predicate this tool
// cannot evaluate over REST, if any.
type reviewRules struct {
	required int
	opaque   string
}

func reviewRulesREST(repo, base string) (reviewRules, error) {
	out, err := rulesetReviewRules(repo, base)
	if err != nil {
		return reviewRules{}, err
	}
	classic, err := classicReviewRules(repo, base)
	if err != nil {
		return reviewRules{}, err
	}
	if classic.required > out.required {
		out.required = classic.required
	}
	if out.opaque == "" {
		out.opaque = classic.opaque
	}
	return out, nil
}

// classicReviewRules reads classic branch protection, which rulesets did
// not replace. The branch document's `protection.enabled` says whether
// classic protection exists at all (its `protected` flag is also true under
// a ruleset, so it cannot tell the two apart), so a branch without classic
// protection costs one request that any reader may make. A branch with it
// whose protection document cannot be read, as an integration token
// without administration cannot, is an error, not "no requirement".
func classicReviewRules(repo, base string) (reviewRules, error) {
	var branch struct {
		Protection struct {
			Enabled bool `json:"enabled"`
		} `json:"protection"`
	}
	if err := restGet(fmt.Sprintf("repos/%s/branches/%s", repo, base), &branch); err != nil {
		return reviewRules{}, err
	}
	if !branch.Protection.Enabled {
		return reviewRules{}, nil
	}
	var protection struct {
		RequiredPullRequestReviews *struct {
			RequiredApprovingReviewCount int  `json:"required_approving_review_count"`
			RequireCodeOwnerReviews      bool `json:"require_code_owner_reviews"`
			RequireLastPushApproval      bool `json:"require_last_push_approval"`
		} `json:"required_pull_request_reviews"`
	}
	if err := restGet(fmt.Sprintf("repos/%s/branches/%s/protection", repo, base), &protection); err != nil {
		return reviewRules{}, err
	}
	var out reviewRules
	if reviews := protection.RequiredPullRequestReviews; reviews != nil {
		out.required = reviews.RequiredApprovingReviewCount
		switch {
		case reviews.RequireCodeOwnerReviews:
			out.opaque = "a code-owner review"
		case reviews.RequireLastPushApproval:
			out.opaque = "an approval after the last push"
		}
	}
	return out, nil
}

// rulesetReviewRules walks the rules that apply to base, paged like every
// other list: a pull_request rule past the first page is still a rule.
func rulesetReviewRules(repo, base string) (reviewRules, error) {
	items, err := restPages(fmt.Sprintf("repos/%s/rules/branches/%s", repo, base), "")
	if err != nil {
		return reviewRules{}, err
	}
	rules, err := decodeEach[struct {
		Type       string `json:"type"`
		Parameters struct {
			RequiredApprovingReviewCount int  `json:"required_approving_review_count"`
			RequireCodeOwnerReview       bool `json:"require_code_owner_review"`
			RequireLastPushApproval      bool `json:"require_last_push_approval"`
		} `json:"parameters"`
	}](items, "branch rules")
	if err != nil {
		return reviewRules{}, err
	}
	var out reviewRules
	for _, rule := range rules {
		if rule.Type != "pull_request" {
			continue
		}
		if rule.Parameters.RequiredApprovingReviewCount > out.required {
			out.required = rule.Parameters.RequiredApprovingReviewCount
		}
		switch {
		case rule.Parameters.RequireCodeOwnerReview:
			out.opaque = "a code-owner review"
		case rule.Parameters.RequireLastPushApproval:
			out.opaque = "an approval after the last push"
		}
	}
	return out, nil
}

func loadCommentsREST(repo string, number int) ([]comment, error) {
	items, err := restPages(fmt.Sprintf("repos/%s/issues/%d/comments", repo, number), "")
	if err != nil {
		return nil, err
	}
	raw, err := decodeEach[struct {
		User struct {
			Login string `json:"login"`
		} `json:"user"`
		AuthorAssociation string `json:"author_association"`
		Body              string `json:"body"`
		CreatedAt         string `json:"created_at"`
		UpdatedAt         string `json:"updated_at"`
		URL               string `json:"html_url"`
	}](items, "comments")
	if err != nil {
		return nil, err
	}
	comments := make([]comment, 0, len(raw))
	for _, c := range raw {
		comments = append(comments, comment{
			Author:            actor{Login: c.User.Login},
			AuthorAssociation: c.AuthorAssociation,
			Body:              c.Body,
			CreatedAt:         c.CreatedAt,
			UpdatedAt:         c.UpdatedAt,
			URL:               c.URL,
		})
	}
	return comments, nil
}

// unresolvedReviewThreadsREST reads the review-thread route the Claude Code
// proxy serves beside the REST API, which returns every thread in one
// document. More threads than the GraphQL walk would read is reported as
// incomplete rather than counted.
func unresolvedReviewThreadsREST(repo string, number int) (int, error) {
	var threads []struct {
		Resolved bool `json:"resolved"`
	}
	if err := restGet(fmt.Sprintf("repos/%s/pulls/%d/ccr/review_threads", repo, number), &threads); err != nil {
		return 0, err
	}
	// A JSON null decodes into a nil slice without error; it is a missing
	// thread list, not an empty one, and must not read as "all resolved".
	if threads == nil {
		return 0, fmt.Errorf("review-thread route returned no thread list")
	}
	if len(threads) > maxThreadPages*restPageSize {
		return 0, fmt.Errorf("review-thread route returned %d threads, more than the %d this check reads", len(threads), maxThreadPages*restPageSize)
	}
	unresolved := 0
	for _, thread := range threads {
		if !thread.Resolved {
			unresolved++
		}
	}
	return unresolved, nil
}
