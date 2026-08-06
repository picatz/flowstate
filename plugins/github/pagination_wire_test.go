package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-github/v75/github"
)

// newPagedTestServer starts an httptest server that serves count JSON
// records, itemJSON(i) for i in [0, count), paginated at whatever per_page
// a request asks for (falling back to defaultPerPage when unset - go-github
// omits per_page entirely when this task's own perPage matches its
// client's own default, which does not happen here, but the fallback keeps
// this helper honest against any caller that does not set it), with a
// GitHub-shaped Link "next" header on every page but the last - the same
// signal go-github's own Response.NextPage parsing (populatePageValues)
// looks for, so a test driving one of this plugin's doXxxList functions
// through a real *github.Client sees the identical pagination behavior a
// real GitHub server would produce, not the paginateBounded package's own
// synthetic *github.Response fixtures (paginate_test.go's own pagedInts).
//
// Deliberately item- and endpoint-agnostic: it does not care whether the
// caller is listing issues, pull requests, or files - only that whatever
// itemJSON returns is a complete JSON object.
func newPagedTestServer(t *testing.T, count, defaultPerPage int, itemJSON func(i int) string) *github.Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		if page < 1 {
			page = 1
		}
		perPage, _ := strconv.Atoi(q.Get("per_page"))
		if perPage < 1 {
			perPage = defaultPerPage
		}

		start := (page - 1) * perPage
		end := min(start+perPage, count)

		var items []string
		for i := start; i < end; i++ {
			if i < 0 || i >= count {
				continue
			}
			items = append(items, itemJSON(i))
		}

		if end < count {
			w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?page=%d>; rel="next"`, r.Host, r.URL.Path, page+1))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "[%s]", strings.Join(items, ","))
	}))
	t.Cleanup(server.Close)

	client := github.NewClient(http.DefaultClient)
	base := strings.TrimSuffix(server.URL, "/") + "/"
	client, err := client.WithEnterpriseURLs(base, base)
	if err != nil {
		t.Fatalf("WithEnterpriseURLs: unexpected error: %v", err)
	}
	return client
}

// issueJSON builds one github.Issue's JSON body: number n+1, created (and
// updated) at a timestamp that increases with n - "created" ascending order
// (this plugin's own required stable sort for a cursor-driven walk) then
// matches array order, the same way a real repository's issues do.
func issueJSON(n int) string {
	ts := fmt.Sprintf("2024-01-01T00:%02d:00Z", n%60)
	return fmt.Sprintf(`{"number": %d, "title": "issue %d", "state": "open", "created_at": %q, "updated_at": %q}`,
		n+1, n+1, ts, ts)
}

// pullRequestJSON builds one github.PullRequest's JSON body - same
// created-ascending shape as issueJSON.
func pullRequestJSON(n int) string {
	ts := fmt.Sprintf("2024-01-01T00:%02d:00Z", n%60)
	return fmt.Sprintf(`{"number": %d, "title": "pr %d", "state": "open", "created_at": %q, "updated_at": %q}`,
		n+1, n+1, ts, ts)
}

// newMutableIssueServer is newPagedTestServer's stateful counterpart, built
// specifically for the tests that need the repository's own issue list to
// change shape BETWEEN two of this task's calls -
// TestIssueListCursorToleratesAnInsertionBetweenPages,
// TestIssueListCursorCanMissAnItemDeletedBetweenPages, and
// TestIssueListCursorCanRepeatAReopenedIssueBetweenPages. It starts by
// serving issue numbers 1..n, in that order (matching issueJSON's own
// created-ascending timestamps), and returns three functions:
//
//   - grow appends by more issues at the tail (higher numbers, later
//     timestamps - the shape a newly opened issue actually has).
//   - remove deletes one issue number from the currently served list,
//     shifting everything after it one position earlier - the shape a
//     closed-and-filtered-out (or deleted) issue actually has.
//   - insertFront re-admits an issue number NOT currently in the served
//     list at the very front - the shape a REOPENED issue (or one newly
//     matching a label/since filter) actually has: its created_at did not
//     change, so if it is chronologically the oldest of everything this
//     walk could ever see, "created" ascending places it first regardless
//     of when it re-entered the matching set, ahead of positions a walk
//     may already have passed.
func newMutableIssueServer(t *testing.T, n, defaultPerPage int) (client *github.Client, grow func(by int), remove func(number int), insertFront func(number int)) {
	t.Helper()
	return newMutableIssueServerFrom(t, 1, n, defaultPerPage)
}

// newMutableIssueServerFrom is newMutableIssueServer with an explicit
// starting issue number, rather than always 1 - what
// TestIssueListCursorCanRepeatAReopenedIssueBetweenPages needs: a "reopened"
// issue's own created_at cannot move, and issueJSON derives a strictly
// increasing timestamp from a number's own value, so the only way to give a
// reopened issue a timestamp OLDER than every number a walk has already
// seen is to reserve a smaller number for it up front, unserved until
// insertFront admits it.
func newMutableIssueServerFrom(t *testing.T, start, n, defaultPerPage int) (client *github.Client, grow func(by int), remove func(number int), insertFront func(number int)) {
	t.Helper()

	var mu sync.Mutex
	numbers := make([]int, n)
	for i := range numbers {
		numbers[i] = start + i
	}
	next := start + n

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		current := append([]int(nil), numbers...)
		mu.Unlock()

		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		if page < 1 {
			page = 1
		}
		perPage, _ := strconv.Atoi(q.Get("per_page"))
		if perPage < 1 {
			perPage = defaultPerPage
		}

		start := (page - 1) * perPage
		end := min(start+perPage, len(current))

		var items []string
		for i := start; i < end; i++ {
			if i < 0 || i >= len(current) {
				continue
			}
			num := current[i]
			items = append(items, issueJSON(num-1)) // issueJSON is 0-indexed; number = i+1 there
		}

		if end < len(current) {
			w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?page=%d>; rel="next"`, r.Host, r.URL.Path, page+1))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "[%s]", strings.Join(items, ","))
	}))
	t.Cleanup(server.Close)

	httpClient := github.NewClient(http.DefaultClient)
	base := strings.TrimSuffix(server.URL, "/") + "/"
	httpClient, err := httpClient.WithEnterpriseURLs(base, base)
	if err != nil {
		t.Fatalf("WithEnterpriseURLs: unexpected error: %v", err)
	}

	grow = func(by int) {
		mu.Lock()
		defer mu.Unlock()
		for range by {
			numbers = append(numbers, next)
			next++
		}
	}
	remove = func(number int) {
		mu.Lock()
		defer mu.Unlock()
		out := numbers[:0]
		for _, n := range numbers {
			if n != number {
				out = append(out, n)
			}
		}
		numbers = out
	}
	insertFront = func(number int) {
		mu.Lock()
		defer mu.Unlock()
		numbers = append([]int{number}, numbers...)
	}

	return httpClient, grow, remove, insertFront
}

// newEmptyThenRealServer starts an httptest server that answers the first
// emptyPages page requests with a zero-item page and a Link "next" header
// (a legitimate GitHub response shape - CLAUDE.md's own List lesson: a
// large result set can be computed with the first several pages empty and
// NextPage still set), then serves count real items - itemJSON(i) for i in
// [0, count) - starting at page emptyPages+1, paginated at defaultPerPage.
// Built for the "a peer that pages through empties to the request bound
// yields a resumable cursor" requirement: emptyPages is meant to exceed
// maxListRequests, so a single doXxxList call spends its whole request
// budget on nothing but empty pages before ever reaching the real content,
// and the test asserts a cursor is still handed back - see
// cursorHasResumePosition's own doc comment for why len(out) > 0 alone is
// the wrong gate for that.
func newEmptyThenRealServer(t *testing.T, emptyPages, defaultPerPage, count int, itemJSON func(i int) string) *github.Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		if page < 1 {
			page = 1
		}
		perPage, _ := strconv.Atoi(q.Get("per_page"))
		if perPage < 1 {
			perPage = defaultPerPage
		}

		w.Header().Set("Content-Type", "application/json")

		if page <= emptyPages {
			w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?page=%d>; rel="next"`, r.Host, r.URL.Path, page+1))
			fmt.Fprint(w, "[]")
			return
		}

		realPage := page - emptyPages
		start := (realPage - 1) * perPage
		end := min(start+perPage, count)

		var items []string
		for i := start; i < end; i++ {
			if i < 0 || i >= count {
				continue
			}
			items = append(items, itemJSON(i))
		}
		if end < count {
			w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?page=%d>; rel="next"`, r.Host, r.URL.Path, page+1))
		}
		fmt.Fprintf(w, "[%s]", strings.Join(items, ","))
	}))
	t.Cleanup(server.Close)

	client := github.NewClient(http.DefaultClient)
	base := strings.TrimSuffix(server.URL, "/") + "/"
	client, err := client.WithEnterpriseURLs(base, base)
	if err != nil {
		t.Fatalf("WithEnterpriseURLs: unexpected error: %v", err)
	}
	return client
}

// commitFileJSON builds one github.CommitFile's JSON body - pull_request_files'
// own record shape, which (unlike issues and pull requests) GitHub gives
// this task no sort control over at all.
func commitFileJSON(n int) string {
	return fmt.Sprintf(`{"filename": "file-%d.go", "status": "modified", "additions": 1, "deletions": 0, "changes": 1}`, n)
}
