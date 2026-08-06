package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-github/v75/github"
)

// TestDoIssueListMapsSortAndDirectionToTheRightQueryParameters proves
// issueListParams.sort and .direction actually reach GitHub's own "sort"
// and "direction" query parameters on the request go-github builds - the
// contract IssueListInputs.sort and .direction's own doc comments make
// (github.proto): element zero of a listing is the newest match unless a
// workflow asks for direction: asc, and that promise is only real if the
// value this task validates is the value GitHub actually receives.
//
// A plain *github.Client pointed at this local server, not newClient's
// egress-governed one, since this test is only about the query string
// doIssueList builds - not about egress policy, which by default denies
// loopback and would refuse this local server for a reason unrelated to
// what this test checks.
func TestDoIssueListMapsSortAndDirectionToTheRightQueryParameters(t *testing.T) {
	gotQuery := make(chan string, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery <- r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
	}))
	defer server.Close()

	client := github.NewClient(http.DefaultClient)
	baseURL := strings.TrimSuffix(server.URL, "/") + "/"
	client, err := client.WithEnterpriseURLs(baseURL, baseURL)
	if err != nil {
		t.Fatalf("WithEnterpriseURLs: unexpected error: %v", err)
	}

	sort, err := validateIssueSort("sort", "updated")
	if err != nil {
		t.Fatalf("validateIssueSort: unexpected error: %v", err)
	}
	direction, err := validateIssueDirection("direction", "asc")
	if err != nil {
		t.Fatalf("validateIssueDirection: unexpected error: %v", err)
	}

	_, _, _, err = doIssueList(context.Background(), client, "octocat", "hello-world", issueListParams{
		state:      "open",
		sort:       sort,
		direction:  direction,
		maxResults: 5,
	})
	if err != nil {
		t.Fatalf("doIssueList: unexpected error: %v", err)
	}

	select {
	case rawQuery := <-gotQuery:
		query, err := url.ParseQuery(rawQuery)
		if err != nil {
			t.Fatalf("parsing captured query %q: %v", rawQuery, err)
		}
		if got := query.Get("sort"); got != "updated" {
			t.Errorf(`query "sort" = %q, want "updated" (raw query: %q)`, got, rawQuery)
		}
		if got := query.Get("direction"); got != "asc" {
			t.Errorf(`query "direction" = %q, want "asc" (raw query: %q)`, got, rawQuery)
		}
	default:
		t.Fatal("the handler was never invoked")
	}
}
