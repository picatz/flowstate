package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
)

const goodResponseURL = "https://hooks.slack.com/actions/T0001/123/abcdef"

func TestResponseURLIsPinnedToSlackHooks(t *testing.T) {
	for _, bad := range []string{
		"",
		"https://evil.example/actions/T0001/123/abc",
		"http://hooks.slack.com/actions/T0001/123/abc",
		"https://hooks.slack.com:8443/actions/T0001/123/abc",
		"https://hooks.slack.com.evil.example/actions/T0001/123/abc",
		"https://user@hooks.slack.com/actions/T0001/123/abc",
		"https://hooks.slack.com/services/T0001/B0001/abc",
		"https://hooks.slack.com/actions/T0001/123/abc?x=1",
		"https://hooks.slack.com/actions/T0001/123/abc#frag",
		"https://hooks.slack.com/actions/" + strings.Repeat("a", maxResponseURLBytes),
	} {
		if _, err := checkResponseURL(bad); err == nil {
			t.Errorf("checkResponseURL(%q) accepted an address Slack does not issue", bad)
		}
	}
	for _, good := range []string{goodResponseURL, "https://hooks.slack.com/commands/T0001/123/abc"} {
		if _, err := checkResponseURL(good); err != nil {
			t.Errorf("checkResponseURL(%q) = %v, want accepted", good, err)
		}
	}
}

func TestPlanRespond(t *testing.T) {
	t.Run("replace is the default and idempotent", func(t *testing.T) {
		p, err := planRespond(&slackv1.RespondInputs{ResponseUrl: goodResponseURL, Text: "Approved <!channel>"})
		if err != nil {
			t.Fatal(err)
		}
		if p.how != howReplace || !p.op.idempotent || p.body.ReplaceOrig == nil || !*p.body.ReplaceOrig {
			t.Fatalf("plan = %+v", p)
		}
		if strings.Contains(p.body.Text, "<!channel>") {
			t.Fatalf("text %q left a mention live", p.body.Text)
		}
	})
	t.Run("a new message is not retried", func(t *testing.T) {
		for _, how := range []string{howEphemeral, howInChannel} {
			p, err := planRespond(&slackv1.RespondInputs{ResponseUrl: goodResponseURL, How: how, Text: "hi"})
			if err != nil || p.op.idempotent || p.body.ResponseType != how || *p.body.ReplaceOrig {
				t.Fatalf("%s: plan = %+v err = %v", how, p, err)
			}
		}
	})
	t.Run("delete takes no body", func(t *testing.T) {
		if _, err := planRespond(&slackv1.RespondInputs{ResponseUrl: goodResponseURL, How: howDelete, Text: "x"}); err == nil {
			t.Fatal("delete with text was accepted")
		}
		p, err := planRespond(&slackv1.RespondInputs{ResponseUrl: goodResponseURL, How: howDelete})
		if err != nil || p.body.DeleteOriginal == nil || !*p.body.DeleteOriginal {
			t.Fatalf("plan = %+v err = %v", p, err)
		}
	})
	t.Run("a message needs content", func(t *testing.T) {
		if _, err := planRespond(&slackv1.RespondInputs{ResponseUrl: goodResponseURL}); err == nil {
			t.Fatal("empty replace was accepted")
		}
	})
}

func TestSendRespond(t *testing.T) {
	var got atomic.Pointer[map[string]any]
	var status atomic.Int32
	status.Store(200)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		got.Store(&m)
		if r.Header.Get("Authorization") != "" {
			t.Error("a response_url call must carry no credential header")
		}
		w.WriteHeader(int(status.Load()))
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(server.Close)

	p, err := planRespond(&slackv1.RespondInputs{ResponseUrl: goodResponseURL, Text: "Approved"})
	if err != nil {
		t.Fatal(err)
	}
	p.url = server.URL + "/actions/T/1/x"
	if err := sendRespond(t.Context(), server.Client(), p); err != nil {
		t.Fatalf("sendRespond = %v", err)
	}
	if m := *got.Load(); m["text"] != "Approved" || m["replace_original"] != true {
		t.Fatalf("body = %v", m)
	}

	for code, wantRetry := range map[int32]bool{404: false, 500: true} {
		status.Store(code)
		err := sendRespond(t.Context(), server.Client(), p)
		if err == nil {
			t.Fatalf("status %d: want an error", code)
		}
		if retryable := strings.Contains(err.Error(), "may be retried"); retryable != wantRetry {
			t.Errorf("status %d: %v (retryable=%v, want %v)", code, err, retryable, wantRetry)
		}
	}
}

func TestSendRespondDoesNotFollowARedirect(t *testing.T) {
	var elsewhere atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { elsewhere.Add(1) }))
	t.Cleanup(other.Close)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	p, _ := planRespond(&slackv1.RespondInputs{ResponseUrl: goodResponseURL, Text: "x"})
	p.url = server.URL + "/actions/T/1/x"
	if err := sendRespond(t.Context(), server.Client(), p); err == nil {
		t.Fatal("a redirect was treated as success")
	}
	if elsewhere.Load() != 0 {
		t.Fatal("the redirect target received the body")
	}
}
