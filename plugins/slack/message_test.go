package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
)

// fakeSlack stands in for slack.com: it records every request and answers with
// a canned body, so each test reads what the plugin actually put on the wire.
type fakeSlack struct {
	mu       sync.Mutex
	requests []recorded
	status   int
	answer   string
	srv      *httptest.Server
	client   *http.Client
}

type recorded struct {
	path string
	auth string
	body map[string]any
}

func newFakeSlack(t *testing.T, answer string) *fakeSlack {
	t.Helper()
	f := &fakeSlack{answer: answer, status: http.StatusOK}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("request body is not JSON: %v\n%s", err, raw)
		}
		f.mu.Lock()
		f.requests = append(f.requests, recorded{path: r.URL.Path, auth: r.Header.Get("Authorization"), body: body})
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(f.answer))
	}))
	t.Cleanup(f.srv.Close)
	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithDenyRedirects(),
		netpolicy.WithMaxResponseBytes(64<<10), netpolicy.WithTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	f.client = policy.Client()
	return f
}

func (f *fakeSlack) only(t *testing.T) recorded {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 1 {
		t.Fatalf("Slack received %d requests, want exactly 1", len(f.requests))
	}
	return f.requests[0]
}

func (f *fakeSlack) post(t *testing.T, in *slackv1.PostInputs) (*slackResponse, error) {
	t.Helper()
	p, err := planPost(in)
	if err != nil {
		t.Fatalf("planPost: %v", err)
	}
	return send(context.Background(), f.client, f.srv.URL+"/api/", "xoxb-inert", p)
}

func (f *fakeSlack) update(t *testing.T, in *slackv1.UpdateInputs) (*slackResponse, error) {
	t.Helper()
	p, err := planUpdate(in)
	if err != nil {
		t.Fatalf("planUpdate: %v", err)
	}
	return send(context.Background(), f.client, f.srv.URL+"/api/", "xoxb-inert", p)
}

const (
	okPost      = `{"ok":true,"channel":"C123APPROVAL","ts":"1503435956.000247"}`
	okUpdate    = `{"ok":true,"channel":"C123APPROVAL","ts":"1503435956.000247"}`
	okEphemeral = `{"ok":true,"message_ts":"1503435960.000300"}`
	threadTS    = "1503435956.000247"
)

func TestPostSendsTheBoundedWireShape(t *testing.T) {
	f := newFakeSlack(t, okPost)
	in := validPostInputs()
	in.ThreadTs, in.ReplyBroadcast = threadTS, true
	in.Metadata = &slackv1.Metadata{EventType: "flowstate_gate", EventPayload: map[string]string{"run": "r-1"}}
	got, err := f.post(t, in)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if got.Channel != "C123APPROVAL" || got.TS != threadTS {
		t.Errorf("response = %+v", got)
	}
	r := f.only(t)
	if r.path != "/api/chat.postMessage" || r.auth != "Bearer xoxb-inert" {
		t.Errorf("request = %s with %q", r.path, r.auth)
	}
	for key, want := range map[string]any{
		"channel": "C123APPROVAL", "text": "Approve deploy 42?", "client_msg_id": testIdempotencyKey,
		"thread_ts": threadTS, "reply_broadcast": true, "unfurl_links": false, "unfurl_media": false,
	} {
		if r.body[key] != want {
			t.Errorf("%s = %v, want %v", key, r.body[key], want)
		}
	}
	if meta, _ := r.body["metadata"].(map[string]any); meta["event_type"] != "flowstate_gate" {
		t.Errorf("metadata = %v", r.body["metadata"])
	}
	for _, forbidden := range []string{"icon_url", "icon_emoji", "username", "blocks", "as_user"} {
		if _, present := r.body[forbidden]; present {
			t.Errorf("request carries %q", forbidden)
		}
	}
}

func TestPostTextIsPlainNeverMarkup(t *testing.T) {
	f := newFakeSlack(t, okPost)
	in := validPostInputs()
	in.Text = "<!channel> ship it <@U1> <https://x|y> & co"
	if _, err := f.post(t, in); err != nil {
		t.Fatal(err)
	}
	want := "&lt;!channel&gt; ship it &lt;@U1&gt; &lt;https://x|y&gt; &amp; co"
	if got := f.only(t).body["text"]; got != want {
		t.Errorf("text on the wire = %q, want %q", got, want)
	}
}

func TestPostCardSendsBlocksAndDerivedFallback(t *testing.T) {
	f := newFakeSlack(t, okPost)
	in := validPostInputs()
	in.Text = ""
	in.Card = &chatv1.Card{Kind: &chatv1.Card_Notice{Notice: &chatv1.Notice{
		Title: &chatv1.Text{Kind: &chatv1.Text_Plain{Plain: "Maintenance <soon>"}},
	}}}
	if _, err := f.post(t, in); err != nil {
		t.Fatal(err)
	}
	r := f.only(t)
	if r.body["text"] != "Maintenance &lt;soon&gt;" {
		t.Errorf("derived fallback = %q", r.body["text"])
	}
	blocks, _ := r.body["blocks"].([]any)
	if len(blocks) != 1 || blocks[0].(map[string]any)["type"] != "header" {
		t.Errorf("blocks = %v", r.body["blocks"])
	}

	// An explicit text beside the card is the fallback instead.
	f2 := newFakeSlack(t, okPost)
	in.Text = "Deploy needs you"
	if _, err := f2.post(t, in); err != nil {
		t.Fatal(err)
	}
	if got := f2.only(t).body["text"]; got != "Deploy needs you" {
		t.Errorf("explicit fallback = %q", got)
	}
}

func TestEphemeralUsesPostEphemeralAndReturnsMessageTS(t *testing.T) {
	f := newFakeSlack(t, okEphemeral)
	in := validPostInputs()
	in.ToUser, in.ThreadTs = "U0ALICE", threadTS
	got, err := f.post(t, in)
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageTS != "1503435960.000300" {
		t.Errorf("message_ts = %q", got.MessageTS)
	}
	r := f.only(t)
	if r.path != "/api/chat.postEphemeral" || r.body["user"] != "U0ALICE" || r.body["thread_ts"] != threadTS {
		t.Errorf("request = %s %v", r.path, r.body)
	}
	for _, unsupported := range []string{"client_msg_id", "metadata", "unfurl_links", "reply_broadcast"} {
		if _, present := r.body[unsupported]; present {
			t.Errorf("ephemeral request carries %q, which chat.postEphemeral does not take", unsupported)
		}
	}
}

func TestUpdateSendsTargetAndClearsBlocksForText(t *testing.T) {
	f := newFakeSlack(t, okUpdate)
	got, err := f.update(t, &slackv1.UpdateInputs{Channel: "C123APPROVAL", Ts: threadTS, Text: "Approved by <@U1>"})
	if err != nil {
		t.Fatal(err)
	}
	if got.TS != threadTS {
		t.Errorf("ts = %q", got.TS)
	}
	r := f.only(t)
	if r.path != "/api/chat.update" || r.body["ts"] != threadTS || r.body["channel"] != "C123APPROVAL" {
		t.Errorf("request = %s %v", r.path, r.body)
	}
	if blocks, present := r.body["blocks"]; !present || len(blocks.([]any)) != 0 {
		t.Errorf("blocks = %v (present %v), want an explicit empty list so the old layout is cleared", blocks, present)
	}
	if r.body["text"] != "Approved by &lt;@U1&gt;" {
		t.Errorf("text = %q", r.body["text"])
	}
}

func TestUpdateWithAStatusCard(t *testing.T) {
	f := newFakeSlack(t, okUpdate)
	_, err := f.update(t, &slackv1.UpdateInputs{Channel: "C123APPROVAL", Ts: threadTS, Card: &chatv1.Card{Kind: &chatv1.Card_Status{Status: &chatv1.Status{
		State: chatv1.State_STATE_OK, Title: &chatv1.Text{Kind: &chatv1.Text_Plain{Plain: "Deployed"}},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	r := f.only(t)
	if blocks := r.body["blocks"].([]any); len(blocks) != 2 || r.body["text"] != "Deployed" {
		t.Errorf("body = %v", r.body)
	}
}

func TestUpdateInputBounds(t *testing.T) {
	for name, mutate := range map[string]func(*slackv1.UpdateInputs){
		"channel name":    func(in *slackv1.UpdateInputs) { in.Channel = "general" },
		"missing ts":      func(in *slackv1.UpdateInputs) { in.Ts = "" },
		"nothing to send": func(in *slackv1.UpdateInputs) { in.Text = "" },
		"card and blocks": func(in *slackv1.UpdateInputs) { in.Card, in.Blocks = testCard(), testBlocks() },
	} {
		t.Run(name, func(t *testing.T) {
			in := &slackv1.UpdateInputs{Channel: "C123APPROVAL", Ts: threadTS, Text: "x"}
			mutate(in)
			if _, err := planUpdate(in); err == nil || !sdk.IsInvalidInput(err) {
				t.Fatalf("planUpdate error = %v, want InvalidInput", err)
			}
		})
	}
}

// TestUnknownOutcomePostureDiffersByTask is the retry contract: a lost answer
// to a post is never retried (the message may exist), a lost answer to an
// update is retryable (the same content on the same message is the same state).
func TestUnknownOutcomePostureDiffersByTask(t *testing.T) {
	f := newFakeSlack(t, `{"ok":false,"error":"internal_error"}`)
	f.status = http.StatusInternalServerError
	_, postErr := f.post(t, validPostInputs())
	if !sdk.IsOutcomeUnknown(postErr) || sdk.IsUnavailable(postErr) {
		t.Errorf("post error = %v, want an unknown outcome that is not retryable", postErr)
	}
	_, updateErr := f.update(t, &slackv1.UpdateInputs{Channel: "C123APPROVAL", Ts: threadTS, Text: "x"})
	if !sdk.IsUnavailable(updateErr) || sdk.IsOutcomeUnknown(updateErr) {
		t.Errorf("update error = %v, want retryable", updateErr)
	}
}

func TestSlackRefusalsAreClassifiedAndCarryDetail(t *testing.T) {
	f := newFakeSlack(t, `{"ok":false,"error":"invalid_blocks","response_metadata":{"messages":["[ERROR] must be less than 3001 characters [json-pointer:/blocks/0/text/text]"]}}`)
	_, err := f.post(t, validPostInputs())
	if !sdk.IsInvalidInput(err) || !strings.Contains(err.Error(), "invalid_blocks") || !strings.Contains(err.Error(), "json-pointer:/blocks/0/text/text") {
		t.Errorf("error = %v, want InvalidInput carrying Slack's detail", err)
	}
	f.answer = `{"ok":false,"error":"not_in_channel"}`
	if _, err = f.post(t, validPostInputs()); !sdk.IsPermissionDenied(err) || !strings.Contains(err.Error(), "invite it") {
		t.Errorf("not_in_channel error = %v", err)
	}
}

func TestAnAcknowledgementThatDoesNotMatchIsUnknown(t *testing.T) {
	f := newFakeSlack(t, `{"ok":true,"channel":"C0OTHER","ts":"1503435956.000247"}`)
	if _, err := f.post(t, validPostInputs()); !sdk.IsOutcomeUnknown(err) {
		t.Errorf("post to the wrong channel echo: %v", err)
	}
	f.answer = `{"ok":true,"channel":"C123APPROVAL","ts":"1503435999.000001"}`
	if _, err := f.update(t, &slackv1.UpdateInputs{Channel: "C123APPROVAL", Ts: threadTS, Text: "x"}); !sdk.IsUnavailable(err) {
		t.Errorf("update echoing another ts: %v", err)
	}
}

func TestBadInputsReachNoNetwork(t *testing.T) {
	f := newFakeSlack(t, okPost)
	in := validPostInputs()
	in.ReplyBroadcast = true
	if _, err := planPost(in); err == nil {
		t.Fatal("planPost accepted reply_broadcast without thread_ts")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 0 {
		t.Fatalf("Slack received %d requests for an input the plugin refuses", len(f.requests))
	}
}

// TestFlowfileShapedLiteralsDecodeIntoNestedInputs proves the SDK's structural
// decode reaches slack.post: the maps and lists below are what a Flowfile's
// `card:` and `blocks:` literals become, with enums written the Flowfile's way.
func TestFlowfileShapedLiteralsDecodeIntoNestedInputs(t *testing.T) {
	approval := flowstatev1.NewValue(map[string]any{"approval": map[string]any{
		"title":   map[string]any{"plain": "Production deploy"},
		"summary": map[string]any{"markup": map[string]any{"template": "*{change}* by {who}", "args": map[string]any{"change": "api v1", "who": "alice"}}},
		"fields":  []any{map[string]any{"label": "Service", "value": map[string]any{"plain": "api"}}},
		"approve": map[string]any{"label": map[string]any{"plain": "Approve"}, "callback": map[string]any{"action": "approve", "value": "run-1"}},
		"reject": map[string]any{
			"label": map[string]any{"plain": "Reject"}, "callback": map[string]any{"action": "reject", "value": "run-1"}, "style": "danger",
			"confirm": map[string]any{"title": map[string]any{"plain": "Reject?"}, "text": map[string]any{"plain": "Final."}},
		},
		"footer": map[string]any{"plain": "Expires in 24h"},
	}})
	var in slackv1.PostInputs
	err := sdk.DecodeInputs(map[string]*flowstatev1.Value{
		"channel": flowstatev1.NewValue("C123APPROVAL"), "idempotency_key": flowstatev1.NewValue(testIdempotencyKey), "card": approval,
	}, &in)
	if err != nil {
		t.Fatalf("decoding an approval card: %v", err)
	}
	p, err := planPost(&in)
	if err != nil {
		t.Fatalf("planPost: %v", err)
	}
	wire, _ := json.Marshal(p.body)
	for _, want := range []string{`"action_id":"approve"`, `"value":"run-1"`, `"style":"primary"`, `"style":"danger"`, `"confirm"`, `"text":"Expires in 24h"`} {
		if !strings.Contains(string(wire), want) {
			t.Errorf("rendered request lacks %s:\n%s", want, wire)
		}
	}

	var native slackv1.PostInputs
	err = sdk.DecodeInputs(map[string]*flowstatev1.Value{
		"channel": flowstatev1.NewValue("C123APPROVAL"), "idempotency_key": flowstatev1.NewValue(testIdempotencyKey),
		"blocks": flowstatev1.NewValue([]any{
			map[string]any{"header": map[string]any{"text": map[string]any{"plain": "Release"}}},
			map[string]any{"section": map[string]any{
				"text":      map[string]any{"markup": map[string]any{"template": "{n} changes", "args": map[string]any{"n": "3"}}},
				"accessory": map[string]any{"button": map[string]any{"text": map[string]any{"plain": "Open"}, "action_id": "open", "url": "https://example.com"}},
			}},
			map[string]any{"actions": map[string]any{"elements": []any{
				map[string]any{"button": map[string]any{"text": map[string]any{"plain": "Go"}, "action_id": "go", "style": "primary"}},
				map[string]any{"overflow": map[string]any{"action_id": "more", "options": []any{
					map[string]any{"text": map[string]any{"plain": "Snooze"}, "value": "snooze"},
					map[string]any{"text": map[string]any{"plain": "Mute"}, "value": "mute"},
				}}},
			}}},
		}),
	}, &native)
	if err != nil {
		t.Fatalf("decoding native blocks: %v", err)
	}
	if _, err := planPost(&native); err != nil {
		t.Fatalf("planPost(native blocks): %v", err)
	}

	// A misspelt member is refused by name instead of vanishing.
	err = sdk.DecodeInputs(map[string]*flowstatev1.Value{
		"blocks": flowstatev1.NewValue([]any{map[string]any{"sectoin": map[string]any{}}}),
	}, &slackv1.PostInputs{})
	if err == nil || !strings.Contains(err.Error(), "sectoin") {
		t.Errorf("misspelt block member: error = %v", err)
	}
}
