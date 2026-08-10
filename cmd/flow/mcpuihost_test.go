package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"

	"github.com/picatz/flowstate/cmd/flow/internal/fragments"
)

// The host double: the approval card, executed.
//
// What this adds to the tests next door. Everything in mcpui_test.go reads the
// card as text: that a coordinate is a member read, that a guard is written,
// that a golden's field names all appear in the function that builds it. Those
// are good tests of a document nobody runs, and #364 said so in as many words:
// nothing executed the JavaScript. Here it runs. A browser loads the card into a
// frame arranged the way the specification arranges one, a double on the outside
// speaks the postMessage dialect the card speaks, and the assertions are made
// against what the card *did*: the handshake it completed, the DOM it built from
// a delivered tool result, the tools/call a click actually produced, and the
// message it refused to act on.
//
// What this deliberately does not claim, in the words of #364's own checklist,
// which stays a manual acceptance list and is not moved into CI by this file:
//
//   - Not consent. No host here asks a human before forwarding a tools/call the
//     view makes, and nothing in this file could tell whether a real one does.
//     The double forwards nothing at all.
//   - Not Content-Security-Policy enforcement. The view is served with the
//     restrictive default the specification names, so the card is executed under
//     a policy at least as strict as a conforming host's, which shows the card
//     survives it, and shows nothing whatever about whether a host applies one.
//   - Not sandbox strength. The frames carry the specification's sandbox
//     attributes, and this asserts that the card behaves correctly inside them.
//     Whether those attributes contain a hostile document is the browser's claim
//     and the host's, never this test's.
//
// The four boxes on #364 (the reference host, two real hosts, and a host
// without the extension) are still ticked by a person against a real host, and
// still recorded in a pull request rather than in a test run.

// hostDoubleBudget bounds the whole of one browser test.
//
// Everything inside it is a poll against this deadline, so a card that never
// renders fails by naming what never became true rather than by hanging: a
// browser that will not start, a frame that never appears, and a handshake that
// never completes are all the same shape of failure and all reported here.
const hostDoubleBudget = 60 * time.Second

// specDefaultCSP is the policy the MCP Apps specification tells a host to apply
// to a view whose resource declares none, which the card deliberately does.
//
// Served as a real header on the view, so the document under test executes under
// it. It is here for fidelity of the environment, not as a claim: see the file
// comment above.
const specDefaultCSP = "default-src 'none'; " +
	"script-src 'self' 'unsafe-inline'; " +
	"style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; " +
	"media-src 'self' data:; " +
	"connect-src 'none'"

// cardHarness is one loaded card, its host double, and the way into both.
type cardHarness struct {
	ctx context.Context

	// page is the host double's own document; view is the card's, in a session
	// of its own because the browser puts an opaque-origin frame in a process of
	// its own. Two sessions because they are two documents on two origins, which
	// is the arrangement being reproduced rather than an artefact of the harness.
	page *session
	view *session

	// viewWorld is an isolated world inside the card's frame: the only way to
	// read that document, since no script on the page can reach it.
	viewWorld int
}

// newCardHarness serves the card behind a host double, loads it in a browser,
// and returns once the handshake has completed.
//
// Two servers, because the specification requires the host and the sandbox proxy
// to be on different origins and two loopback listeners on different ports are
// two origins. Both are httptest servers on 127.0.0.1, and the browser is
// launched with name resolution disabled, so the whole exercise is reachable
// only from itself.
func newCardHarness(t *testing.T) *cardHarness {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping: launches a browser to execute the card; CI runs the full suite")
	}

	ctx, cancel := context.WithTimeout(t.Context(), hostDoubleBudget)
	t.Cleanup(cancel)

	view := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/view":
			// The bytes compiled into the binary, unmodified. A harness that
			// edited the document to make it testable would be testing its own
			// edit; the whole point is that this is what a host is served.
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Content-Security-Policy", specDefaultCSP)
			_, _ = w.Write([]byte(fragments.ApprovalCard()))
		case "/sandbox":
			writeTestdataPage(w, r, "sandbox-proxy.html")
		case "/neighbour":
			writeTestdataPage(w, r, "neighbour.html")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(view.Close)

	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestdataPage(w, r, "host-double.html")
	}))
	t.Cleanup(host.Close)

	b := startBrowser(t, ctx)

	page := b.page()
	page.call(t, ctx, "Page.navigate", map[string]any{
		"url": host.URL + "/?sandbox=" + url.QueryEscape(view.URL),
	})

	harness := &cardHarness{ctx: ctx, page: page}
	harness.view = b.sessionFor(t, ctx, "/view")
	harness.viewWorld = harness.view.isolatedWorld(t, ctx)

	// The handshake, waited for rather than assumed: every other test here
	// begins from a card that is connected, and a card that never connected
	// would otherwise fail those tests somewhere less informative.
	page.until(t, ctx, 0, "window.__ready === true",
		"the card never completed the ui/initialize handshake with the host double")

	return harness
}

// writeTestdataPage serves one of the double's own documents.
func writeTestdataPage(w http.ResponseWriter, r *http.Request, name string) {
	page, err := os.ReadFile(filepath.Join("testdata", "approval-card", name))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(page)
}

// deliver hands the card a flowstate_get result through the host double.
//
// Marshalled exactly as [mcpHandler] marshals a response, protojson with
// EmitUnpopulated, so what the card parses here is the document the tool
// actually returns rather than a convenient approximation of it.
func (h *cardHarness) deliver(t *testing.T, response *v1.GetResponse) {
	t.Helper()

	h.page.evaluate(t, h.ctx, 0, "window.__deliver("+jsString(t, marshalGetResponse(t, response))+"), true", nil)
}

// spoof has a neighbouring frame send the card the same kind of notification.
func (h *cardHarness) spoof(t *testing.T, response *v1.GetResponse) {
	t.Helper()

	h.page.evaluate(t, h.ctx, 0, "window.__spoof("+jsString(t, marshalGetResponse(t, response))+"), true", nil)
}

// watchNeighbourSentinels installs a listener inside the card's frame that
// records the harness's own sentinel messages as they arrive.
//
// In the isolated world, which is what makes it usable as evidence: the card's
// document cannot see this listener and this listener cannot change the card's
// state. It observes the frame receiving a message and nothing else. The card's
// own listener sees the same events, and ignores these ones, because a sentinel
// carries no jsonrpc member.
func (h *cardHarness) watchNeighbourSentinels(t *testing.T) {
	t.Helper()

	h.inView(t, `(function () {
		window.__sentinels = [];
		window.addEventListener('message', function (event) {
			var data = event.data || {};
			if (typeof data.flowstateHarnessSentinel === 'string') {
				window.__sentinels.push(data.flowstateHarnessSentinel);
			}
		});
		return true;
	})()`, nil)
}

// neighbourSentinel has the neighbouring frame send the card a message the card
// can only ignore, and waits for the card's frame to have received it.
//
// This is the barrier the negative test needs. It travels the neighbour's
// channel, the same one a spoof travels, so it cannot arrive before a spoof sent
// ahead of it; and it carries nothing the card would act on, so it cannot
// overwrite whatever a spoof did.
func (h *cardHarness) neighbourSentinel(t *testing.T, name string) {
	t.Helper()

	h.page.evaluate(t, h.ctx, 0, "window.__neighbourSentinel("+jsString(t, name)+"), true", nil)
	h.untilInView(t,
		"(window.__sentinels || []).indexOf("+jsString(t, name)+") !== -1",
		"the neighbour's sentinel never reached the card's frame, so nothing can be concluded about "+
			"what the frame did with the message sent before it")
}

// respond answers a tools/call the card made.
func (h *cardHarness) respond(t *testing.T, id int, result map[string]any) {
	t.Helper()

	encoded, err := json.Marshal(result)
	require.NoError(t, err)

	h.page.evaluate(t, h.ctx, 0,
		"window.__respond("+jsNumber(id)+", "+string(encoded)+"), true", nil)
}

// calls returns the tool calls the card has made, in order.
func (h *cardHarness) calls(t *testing.T) []toolCall {
	t.Helper()

	var calls []toolCall
	h.page.evaluate(t, h.ctx, 0, "window.__calls()", &calls)

	return calls
}

// toolCall is one JSON-RPC request the card sent to its host.
type toolCall struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// inView evaluates an expression inside the card's own document.
func (h *cardHarness) inView(t *testing.T, expression string, out any) {
	t.Helper()

	h.view.evaluate(t, h.ctx, h.viewWorld, expression, out)
}

// untilInView polls an expression inside the card's document.
func (h *cardHarness) untilInView(t *testing.T, expression, what string) {
	t.Helper()

	h.view.until(t, h.ctx, h.viewWorld, expression, what)
}

// text returns the text of the first element matching a selector inside the
// card, or fails if there is none.
func (h *cardHarness) text(t *testing.T, selector string) string {
	t.Helper()

	var value string
	h.inView(t, "(document.querySelector("+jsString(t, selector)+") || {textContent: null}).textContent", &value)

	return value
}

func jsString(t *testing.T, value string) string {
	t.Helper()

	encoded, err := json.Marshal(value)
	require.NoError(t, err)

	return string(encoded)
}

func jsNumber(value int) string {
	encoded, _ := json.Marshal(value)

	return string(encoded)
}

func marshalGetResponse(t *testing.T, response *v1.GetResponse) string {
	t.Helper()

	encoded, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(response)
	require.NoError(t, err)

	return string(encoded)
}

// waitingRun is the run every test here displays: the shape a real flowstate_get
// answers with for a RUNNING run parked on two waits.
//
// The coordinates are the golden messages' coordinates, deliberately. The
// goldens in testdata were hand-written in #364 and checked against the schema;
// what was never checked is that the card produces them. Driving the card with
// the run they describe makes that a comparison rather than an assumption.
//
// Two waits, and only one of them prompted: the promptless one is the boundary
// the Codex review on #364 drew, and it is a runtime claim as much as a textual
// one: the buttons have to be gone from the document, not merely absent from a
// branch of the source.
func waitingRun() *v1.GetResponse {
	return &v1.GetResponse{
		WorkflowId: "expense-report-4417",
		RunId:      "0199a1f0-6a3f-7c21-9b1e-2f6c4d8e5a71",
		Status:     v1.RunResponse_STATUS_RUNNING,
		Starter:    "kim@example.com",
		StartTime:  timestamppb.New(time.Date(2026, 8, 8, 9, 0, 0, 0, time.UTC)),
		Progress: &v1.RunProgress{
			StepId: "approval",
			PendingWaits: []*v1.PendingWait{
				{
					StepId:     "manager-gate",
					SignalName: "manager-approved",
					Prompt:     "Approve $412.90 of travel expenses for Kim?",
					Policed:    true,
					Deadline:   timestamppb.New(time.Date(2026, 8, 9, 9, 0, 0, 0, time.UTC)),
				},
				{
					StepId:     "order-gate",
					Path:       []string{"fulfilment"},
					SignalName: "update",
				},
			},
		},
	}
}

// TestTheCardCompletesTheHandshakeWithAHost.
//
// The static test next door asserts the three method names appear in the file
// and that the listener is installed before the request goes out. Neither can
// see whether the sequence completes: a card that installs its listener, sends
// ui/initialize and then never resolves its promise passes every one of those
// scans and connects to nothing. Here the messages are counted as they cross a
// frame boundary, in order, in a browser.
func TestTheCardCompletesTheHandshakeWithAHost(t *testing.T) {
	harness := newCardHarness(t)

	var transcript []struct {
		Dir     string `json:"dir"`
		Message struct {
			JSONRPC string `json:"jsonrpc"`
			ID      *int   `json:"id"`
			Method  string `json:"method"`
			Params  struct {
				ProtocolVersion string `json:"protocolVersion"`
				ClientInfo      struct {
					Name string `json:"name"`
				} `json:"clientInfo"`
				AppCapabilities struct {
					AvailableDisplayModes []string `json:"availableDisplayModes"`
				} `json:"appCapabilities"`
			} `json:"params"`
		} `json:"message"`
	}
	harness.page.evaluate(t, harness.ctx, 0, "window.__transcript", &transcript)

	require.GreaterOrEqual(t, len(transcript), 3,
		"the handshake is three messages and the boundary carried %d", len(transcript))

	initialize := transcript[0]
	assert.Equal(t, "from-view", initialize.Dir,
		"the first message across the boundary was not one the card sent")
	assert.Equal(t, "ui/initialize", initialize.Message.Method)
	assert.Equal(t, "2.0", initialize.Message.JSONRPC)
	require.NotNil(t, initialize.Message.ID,
		"ui/initialize went out as a notification, so no host could ever answer it")
	assert.Equal(t, "2026-01-26", initialize.Message.Params.ProtocolVersion)
	assert.Equal(t, "flowstate-approval-card", initialize.Message.Params.ClientInfo.Name)
	assert.Equal(t, []string{"inline"}, initialize.Message.Params.AppCapabilities.AvailableDisplayModes)

	answer := transcript[1]
	assert.Equal(t, "to-view", answer.Dir)
	require.NotNil(t, answer.Message.ID)
	assert.Equal(t, *initialize.Message.ID, *answer.Message.ID,
		"the host answered an id the card was not waiting on")

	initialized := transcript[2]
	assert.Equal(t, "from-view", initialized.Dir)
	assert.Equal(t, "ui/notifications/initialized", initialized.Message.Method,
		"the card never told the host it was ready, so a host that waits for it sends no data")
	assert.Nil(t, initialized.Message.ID,
		"the readiness notification carries an id, so a host would wait for a reply to it")

	// A card that failed its handshake says so in the live region. That it is
	// empty here is the other half of the claim: the promise resolved rather
	// than rejected.
	assert.Empty(t, harness.text(t, "#status"),
		"the card announced a handshake problem to a host that completed the handshake")
}

// TestADeliveredToolResultRendersTheRunAndItsGates.
//
// The static tests assert the binding function reads the right field names out
// of the response. What they cannot assert is that anything reaches the
// document: a binding that parsed the result perfectly and never called render
// would satisfy every one of them. This delivers the protojson a real
// flowstate_get returns and reads the DOM the card built out of it.
func TestADeliveredToolResultRendersTheRunAndItsGates(t *testing.T) {
	harness := newCardHarness(t)

	run := waitingRun()
	harness.deliver(t, run)

	harness.untilInView(t, "document.querySelectorAll('section').length === 2",
		"the delivered tool result never became two rendered gates")

	assert.Equal(t, run.GetWorkflowId(), harness.text(t, "#run-workflow"))
	assert.Equal(t, run.GetRunId(), harness.text(t, "#run-id"))
	assert.Equal(t, run.GetStarter(), harness.text(t, "#run-starter"))
	assert.Equal(t, "STATUS_RUNNING", harness.text(t, "#run-status"),
		"the card renders a status other than the one the response carried")

	var headings []string
	harness.inView(t, "Array.from(document.querySelectorAll('section h2')).map(function (h) { return h.textContent; })", &headings)
	assert.Equal(t, []string{"manager-approved", "update"}, headings,
		"the gates rendered are not the pending waits the response reported, in order")

	assert.Equal(t, run.GetProgress().GetPendingWaits()[0].GetPrompt(),
		harness.text(t, "section:nth-of-type(1) .prompt"),
		"the author's prompt is not what the card shows the person deciding")

	facts := harness.text(t, "section:nth-of-type(1) .facts")
	assert.Contains(t, facts, "signal manager-approved")
	assert.Contains(t, facts, "step manager-gate")
	assert.Contains(t, facts, "policed",
		"a policed gate does not say so, so the only carrier of that state would be a colour")
	assert.Contains(t, facts, "2026-08-09",
		"a gate that lapses does not print when")

	// Every state is a word before it is a hue, and the second gate is where the
	// negative spellings live.
	assert.Contains(t, harness.text(t, "section:nth-of-type(2) .facts"), "not policed")
	assert.Contains(t, harness.text(t, "section:nth-of-type(2) .facts"), "no deadline")
	assert.Contains(t, harness.text(t, "section:nth-of-type(2) .facts"), "inside fulfilment")

	// The Codex boundary, as the document rather than as the source: a wait
	// whose author wrote no prompt has no controls in the DOM at all. Not
	// hidden, not disabled, but absent, so there is nothing to click and nothing
	// for a keyboard or a screen reader to reach.
	var second struct {
		Buttons int `json:"buttons"`
		Inputs  int `json:"inputs"`
		Labels  int `json:"labels"`
	}
	harness.inView(t, `(function () {
		var section = document.querySelectorAll('section')[1];
		return {
			buttons: section.querySelectorAll('button').length,
			inputs: section.querySelectorAll('input').length,
			labels: section.querySelectorAll('label').length
		};
	})()`, &second)
	assert.Equal(t, 0, second.Buttons,
		"a wait whose author wrote no prompt offers a decision; clicking it would release the wait "+
			"with {approved} where the workflow declared some other payload entirely")
	assert.Equal(t, 0, second.Inputs)
	assert.Equal(t, 0, second.Labels)

	// And the prompted gate has exactly the two decisions, each named for the
	// gate it answers rather than being one of two identical "Approve"s.
	var labels []string
	harness.inView(t, "Array.from(document.querySelectorAll(\"section:nth-of-type(1) button\")).map(function (b) { return b.getAttribute('aria-label'); })", &labels)
	assert.Equal(t, []string{"Approve manager-approved", "Reject manager-approved"}, labels)

	// The comment box is bound to its label by id, which is a claim about two
	// attributes agreeing and therefore one only a document can answer.
	var bound bool
	harness.inView(t, `(function () {
		var label = document.querySelector('section:nth-of-type(1) label');
		var input = document.querySelector('section:nth-of-type(1) input');
		return !!label && !!input && !!input.id && label.getAttribute('for') === input.id;
	})()`, &bound)
	assert.True(t, bound, "the comment box has no label bound to it")
}

// TestApprovingEmitsASignalTheServiceWouldAccept is the assertion the whole
// design turns on, made by clicking.
//
// The static test forbids a string literal in the bridge; this one takes the
// message the bridge actually produced in a browser, strictly unmarshals it into
// the request type the server enforces, validates it, and compares its params to
// the golden's, field for field. The golden stops being a hand-written
// approximation of what the card sends and becomes what the card sent.
func TestApprovingEmitsASignalTheServiceWouldAccept(t *testing.T) {
	harness := newCardHarness(t)

	run := waitingRun()
	harness.deliver(t, run)
	harness.untilInView(t, "document.querySelectorAll('section').length === 2",
		"the delivered tool result never rendered")

	// Typed into the box, then clicked, in that order: the card reads the
	// comment at click time and a card that read it at render time would send an
	// empty one.
	harness.inView(t, `(function () {
		document.querySelector('section:nth-of-type(1) input').value = 'receipts check out';
		return true;
	})()`, nil)
	harness.inView(t, "document.querySelector('section:nth-of-type(1) .approve').click(), true", nil)

	harness.page.until(t, harness.ctx, 0, "window.__calls().length === 1",
		"clicking approve produced no tools/call")

	calls := harness.calls(t)
	require.Len(t, calls, 1)
	assert.Equal(t, "2.0", calls[0].JSONRPC)
	assert.Equal(t, "tools/call", calls[0].Method)
	assert.Positive(t, calls[0].ID, "the call went out without an id, so no answer could reach it")

	request := strictSignalRequest(t, calls[0].Params)

	// The coordinates are the delivered run's, which is the claim a static scan
	// can only approximate: a card that had been built with a run id of its own
	// would still read `gate.workflowId` and still send the wrong workload.
	assert.Equal(t, run.GetWorkflowId(), request.GetWorkflowId(),
		"the signal addresses a workflow other than the one the host delivered")
	assert.Equal(t, run.GetProgress().GetPendingWaits()[0].GetSignalName(), request.GetName())
	assert.Empty(t, request.GetRunId(),
		"the card pinned its signal to one run; a gate answered after a Continue-As-New would be "+
			"refused, and the schema says an approver is approving the workload")
	assert.True(t, request.GetPayload().GetNamedValues()["approved"].GetLiteral().GetBoolValue())
	assert.Equal(t, "receipts check out",
		request.GetPayload().GetNamedValues()["comment"].GetLiteral().GetStringValue())

	assertMatchesGoldenCall(t, "approve.json", calls[0])

	// In flight, before the server has said anything: both decisions on that
	// gate are disabled, so the gate cannot be answered twice while an answer is
	// outstanding.
	var disabled []bool
	harness.inView(t, "Array.from(document.querySelectorAll('section:nth-of-type(1) button')).map(function (b) { return b.disabled; })", &disabled)
	assert.Equal(t, []bool{true, true}, disabled,
		"the decision buttons stayed live while a decision was in flight")

	assert.Contains(t, harness.text(t, "#status"), "Approving manager-approved",
		"nothing was announced to a screen reader while the decision was in flight")

	// And the server accepts it.
	harness.respond(t, calls[0].ID, map[string]any{
		"content": []any{map[string]any{"type": "text", "text": "{}"}},
		"isError": false,
	})

	harness.untilInView(t, "document.getElementById('status').textContent === 'Approved manager-approved.'",
		"the card never announced the approval it was told succeeded")

	harness.inView(t, "Array.from(document.querySelectorAll('section:nth-of-type(1) button')).map(function (b) { return b.disabled; })", &disabled)
	assert.Equal(t, []bool{true, true}, disabled,
		"the decision buttons came back after a decision the server accepted, so the gate could be "+
			"answered twice")

	// Exactly one call, still: a success is not a reason to send another.
	assert.Len(t, harness.calls(t), 1)
}

// TestRejectingEmitsTheOtherGoldenCall is the same walk down the other button,
// which is where the payload differs and where an empty comment is left out
// rather than sent as "".
func TestRejectingEmitsTheOtherGoldenCall(t *testing.T) {
	harness := newCardHarness(t)

	harness.deliver(t, waitingRun())
	harness.untilInView(t, "document.querySelectorAll('section').length === 2",
		"the delivered tool result never rendered")

	harness.inView(t, "document.querySelector('section:nth-of-type(1) .reject').click(), true", nil)
	harness.page.until(t, harness.ctx, 0, "window.__calls().length === 1",
		"clicking reject produced no tools/call")

	calls := harness.calls(t)
	require.Len(t, calls, 1)

	request := strictSignalRequest(t, calls[0].Params)
	assert.False(t, request.GetPayload().GetNamedValues()["approved"].GetLiteral().GetBoolValue(),
		"approve and reject do not differ in the payload they send")
	assert.NotContains(t, request.GetPayload().GetNamedValues(), "comment",
		"an untyped empty comment travels as a value; a comment nobody wrote is not a comment")

	assertMatchesGoldenCall(t, "reject.json", calls[0])
}

// TestARefusedSignalReEnablesTheDecisionAndSaysSo.
//
// The failure this exists for is a card that disables its buttons on click and
// leaves them disabled when the server refuses: the gate is still open, the
// person is still the approver, and the only way back is to reload a frame they
// may not be able to reload. Nothing textual can see it, because the re-enabling
// is a callback on a promise rejection path.
func TestARefusedSignalReEnablesTheDecisionAndSaysSo(t *testing.T) {
	harness := newCardHarness(t)

	harness.deliver(t, waitingRun())
	harness.untilInView(t, "document.querySelectorAll('section').length === 2",
		"the delivered tool result never rendered")

	harness.inView(t, "document.querySelector('section:nth-of-type(1) .approve').click(), true", nil)
	harness.page.until(t, harness.ctx, 0, "window.__calls().length === 1",
		"clicking approve produced no tools/call")

	calls := harness.calls(t)
	require.Len(t, calls, 1)

	// What the signal policy actually answers with when it refuses an approver:
	// a tool result marked as an error, not a transport failure.
	harness.respond(t, calls[0].ID, map[string]any{
		"content": []any{map[string]any{
			"type": "text",
			"text": "signal manager-approved refused: the starter of a run may not approve it",
		}},
		"isError": true,
	})

	// The count is part of the predicate, and it is the whole point of writing it
	// this way. `every` over an empty list is true, so a card that answered a
	// refusal by removing its decisions altogether would satisfy a bare `every`
	// immediately: the person deciding would be left with a gate that is still
	// open and no way to answer it, and the test would call that a pass. Both
	// buttons have to be there, and both have to be live.
	harness.untilInView(t,
		"(function () {"+
			" var buttons = document.querySelectorAll('section:nth-of-type(1) button');"+
			" return buttons.length === 2 && Array.from(buttons).every(function (b) { return !b.disabled; });"+
			"})()",
		"the two decision buttons never came back, live, after the server refused the signal")

	status := harness.text(t, "#status")
	assert.Contains(t, status, "The server refused it",
		"the refusal was not announced, so the live region still says the decision is in flight")
	assert.Contains(t, status, "the starter of a run may not approve it",
		"the reason the server gave was dropped, leaving the person deciding with nothing to act on")

	// Still exactly one call: a refusal is not retried on its own.
	assert.Len(t, harness.calls(t), 1)
}

// TestAMessageFromANeighbouringFrameIsIgnored is the negative direction, which
// is the one that matters: not that the host can reach the card, but that
// somebody else cannot.
//
// The frame is sandboxed and its origin is opaque, so an origin comparison in
// the card would compare nothing; what is checkable is the sender. A neighbour
// frame in the same host page sends a well-formed tool-result naming a different
// workload, and the card must not render it, which is to say must not become
// willing to sign it.
//
// The proof that "nothing happened" happened is a barrier rather than a sleep,
// and the barrier is deliberately not a second delivery from the host. A real
// delivery rewrites the very state a spoof would have changed, so a card that
// accepted the spoof and was then corrected by the host looks exactly like a
// card that refused it; and the two travel different window-to-window channels,
// which orders neither against the other. The barrier here is a message from the
// neighbour itself: same channel as the spoof, therefore delivered after it, and
// carrying nothing the card would act on, therefore unable to repair the state
// under test. When it has arrived, whatever the card did with the spoof it has
// already done.
func TestAMessageFromANeighbouringFrameIsIgnored(t *testing.T) {
	harness := newCardHarness(t)

	harness.deliver(t, waitingRun())
	harness.untilInView(t, "document.querySelectorAll('section').length === 2",
		"the delivered tool result never rendered")

	harness.watchNeighbourSentinels(t)

	spoofed := waitingRun()
	spoofed.WorkflowId = "attacker-controlled-workload"
	spoofed.Progress.PendingWaits = spoofed.Progress.PendingWaits[:1]
	spoofed.Progress.PendingWaits[0].SignalName = "release-funds"
	harness.spoof(t, spoofed)

	harness.neighbourSentinel(t, "after-the-spoof")

	// The card is still showing the run its host delivered, which is the whole
	// claim: the spoof has been through the card's listener and changed nothing.
	assert.Equal(t, "expense-report-4417", harness.text(t, "#run-workflow"),
		"a frame that is not the card's host changed the workload the card displays, and therefore "+
			"the workload its approve button would sign")

	var headings []string
	harness.inView(t, "Array.from(document.querySelectorAll('section h2')).map(function (h) { return h.textContent; })", &headings)
	assert.Equal(t, []string{"manager-approved", "update"}, headings,
		"a neighbouring frame replaced the gates on offer")

	// The host itself is still heard, which is the other half of a sender check:
	// a card that refused everything would pass every assertion above and be
	// useless.
	current := waitingRun()
	current.Starter = "second-delivery@example.com"
	harness.deliver(t, current)
	harness.untilInView(t,
		"document.getElementById('run-starter').textContent === 'second-delivery@example.com'",
		"the card stopped accepting deliveries from its own host")

	// And the click that follows still addresses the real run.
	harness.inView(t, "document.querySelector('section:nth-of-type(1) .approve').click(), true", nil)
	harness.page.until(t, harness.ctx, 0, "window.__calls().length === 1",
		"clicking approve produced no tools/call")

	calls := harness.calls(t)
	require.Len(t, calls, 1)
	request := strictSignalRequest(t, calls[0].Params)
	assert.Equal(t, "expense-report-4417", request.GetWorkflowId())
	assert.Equal(t, "manager-approved", request.GetName())
}

// strictSignalRequest puts the params of a card's tools/call through the schema
// the server enforces, and is the one place that check is written.
//
// Shared with the golden test next door on purpose. The goldens prove the shape
// in testdata is one this service accepts; this file proves the card builds that
// shape in a browser. Two copies of the check would let the two claims drift
// apart, which is exactly the failure both are guarding against.
//
// DiscardUnknown stays false, exactly as [mcpHandler] leaves it: the tool refuses
// a field the schema does not have, so a message carrying one is a message the
// server rejects.
func strictSignalRequest(t *testing.T, params json.RawMessage) *v1.SignalRequest {
	t.Helper()

	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	require.NoError(t, json.Unmarshal(params, &call))

	require.Equal(t, mcpToolName("Signal"), call.Name,
		"the card calls a tool other than the one that delivers a signal")

	var request v1.SignalRequest
	require.NoError(t, protojson.Unmarshal(call.Arguments, &request),
		"the card's arguments are not a SignalRequest this surface would accept")
	require.NoError(t, v1.Validate(&request),
		"the card's arguments do not satisfy the schema's own rules")

	return &request
}

// assertMatchesGoldenCall compares a call the card made in a browser against the
// golden message in testdata, on everything but the id.
//
// The id is left out because it is a per-connection counter and the goldens
// record one particular ordering. Everything that decides what the server does
// is compared exactly.
func assertMatchesGoldenCall(t *testing.T, name string, call toolCall) {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "approval-card", name))
	require.NoError(t, err)

	var golden struct {
		Params json.RawMessage `json:"params"`
	}
	require.NoError(t, json.Unmarshal(raw, &golden))

	var want, got any
	require.NoError(t, json.Unmarshal(golden.Params, &want))
	require.NoError(t, json.Unmarshal(call.Params, &got))

	assert.Equal(t, want, got,
		"the message the card built in a browser is not the golden %s that was written for it; the "+
			"golden is meant to be what this card sends, not an approximation of it", name)
}
