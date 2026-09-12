package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// blockingEmitter holds every record until it is released, and says when one
// arrived. It stands in for the sink a required recorder writes through
// synchronously — an exporter with a round trip in it — without needing one.
type blockingEmitter struct {
	arrived chan struct{}
	release chan struct{}

	// contexts carries the context each record was written under, so a test can
	// assert what happened to it while the record was still in the sink.
	contexts chan context.Context
}

func newBlockingEmitter() *blockingEmitter {
	return &blockingEmitter{
		arrived:  make(chan struct{}, 16),
		release:  make(chan struct{}),
		contexts: make(chan context.Context, 16),
	}
}

func (e *blockingEmitter) Emit(ctx context.Context, _ *v1.AuditRecord) error {
	e.contexts <- ctx
	e.arrived <- struct{}{}
	<-e.release

	return nil
}

// TestARefusalIsAnsweredBeforeItIsRecorded is the timing half of
// [TestARefusalDoesNotSayWhichRefusalItIs] (#1119).
//
// That test pins the answer: an unknown route and a configured route with a bad
// signature get the same status and the same sentence, and this handler spends
// the same verification work on both, so the two are indistinguishable by
// timing too. The refusal record was not part of that equality. It is written
// once per route and class per interval, so a path naming a route this
// deployment serves can reach the sink where an already-primed unknown path
// does not — and under a required recorder that sink is synchronous, so the
// difference lands in the time before the response.
//
// The assertion is the ordering that removes the channel, rather than a
// measurement of it: with the sink held open, the sender still has its complete
// refusal. A timing test would be a benchmark; this is exact.
func TestARefusalIsAnsweredBeforeItIsRecorded(t *testing.T) {
	t.Parallel()

	emitter := newBlockingEmitter()

	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(emitter))
	require.NoError(t, err)

	receiver, err := mustNew(t, nil, server.WithAudit(recorder)).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	// A real server and a real client, because the claim is about what reaches
	// the sender rather than about what a handler did to a recorder.
	srv := httptest.NewServer(receiver)
	defer srv.Close()

	// Released before the server is closed, since defers unwind in reverse:
	// httptest waits for outstanding handlers, and a handler still inside the
	// sink is one.
	defer close(emitter.release)

	body := deliveryBody("evt_probe")

	for name, path := range map[string]string{
		"a path naming no route":                      "/webhooks/no-such-workflow/nope",
		"a path naming a route the deployment serves": "/webhooks/order-webhook/storefront",
	} {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				srv.URL+path, strings.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(v1.WebhookSignatureHeader, forged(body))

			// The sink is blocked for the whole of this, so a refusal that
			// recorded before answering cannot complete. Bounded rather than
			// left to the package timeout, so that ordering shows up as this
			// assertion failing rather than as a hang somebody has to diagnose.
			client := srv.Client()
			client.Timeout = 15 * time.Second

			resp, err := client.Do(req)
			require.NoError(t, err, "the sender never got its refusal: the record was written first")
			defer resp.Body.Close()

			require.Equal(t, http.StatusNotFound, resp.StatusCode)

			// And the record is still on its way, which is what makes the
			// success above the ordering rather than a sink that was fast.
			select {
			case <-emitter.arrived:
			case <-time.After(10 * time.Second):
				t.Fatal("the refusal was answered but never recorded")
			}
		})
	}
}

// TestAPostAnswerRecordSurvivesTheSenderClosing is the other half of the
// ordering above, and the reason answering first is safe.
//
// Answering first moves the record after a point the *sender* controls: Go arms
// its background close-detection read once the body has been consumed, so a
// client that reads the flushed refusal and closes cancels the request context
// while the record is still on its way to the sink. The ledger has already
// spent the interval's slot by then, so a prober who aborts every delivery
// would suppress the whole window for that route and class rather than one
// record — a refusal trail the prober switches off by probing.
//
// So the record is written under a context carrying the request's values and
// not its cancellation. Asserted at the sink, on the context the record is
// actually written under, after the server has been *seen* to notice the close:
// a wall-clock wait would prove nothing about a race it happened to win.
func TestAPostAnswerRecordSurvivesTheSenderClosing(t *testing.T) {
	t.Parallel()

	emitter := newBlockingEmitter()

	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(emitter))
	require.NoError(t, err)

	receiver, err := mustNew(t, nil, server.WithAudit(recorder)).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	// The handler's own request context, watched from outside it: this is the
	// positive signal that the server has observed the sender close, which is
	// what makes the assertion below an ordering rather than a race.
	closed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		go func() {
			<-req.Context().Done()
			close(closed)
		}()
		receiver.ServeHTTP(w, req)
	}))
	defer srv.Close()

	// Released before the server is closed, since defers unwind in reverse.
	defer close(emitter.release)

	body := deliveryBody("evt_abort")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		srv.URL+"/webhooks/no-such-workflow/nope", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(v1.WebhookSignatureHeader, forged(body))

	client := srv.Client()
	client.Timeout = 15 * time.Second

	// Returns when the flushed refusal's headers arrive, which is the sender
	// having its answer — the handler is still inside the record.
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	var recordCtx context.Context
	select {
	case recordCtx = <-emitter.contexts:
	case <-time.After(10 * time.Second):
		t.Fatal("the refusal was answered but never reached the sink")
	}

	// The sender closes, and the server is seen to notice.
	cancel()
	_ = resp.Body.Close()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the server never noticed the sender close, so this fixture proves nothing")
	}

	// Not cancelled, rather than not errored: the record carries a deadline of
	// this server's own, and the claim is only that the sender is not the one
	// who ends it.
	require.NotErrorIs(t, recordCtx.Err(), context.Canceled,
		"a sender who read the refusal and closed cancelled the record written about them")
}
