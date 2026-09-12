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
}

func newBlockingEmitter() *blockingEmitter {
	return &blockingEmitter{arrived: make(chan struct{}, 16), release: make(chan struct{})}
}

func (e *blockingEmitter) Emit(context.Context, *v1.AuditRecord) error {
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
