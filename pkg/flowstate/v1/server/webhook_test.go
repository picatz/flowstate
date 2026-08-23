package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The receiver, end to end: a real delivery over HTTP becomes a real run.
//
// Four claims this slice exists for, each written so that it fails if the
// behaviour regresses rather than merely if the code stops compiling:
//
//   - A genuine delivery starts a run, with the payload's values bound to the
//     workflow's declared inputs.
//   - A redelivery does not start a second one, including when the two arrive at
//     the same instant, which is the case a time-window dedupe cannot see.
//   - An unverifiable delivery is refused, and refused in a way that does not tell
//     the sender whether the webhook they addressed even exists.
//   - An oversized body is refused *before* it is read into memory, which is
//     asserted by counting what the handler consumed rather than by trusting a
//     status code.

// webhookSecret is the signing key every delivery in this file is signed with.
const webhookSecret = "whsec_test_receiver"

// keyProvider is the deployment's secret backend: one signing key per tenant,
// recording which tenant it was asked in.
//
// A provider rather than a [secrets.Resolver], because a resolver is already
// scoped to a namespace and the namespace is exactly what these tests are about.
// The receiver is handed the [secrets.Store] and scopes it itself, so `seen` is
// the record of which tenant's secrets a receiver actually reached — the negative
// direction, which a resolver standing in for a whole backend cannot express.
type keyProvider struct {
	// keys is namespace -> the signing key held in it. A namespace absent here
	// holds no key, which is what a tenant that never configured one looks like.
	keys map[string]string

	// err, when set, is what the backend answers regardless of tenant.
	err error

	mu   sync.Mutex
	seen []string
}

func (p *keyProvider) Scheme() string { return "env" }

func (p *keyProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	p.mu.Lock()
	p.seen = append(p.seen, req.Namespace)
	p.mu.Unlock()

	if p.err != nil {
		return secrets.Secret{}, p.err
	}

	value, held := p.keys[req.Namespace]
	if !held {
		return secrets.Secret{}, fmt.Errorf("%w: no signing key in namespace %q",
			secrets.ErrNotFound, req.Namespace)
	}

	return secrets.NewSecret(req.Ref, value), nil
}

// namespaces reports every tenant this backend was asked to resolve in.
func (p *keyProvider) namespaces() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.seen)
}

// storeOf wraps a backend in the store a deployment hands the receiver.
func storeOf(t *testing.T, provider *keyProvider) *secrets.Store {
	t.Helper()

	store, err := secrets.NewStore(provider)
	require.NoError(t, err)

	return store
}

// keyStore holds one signing key, in the unnamed tenant most of this file serves
// under: a single-tenant deployment, which is what an empty namespace means.
func keyStore(t *testing.T, value string) *secrets.Store {
	t.Helper()

	return storeOf(t, &keyProvider{keys: map[string]string{"": value}})
}

// orderWebhookWorkflow is the served specification: a signature with an `int` in
// it, bound by a webhook that signs with the generic scheme.
func orderWebhookWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "order-webhook",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
			Name: "storefront",
			Verify: map[string]*v1.Value{
				v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "STOREFRONT_WEBHOOK_SECRET"},
				}},
			},
			IdempotencyKey: v1.NewExpr(`event.body.id`),
			Arguments: map[string]*v1.Value{
				"order_id": v1.NewExpr(`event.body.order.id`),
				"amount":   v1.NewExpr(`event.body.order.total_cents`),
			},
		}}},
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "order_id", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
			{Name: "amount", Type: v1.InputDeclaration_TYPE_INT, Required: true},
		},
		Steps: []*v1.Node{{
			Id:   "record",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`inputs.order_id + " for " + string(inputs.amount)`)},
		}},
	}
}

// deliveryBody is one event's payload. The id is what the trigger dedupes on.
func deliveryBody(event string) string {
	return fmt.Sprintf(`{"id":%q,"order":{"id":"ord_H1x9","total_cents":4200}}`, event)
}

// newReceiver builds a receiver over one workflow, with no Temporal behind it.
//
// Enough for every claim decided before a run would start — routing, bounds,
// verification — and deliberately cheap: those tests should not need a cluster,
// and would be skipped under -short if they did.
func newReceiver(t *testing.T, opts ...server.WebhookOption) *server.WebhookReceiver {
	t.Helper()

	receiver, err := mustNew(t, nil).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret), opts...)
	require.NoError(t, err)

	return receiver
}

// deliver POSTs a signed delivery and returns the response.
func deliver(t *testing.T, handler http.Handler, path, body string, sign func(string) string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(v1.WebhookSignatureHeader, sign(body))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	return recorder.Result()
}

// signed is the signature a genuine sender computes.
func signed(body string) string {
	return v1.SignWebhookBody(secrets.NewSecret(secrets.NewRef("env", "k"), webhookSecret), []byte(body))
}

// forged is a signature computed under a key the deployment does not hold.
func forged(body string) string {
	return v1.SignWebhookBody(secrets.NewSecret(secrets.NewRef("env", "k"), "not-the-key"), []byte(body))
}

// readAccepted decodes what the receiver answered an accepted delivery with.
func readAccepted(t *testing.T, resp *http.Response) server.AcceptedDelivery {
	t.Helper()

	var accepted server.AcceptedDelivery
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&accepted))

	return accepted
}

// TestADeliveryStartsARun is the slice, end to end: HTTP in, run out, with the
// payload's values in the run's inputs and provenance recorded on it.
func TestADeliveryStartsARun(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	body := deliveryBody("evt_start")
	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "a genuine delivery was not accepted")

	accepted := readAccepted(t, resp)
	require.False(t, accepted.Joined, "the first delivery of an event reported joining an existing run")
	require.NotEmpty(t, accepted.RunID)

	// The values the payload carried, having travelled the whole path: mapped by
	// `with:`, bound against `inputs:`, submitted, and computed by a step. `4200`
	// arriving as a float would have been refused by the `int` declaration, which
	// is why the assertion is on the rendered value rather than on the inputs.
	var out v1.Workflow_StepOutputs
	require.NoError(t, temporal.GetWorkflow(t.Context(), accepted.WorkflowID, accepted.RunID).
		Get(t.Context(), &out))
	assert.Equal(t, "ord_H1x9 for 4200",
		out.GetStepValues()["record"].GetNamedValues()[v1.ValueOutput].GetLiteral().GetStringValue())

	// Provenance: which trigger, which delivery, which principal. Read off the
	// run rather than off the response, because the response is what a sender
	// sees and this is what an operator has afterwards.
	described, err := temporal.DescribeWorkflowExecution(t.Context(), accepted.WorkflowID, accepted.RunID)
	require.NoError(t, err)

	memo := described.GetWorkflowExecutionInfo().GetMemo().GetFields()
	assert.Equal(t, `"webhook:storefront"`, string(memo["flowstate.trigger"].GetData()),
		"the run does not record which trigger started it")
	assert.Equal(t, `"`+accepted.DeliveryID+`"`, string(memo["flowstate.delivery"].GetData()),
		"the run does not record which delivery started it")
	assert.Equal(t, `"flowstate://webhook#order-webhook/storefront"`, string(memo["flowstate.starter"].GetData()),
		"the run does not record the trigger as its principal")

	// And the delivery id is not the idempotency key: the usual key is a
	// signature header, and a memo is durable and broadly readable.
	assert.NotContains(t, string(memo["flowstate.delivery"].GetData()), "evt_start",
		"the raw idempotency key was written into durable history")
}

// TestARedeliveryDoesNotStartASecondRun is the at-least-once half of the design:
// a provider retrying an event it already delivered must not produce a second run.
func TestARedeliveryDoesNotStartASecondRun(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	body := deliveryBody("evt_retried")

	first := readAccepted(t, deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed))
	require.False(t, first.Joined)

	second := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)
	require.Equal(t, http.StatusOK, second.StatusCode,
		"a redelivery was answered as a newly accepted one")

	joined := readAccepted(t, second)
	assert.True(t, joined.Joined, "a redelivery did not report joining the run its event already started")
	assert.Equal(t, first.WorkflowID, joined.WorkflowID)
	assert.Equal(t, first.RunID, joined.RunID,
		"a redelivery produced a second run, so every provider retry is a duplicate run")

	// After the run has finished, too, which is the window a receiver thinking
	// only about concurrent retries gets wrong: a provider redelivering an hour
	// later would otherwise start a fresh run of an event already processed.
	var out v1.Workflow_StepOutputs
	require.NoError(t, temporal.GetWorkflow(t.Context(), first.WorkflowID, first.RunID).Get(t.Context(), &out))

	afterward := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)
	require.Equal(t, http.StatusOK, afterward.StatusCode)
	assert.Equal(t, first.RunID, readAccepted(t, afterward).RunID,
		"a redelivery after the run completed started a second run")
}

// TestConcurrentRedeliveriesStartOneRun is the claim a dedupe has to make and the
// one a local table with a time window cannot: two deliveries of one event
// arriving at the same instant produce one run.
func TestConcurrentRedeliveriesStartOneRun(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	const arrivals = 8

	body := deliveryBody("evt_storm")

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []server.AcceptedDelivery
	)
	start := make(chan struct{})
	for range arrivals {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-start
			resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)
			if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
				return
			}

			var accepted server.AcceptedDelivery
			if err := json.NewDecoder(resp.Body).Decode(&accepted); err != nil {
				return
			}

			mu.Lock()
			results = append(results, accepted)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	require.Len(t, results, arrivals, "a delivery in the storm was neither accepted nor joined")

	started := 0
	for _, accepted := range results {
		assert.Equal(t, results[0].RunID, accepted.RunID,
			"simultaneous deliveries of one event landed on different runs")
		if !accepted.Joined {
			started++
		}
	}
	assert.Equal(t, 1, started,
		"%d of %d simultaneous deliveries of one event each started a run", started, arrivals)
}

// TestAnUnverifiableDeliveryIsRefused is the fail-closed direction, with the
// negative half that matters: nothing was started.
func TestAnUnverifiableDeliveryIsRefused(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	body := deliveryBody("evt_forged")

	refused := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, forged)
	require.Equal(t, http.StatusNotFound, refused.StatusCode, "a forged delivery was accepted")

	// And it started nothing. Asserted by delivering the *same event* genuinely
	// afterwards: the run is new, which it could not be had the forged one
	// already created it under the same idempotency key.
	accepted := readAccepted(t, deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed))
	assert.False(t, accepted.Joined,
		"a genuine delivery joined a run, so the forged delivery of the same event had started one")
}

// TestADeliveryIsOneJSONDocumentAndNothingAfterIt is the parser-strictness half of
// "bound anything that consumes untrusted input", where what is bounded is the
// grammar rather than a resource.
//
// `Decoder.Decode` reads one value and stops, so a body holding a document plus
// anything else decoded as the document and discarded the rest — and the run was
// then started from a prefix, which can say something other than the payload as a
// whole says. Every case here carries a *valid* first document, because a receiver
// that only refuses malformed JSON passes all of them.
func TestADeliveryIsOneJSONDocumentAndNothingAfterIt(t *testing.T) {
	t.Parallel()

	// Over a cluster that is never reached: a body this refuses is refused before
	// a run is attempted, and the whitespace cases below have to get *past* that
	// point to prove they were not refused, which means having somewhere to fail
	// afterwards instead.
	receiver, err := mustNew(t, unreachableTemporal(t)).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	first := deliveryBody("evt_prefix")

	for name, body := range map[string]string{
		"a second document":       first + first,
		"a trailing scalar":       first + ` 12`,
		"trailing arbitrary text": first + ` not-json`,
		"a trailing NUL":          first + "\x00",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Signed genuinely: the point is that a sender holding the key still
			// cannot get a run started from a prefix of what they sent.
			resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"a body carrying more than one JSON document was accepted, so the run was started "+
					"from the first document alone")
		})
	}

	// And what is still one document: trailing whitespace is not a second value,
	// and refusing it would refuse most senders' pretty-printed payloads.
	for name, body := range map[string]string{
		"trailing whitespace": first + "\n\t ",
		"leading whitespace":  "  " + first,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)
			assert.NotEqual(t, http.StatusBadRequest, resp.StatusCode,
				"a single document surrounded by whitespace was refused as more than one document")
		})
	}
}

// TestARefusalDoesNotSayWhichRefusalItIs is the part of fail-closed that is about
// what a refusal *tells* somebody.
//
// An unknown workflow, an unknown trigger and a wrong signature are three
// different facts, and answering them differently hands a prober a way to
// enumerate which webhooks a deployment serves and to learn when they have found
// a real one.
func TestARefusalDoesNotSayWhichRefusalItIs(t *testing.T) {
	t.Parallel()

	receiver := newReceiver(t)
	body := deliveryBody("evt_probe")

	answers := map[string]*http.Response{
		"a wrong signature":  deliver(t, receiver, "/webhooks/order-webhook/storefront", body, forged),
		"an unknown trigger": deliver(t, receiver, "/webhooks/order-webhook/nope", body, signed),
		"an unknown workflow": deliver(t, receiver, "/webhooks/no-such-workflow/storefront",
			body, signed),
	}

	var reference string
	for name, resp := range answers {
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, "%s answered with its own status code", name)

		payload, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		if reference == "" {
			reference = string(payload)
			continue
		}
		assert.Equal(t, reference, string(payload), "%s answered with its own sentence", name)
	}
}

// countingReader hands out bytes forever, counting what was taken.
//
// The instrument for the bound: an unbounded receiver reads until this stops
// giving, so the count is the difference between a cap that is applied and a cap
// that is described.
type countingReader struct {
	limit int
	read  int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.read >= r.limit {
		return 0, io.EOF
	}
	n := min(len(p), r.limit-r.read)
	for i := range n {
		p[i] = 'x'
	}
	r.read += n

	return n, nil
}

// TestAnOversizedBodyIsRefusedBeforeItIsReadIntoMemory asserts the bound where it
// matters, which is on the stream rather than on the status code.
//
// A receiver that read the whole body and then measured it would pass a test that
// only checked for a 413, while having already allocated whatever the sender sent.
// This one fails that receiver: the count is what the handler actually consumed.
func TestAnOversizedBodyIsRefusedBeforeItIsReadIntoMemory(t *testing.T) {
	t.Parallel()

	receiver := newReceiver(t)

	// Eight times the bound on offer. A handler with no cap consumes all of it.
	body := &countingReader{limit: 8 * v1.MaxWebhookPayloadBytes}

	req := httptest.NewRequest(http.MethodPost, "/webhooks/order-webhook/storefront", body)
	req.Header.Set(v1.WebhookSignatureHeader, "sha256=00")

	recorder := httptest.NewRecorder()
	receiver.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, recorder.Code,
		"an oversized delivery was not refused")

	// The cap plus one read's slack: http.MaxBytesReader lets the final read
	// cross the limit before it reports the error, and never more than that.
	assert.LessOrEqual(t, body.read, v1.MaxWebhookPayloadBytes+4096,
		"the receiver read %d bytes of an %d byte body, so the bound is applied after the read rather "+
			"than on the stream", body.read, body.limit)
}

// TestAReceiverRefusesAKeyItCannotResolve is the "decided when configuration
// loads" half: a deployment that cannot satisfy a declared scheme fails to start
// rather than refusing every delivery at three in the morning.
func TestAReceiverRefusesAKeyItCannotResolve(t *testing.T) {
	t.Parallel()

	_, err := mustNew(t, nil).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()},
		storeOf(t, &keyProvider{err: fmt.Errorf("no such secret")}))
	require.Error(t, err, "a receiver started holding a webhook whose signing key it cannot resolve")
	assert.Contains(t, err.Error(), "storefront", "the refusal does not name the webhook at fault")
}

// TestAReceiverRefusesAWorkflowItCannotServe covers the other load-time refusals,
// including the one that would otherwise be a request-time surprise: a trigger
// naming a scheme this build cannot check.
func TestAReceiverRefusesAWorkflowItCannotServe(t *testing.T) {
	t.Parallel()

	unknownScheme := orderWebhookWorkflow()
	unknownScheme.Triggers.Webhooks[0].Verify = map[string]*v1.Value{
		"paypal_v2": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "K"}}},
	}

	noTriggers := orderWebhookWorkflow()
	noTriggers.Triggers = nil

	for name, workflows := range map[string][]*v1.Workflow{
		"a scheme this build cannot verify": {unknownScheme},
		"a workflow declaring no webhooks":  {noTriggers},
		"two workflows under one name":      {orderWebhookWorkflow(), orderWebhookWorkflow()},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := mustNew(t, nil).NewWebhookReceiver(t.Context(), "", workflows,
				keyStore(t, webhookSecret))
			require.Error(t, err, "the receiver accepted a configuration it cannot serve")
		})
	}
}

// TestAReceiverPrintsNoSigningKey is the containment shape, in the places a
// [secrets.Secret] would leak through if it were held as a string: a receiver
// holds a map of resolved keys, several structs deep.
func TestAReceiverPrintsNoSigningKey(t *testing.T) {
	t.Parallel()

	receiver := newReceiver(t)

	for _, rendered := range []string{
		fmt.Sprintf("%v", receiver),
		fmt.Sprintf("%+v", receiver),
		fmt.Sprintf("%#v", receiver),
		fmt.Sprintf("%v", []*server.WebhookReceiver{receiver}),
		fmt.Sprintf("%v", struct{ R *server.WebhookReceiver }{receiver}),
	} {
		assert.NotContains(t, rendered, webhookSecret,
			"a rendering of the receiver carried a signing key")
	}
}

// TestOnlyAPostIsADelivery covers the method check, which is also the one refusal
// that may safely be specific: it says nothing about what exists.
func TestOnlyAPostIsADelivery(t *testing.T) {
	t.Parallel()

	receiver := newReceiver(t)

	req := httptest.NewRequest(http.MethodGet, "/webhooks/order-webhook/storefront", nil)
	recorder := httptest.NewRecorder()
	receiver.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
}

// TestRoutesReportsWhatIsServed pins the startup surface an operator reads.
func TestRoutesReportsWhatIsServed(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"order-webhook/storefront"}, newReceiver(t).Routes())
}

// TestADeliveryTheDeploymentCannotStartIsRetryable is the other half of that
// asymmetry, and the one a sender's retry logic depends on.
//
// A payload that will never bind and a cluster that was briefly unreachable are
// both "verified, and no run" — but a provider must not retry the first and must
// retry the second. Answering both with one status would make a provider either
// hammer a payload that can never work or give up on an outage.
func TestADeliveryTheDeploymentCannotStartIsRetryable(t *testing.T) {
	t.Parallel()

	// A client that dials only when used, at a port nothing answers on: the
	// receiver's own path is exercised exactly as it would be against a cluster
	// that has gone away.
	unreachable, err := client.NewLazyClient(client.Options{
		HostPort: "127.0.0.1:1",
		Logger:   log.NewStructuredLogger(slog.New(slog.DiscardHandler)),
	})
	require.NoError(t, err)

	receiver, err := mustNew(t, unreachable).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront",
		deliveryBody("evt_unreachable"), signed)

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"a delivery this deployment could not start was answered as though the payload were at fault")
	assert.NotEmpty(t, resp.Header.Get("Retry-After"), "a retryable refusal did not say so")

	said, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(said), "127.0.0.1",
		"the refusal described this deployment's own infrastructure to the sender")
}

// TestAVerifiedDeliveryThatDoesNotMapIsReportedHonestly is the asymmetry the
// refusal policy rests on: once a sender has proved they hold the signing key,
// there is nothing left to conceal from them, so a payload that does not satisfy
// the workflow's `inputs:` gets a sentence they can act on.
func TestAVerifiedDeliveryThatDoesNotMapIsReportedHonestly(t *testing.T) {
	t.Parallel()

	receiver := newReceiver(t)

	// A payload missing the field `with:` reaches for.
	body := `{"id":"evt_thin","order":{}}`
	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)

	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)

	said, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(said), "storefront",
		"a delivery that did not map was refused without saying which webhook or why")
}

// webhookOnlyWorkflowWithManualDenied is a deployment-owned workflow that is
// meant to start only from its webhook: `manual: denied` says so, and it also
// declares a webhook trigger so [FlowstateServer.NewWebhookReceiver] admits it.
func webhookOnlyWorkflowWithManualDenied() *v1.Workflow {
	return &v1.Workflow{
		Name:    "break-glass-webhook",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{
			Manual: &v1.ManualTrigger{Denied: true},
			Webhooks: []*v1.WebhookTrigger{{
				Name: "rotate",
				Verify: map[string]*v1.Value{
					v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
						SecretRef: &v1.SecretRef{Scheme: "env", Name: "ROTATE_WEBHOOK_SECRET"},
					}},
				},
				IdempotencyKey: v1.NewExpr(`event.body.id`),
			}},
		},
		Steps: []*v1.Node{{
			Id:   "rotate",
			Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")},
		}},
	}
}

// TestAWebhookServedWorkflowIsTrustedForRun is the P1 this file's receiver setup
// exists to close: a workflow this deployment serves for webhook deliveries is
// deployment-owned in exactly the sense [server.WithTrustedWorkflows] means, so a
// caller naming it through `Run` must be held to *this* copy's `manual: denied`
// rather than to whatever policy their own submitted copy carries — including
// none at all.
func TestAWebhookServedWorkflowIsTrustedForRun(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal)

	_, err := flowstate.NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{webhookOnlyWorkflowWithManualDenied()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	// The same name, submitted with no `manual:` block at all — the shape an
	// attacker who merely knows the workflow's name, and nothing about its
	// deployment-side policy, would submit.
	submitted := webhookOnlyWorkflowWithManualDenied()
	submitted.Triggers = &v1.Triggers{Webhooks: submitted.Triggers.GetWebhooks()}

	_, err = flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: submitted,
		Reason:   "trying to start a webhook-only workload directly",
	}))
	require.Error(t, err, "a webhook-served workflow's manual: denied was bypassed by a caller's own copy")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// breakGlassWebhookWorkflowFor is [webhookOnlyWorkflowWithManualDenied], but
// the reason `manual: denied` refuses a caller carries the tenant's own name
// — which is the fact [TestATrustedWorkflowRegisteredForOneTenantDoesNotReachAnother]
// reads back off each tenant's own refusal to tell the two copies apart, the
// same way an ordinary workflow's steps would.
func breakGlassWebhookWorkflowFor(tenant string) *v1.Workflow {
	workflow := webhookOnlyWorkflowWithManualDenied()
	workflow.Triggers.Manual.AllowedPrincipals = []string{tenant + "-oncall@example.com"}
	workflow.Triggers.Manual.Denied = false
	return workflow
}

// TestATrustedWorkflowRegisteredForOneTenantDoesNotReachAnother is the negative
// direction of the trust boundary [FlowstateServer.trustedWorkflow] implements:
// not merely that a tenant reaches its own registered entry (that is
// [TestAWebhookServedWorkflowIsTrustedForRun]), but that it cannot be
// substituted by a different tenant's entry registered under the identical
// workflow name.
//
// A lookup keyed on name alone could not tell these apart: registering
// "deploy" for team-a and then for team-b would let the second overwrite the
// first, or — worse, if a namespace slipped into the key by concatenation
// rather than as its own field — let a crafted namespace collide with a
// different tenant's name. See CLAUDE.md's own account of the identical
// mistake made twice already, by the env and file secret providers.
func TestATrustedWorkflowRegisteredForOneTenantDoesNotReachAnother(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	store := storeOf(t, &keyProvider{keys: map[string]string{
		"team-a": webhookSecret,
		"team-b": webhookSecret,
	}})

	flowstate := mustNew(t, temporal)

	_, err := flowstate.NewWebhookReceiver(t.Context(), "team-a",
		[]*v1.Workflow{breakGlassWebhookWorkflowFor("team-a")}, store)
	require.NoError(t, err)

	_, err = flowstate.NewWebhookReceiver(t.Context(), "team-b",
		[]*v1.Workflow{breakGlassWebhookWorkflowFor("team-b")}, store)
	require.NoError(t, err)

	// team-b's principal, naming the identical workflow name — with no
	// `manual:` block on the submitted copy at all, so if the lookup ever
	// answered with team-a's registered entry instead of team-b's, this would
	// either be authorized outright (team-b's oncall is not on team-a's
	// allow-list, so team-a's copy would refuse it) or, in the direction that
	// actually matters here, succeed against a copy this tenant never
	// registered and never reviewed.
	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:    "https://issuer.example.com",
		Subject:   "team-b-oncall@example.com",
		Namespace: "team-b",
	})

	started, err := flowstate.Run(ctx, connect.NewRequest(&v1.RunRequest{
		Workflow: breakGlassWebhookWorkflowFor("team-b"),
		Reason:   "rotating team-b's key",
	}))
	require.NoError(t, err, "team-b's own oncall, naming team-b's own registered workflow, was refused")

	got, err := flowstate.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: started.Msg.GetWorkflowId()}))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, err = flowstate.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: started.Msg.GetWorkflowId()}))
		return err == nil && got.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 30*time.Second, 100*time.Millisecond)

	// The direction that is the whole point of this test: team-a's own oncall,
	// calling from team-a's own namespace, naming the identical workflow name.
	// If the trusted lookup ever fell through to *any* entry under this name —
	// a name-only key, or team-b's registration simply overwriting team-a's in
	// the map — this request would be authorized against team-b's
	// `allowed_principals`, which does not name `team-a-oncall@example.com`,
	// and would be refused. Succeeding here is what proves team-a reached its
	// own entry rather than team-b's.
	ctxA := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:    "https://issuer.example.com",
		Subject:   "team-a-oncall@example.com",
		Namespace: "team-a",
	})
	_, err = flowstate.Run(ctxA, connect.NewRequest(&v1.RunRequest{
		Workflow: breakGlassWebhookWorkflowFor("team-a"),
		Reason:   "trying team-a's own workflow",
	}))
	require.NoError(t, err,
		"team-a's own oncall, naming team-a's own registered workflow, was refused — "+
			"which is what team-a reaching team-b's registered copy instead would look like")

	// The direction that actually distinguishes a namespace-scoped key from a
	// name-only one: team-b's oncall, calling from *team-a's* namespace. If
	// the lookup were keyed on name alone, this would reach the one entry
	// registered under "break-glass-webhook" regardless of which tenant
	// registered it last, and — since that entry's allow-list names
	// team-b-oncall — would be authorized. A namespace-scoped key must refuse
	// it: team-a never registered this workflow for this subject.
	ctxBFromNamespaceA := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:    "https://issuer.example.com",
		Subject:   "team-b-oncall@example.com",
		Namespace: "team-a",
	})
	_, err = flowstate.Run(ctxBFromNamespaceA, connect.NewRequest(&v1.RunRequest{
		Workflow: breakGlassWebhookWorkflowFor("team-a"),
		Reason:   "team-b's subject, addressed through team-a's namespace",
	}))
	require.Error(t, err,
		"team-b's allow-listed subject was authorized against team-a's namespace, so the lookup "+
			"is not actually scoped by namespace")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAFailedWebhookReceiverGrantsNoTrust is the other half of trust
// registration's correctness: a call to [FlowstateServer.NewWebhookReceiver]
// admits every workflow it was given or none of them, atomically as far as
// the trusted set is concerned. Registering trust as each workflow is
// checked and returning the error from a later one would leave the
// server-wide trusted set holding entries for a receiver that was never
// created — this deployment's own `Run` would then substitute a
// specification for a workflow no webhook route exists for.
func TestAFailedWebhookReceiverGrantsNoTrust(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal)

	valid := webhookOnlyWorkflowWithManualDenied()

	// Same name as valid, so the receiver refuses the whole call on "declared
	// twice" — see [TestAReceiverRefusesAWorkflowItCannotServe] — after valid
	// has already passed its own checks. If trust were granted as the loop
	// went rather than after it succeeded, valid would already be trusted by
	// the time this constructor returns its error.
	duplicate := webhookOnlyWorkflowWithManualDenied()

	_, err := flowstate.NewWebhookReceiver(t.Context(), "",
		[]*v1.Workflow{valid, duplicate}, keyStore(t, webhookSecret))
	require.Error(t, err, "a receiver configured with a workflow declared twice was accepted")

	// The workflow the failed call's first entry would have registered,
	// submitted with no `manual:` at all. If the failed constructor left it
	// trusted, this would be silently replaced by the deployment's `manual:
	// denied` copy and refused; since nothing was ever actually served for
	// it, it must be treated as an ordinary, ungoverned submission instead —
	// which this workflow's own (missing) policy permits.
	submitted := webhookOnlyWorkflowWithManualDenied()
	submitted.Triggers = &v1.Triggers{Webhooks: submitted.Triggers.GetWebhooks()}

	_, err = flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: submitted,
		Reason:   "a workflow a failed receiver call must not have trusted",
	}))
	require.NoError(t, err,
		"a workflow from a NewWebhookReceiver call that ultimately failed was trusted anyway")
}
