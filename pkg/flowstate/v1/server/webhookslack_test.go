package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

const slackRoute = "/webhooks/slack-commands/slack"

// slackWorkflow is served under the Slack scheme and maps a form field, so one
// workflow proves both halves: a slash command's fields and an interactive
// component's unwrapped `payload`.
func slackWorkflow(arguments map[string]*v1.Value) *v1.Workflow {
	return &v1.Workflow{
		Name:    "slack-commands",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
			Name: "slack",
			Verify: map[string]*v1.Value{
				v1.WebhookSchemeSlack: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "SLACK_SIGNING_SECRET"},
				}},
			},
			IdempotencyKey: v1.NewExpr(`event.body.trigger_id`),
			Arguments:      arguments,
		}}},
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "what", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
		},
		Steps: []*v1.Node{{Id: "record", Kind: &v1.Node_Value{Value: v1.NewExpr(`inputs.what`)}}},
	}
}

// deliverSlack POSTs a body signed under the Slack scheme with the receiver's
// current time, as Slack signs a request.
func deliverSlack(t *testing.T, handler http.Handler, body, contentType, key string) *http.Response {
	t.Helper()

	headers, err := v1.SignWebhookDelivery(v1.WebhookSchemeSlack,
		secrets.NewSecret(secrets.NewRef("env", "k"), key), []byte(body), time.Now())
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, slackRoute, strings.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	return recorder.Result()
}

func slackReceiver(t *testing.T, sink *trail) http.Handler {
	t.Helper()

	// Over a cluster nothing answers on: a delivery that reaches the start answers
	// 503, one that does not start anything answers otherwise.
	return auditedReceiver(t, unreachableTemporal(t), sink, []*v1.Workflow{
		slackWorkflow(map[string]*v1.Value{"what": v1.NewExpr(`event.body.command + " " + event.body.text`)}),
	})
}

// A verified `url_verification` is answered with its challenge, starts nothing
// (a start against this cluster would be a 503), and is one recorded decision.
func TestASlackHandshakeIsAnsweredAfterVerificationAndStartsNothing(t *testing.T) {
	t.Parallel()

	sink := &trail{}
	receiver := slackReceiver(t, sink)

	body := `{"type":"url_verification","challenge":"3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P"}`
	resp := deliverSlack(t, receiver, body, "application/json", webhookSecret)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var answer map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&answer))
	assert.Equal(t, "3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P", answer["challenge"])

	records := sink.all()
	require.Len(t, records, 1)
	assert.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_WEBHOOK_DECLINED, records[0].GetDenyCode())
}

// The handshake is not a way to learn that a route exists: a challenge signed
// with the wrong key is the same uniform refusal as any other forgery.
func TestAForgedSlackHandshakeIsTheUniformRefusal(t *testing.T) {
	t.Parallel()

	receiver := slackReceiver(t, &trail{})

	resp := deliverSlack(t, receiver, `{"type":"url_verification","challenge":"x"}`, "application/json", "not-the-key")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// A slash command is a form: its fields are text under `event.body`, and the
// delivery reaches the start (503 here, the cluster being absent) rather than
// being refused as not JSON.
func TestASlackSlashCommandFormReachesTheStart(t *testing.T) {
	t.Parallel()

	receiver := slackReceiver(t, &trail{})

	form := url.Values{"command": {"/deploy"}, "text": {"prod east"}, "trigger_id": {"13345224609.738474920.8088930838d88f008e0"}}
	resp := deliverSlack(t, receiver, form.Encode(), "application/x-www-form-urlencoded", webhookSecret)

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"a verified slash command did not reach the start")
}

// An interactive component's `payload` is unwrapped, so a mapping reads
// `event.body.actions[0].action_id`; and a form that repeats a field is refused
// to the key holder, who is owed the reason.
func TestASlackInteractivePayloadIsUnwrappedAndAmbiguityIsRefused(t *testing.T) {
	t.Parallel()

	receiver := auditedReceiver(t, unreachableTemporal(t), &trail{}, []*v1.Workflow{
		func() *v1.Workflow {
			workflow := slackWorkflow(map[string]*v1.Value{"what": v1.NewExpr(`event.body.actions[0].action_id`)})
			workflow.Triggers.Webhooks[0].IdempotencyKey = v1.NewExpr(`event.body.trigger_id`)

			return workflow
		}(),
	})

	payload := `{"type":"block_actions","trigger_id":"t1","actions":[{"action_id":"approve"}]}`
	resp := deliverSlack(t, receiver, "payload="+url.QueryEscape(payload), "application/x-www-form-urlencoded", webhookSecret)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "an interactive payload did not reach the start")

	twice := "payload=" + url.QueryEscape(payload) + "&payload=" + url.QueryEscape(payload)
	refused := deliverSlack(t, receiver, twice, "application/x-www-form-urlencoded", webhookSecret)
	assert.Equal(t, http.StatusBadRequest, refused.StatusCode)
}
