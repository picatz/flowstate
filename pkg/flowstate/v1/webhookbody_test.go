package flowstatev1_test

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

var slackT = schemeTrigger(v1.WebhookSchemeSlack)

func TestFormBodiesDecodeToTextFields(t *testing.T) {
	t.Parallel()

	body, err := v1.DecodeWebhookBody(slackT, "application/x-www-form-urlencoded; charset=utf-8",
		[]byte("command=%2Fdeploy&text=prod+east&user_id=U1"))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"command": "/deploy", "text": "prod east", "user_id": "U1"}, body)
}

func TestAFormPayloadFieldIsTheJSONItCarries(t *testing.T) {
	t.Parallel()

	form := "payload=" + url.QueryEscape(`{"type":"block_actions","actions":[{"action_id":"approve"}],"n":4200}`)
	body, err := v1.DecodeWebhookBody(slackT, "application/x-www-form-urlencoded", []byte(form))
	require.NoError(t, err)

	document, ok := body.(map[string]any)
	require.True(t, ok)
	actions, _ := document["actions"].([]any)
	require.Len(t, actions, 1)
	require.Equal(t, "approve", actions[0].(map[string]any)["action_id"])
	require.NotEqual(t, float64(4200), document["n"], "numbers must read as the receiver reads them, not as float64")

	// Beside other fields it is only text, which is what it is.
	mixed, err := v1.DecodeWebhookBody(slackT, "application/x-www-form-urlencoded", []byte(form+"&x=1"))
	require.NoError(t, err)
	require.IsType(t, "", mixed.(map[string]any)["payload"])
}

func TestFormBodiesAreRefusedWhenAmbiguousOrUnbounded(t *testing.T) {
	t.Parallel()

	const form = "application/x-www-form-urlencoded"

	_, err := v1.DecodeWebhookBody(slackT, form, []byte("payload=%7B%7D&payload=%7B%7D"))
	require.ErrorContains(t, err, `repeats the field "payload"`)

	_, err = v1.DecodeWebhookBody(slackT, form, []byte("a=1&a=2"))
	require.ErrorContains(t, err, "repeats")

	_, err = v1.DecodeWebhookBody(slackT, form, []byte("payload=not-json"))
	require.ErrorContains(t, err, "`payload` field is not a JSON document")

	_, err = v1.DecodeWebhookBody(slackT, form, []byte("payload=%7B%7D%20%7B%7D"))
	require.ErrorContains(t, err, "more than one JSON document")

	_, err = v1.DecodeWebhookBody(slackT, form, []byte("a=%zz"))
	require.Error(t, err)

	var many strings.Builder
	for i := range 300 {
		many.WriteString("f")
		many.WriteString(strings.Repeat("x", i))
		many.WriteString("=1&")
	}
	_, err = v1.DecodeWebhookBody(slackT, form, []byte(many.String()))
	require.ErrorContains(t, err, "more than 256 fields")

	// The bound is on pairs read: a megabyte of them is refused at the 257th,
	// and a repeated one at its second, before either is accumulated.
	var flood strings.Builder
	for i := range 100_000 {
		fmt.Fprintf(&flood, "f%d=&", i)
	}
	_, err = v1.DecodeWebhookBody(slackT, form, []byte(flood.String()))
	require.ErrorContains(t, err, "more than 256 fields")

	_, err = v1.DecodeWebhookBody(slackT, form, []byte(strings.Repeat("a=&", 300_000)))
	require.ErrorContains(t, err, "repeats")

	_, err = v1.DecodeWebhookBody(slackT, form, []byte("a=1;b=2"))
	require.Error(t, err)
}

// Only the form type selects the form reader: a form-shaped body under any other
// media type, or none, is still one JSON document or a refusal.
func TestOnlyTheFormTypeSelectsTheFormReader(t *testing.T) {
	t.Parallel()

	for _, contentType := range []string{"", "application/json", "text/plain", "garbage;;"} {
		_, err := v1.DecodeWebhookBody(slackT, contentType, []byte("a=1&b=2"))
		require.ErrorContains(t, err, "not a JSON document", contentType)
	}

	body, err := v1.DecodeWebhookBody(slackT, "application/json", []byte(`{"a":1}`))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": int64(1)}, body)
}

func TestSlackHandshakeChallengeIsRecognisedOnlyUnderTheSlackScheme(t *testing.T) {
	t.Parallel()

	handshake := map[string]any{"type": "url_verification", "challenge": "abc123"}
	slack := schemeTrigger(v1.WebhookSchemeSlack)

	challenge, ok := v1.SlackURLVerificationChallenge(slack, handshake)
	require.True(t, ok)
	require.Equal(t, "abc123", challenge)

	_, ok = v1.SlackURLVerificationChallenge(schemeTrigger(v1.WebhookSchemeGitHub), handshake)
	require.False(t, ok, "a trigger that is not verified as Slack has no handshake to answer")

	for name, body := range map[string]any{
		"an event":      map[string]any{"type": "event_callback", "challenge": "x"},
		"no challenge":  map[string]any{"type": "url_verification"},
		"empty":         map[string]any{"type": "url_verification", "challenge": ""},
		"not text":      map[string]any{"type": "url_verification", "challenge": int64(1)},
		"too long":      map[string]any{"type": "url_verification", "challenge": strings.Repeat("a", 1025)},
		"line break":    map[string]any{"type": "url_verification", "challenge": "a\nb"},
		"not an object": []any{handshake},
	} {
		_, ok := v1.SlackURLVerificationChallenge(slack, body)
		require.False(t, ok, name)
	}
}

// The Content-Type is not signed, so a trigger that does not expect forms never
// reads one: replaying a captured delivery with another type cannot change how
// its signed bytes are read.
func TestOnlyAFormSendingProviderReadsAForm(t *testing.T) {
	t.Parallel()

	for _, scheme := range []string{v1.WebhookSchemeHMACSHA256, v1.WebhookSchemeGitHub, v1.WebhookSchemeStripe} {
		_, err := v1.DecodeWebhookBody(schemeTrigger(scheme), "application/x-www-form-urlencoded", []byte("a=1"))
		require.ErrorContains(t, err, "not a JSON document", scheme)
	}
}
