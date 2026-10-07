package flowstatev1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/url"
	"strings"
)

// maxWebhookFormFields bounds how many fields a form-encoded delivery may carry.
// The body is already held to [MaxWebhookPayloadBytes]; this bounds the map built
// from it, which a body of nothing but `a&b&c&...` would otherwise make as large
// as it likes.
const maxWebhookFormFields = 256

// webhookFormPayloadField is the one field whose value is a JSON document rather
// than text: Slack sends an interactive component's payload as
// `payload=<url-encoded JSON>` and nothing else.
const webhookFormPayloadField = "payload"

// DecodeWebhookBody reads a verified delivery's raw body into the value
// `event.body` holds, by the delivery's declared media type.
//
// One function for the live receiver and for `flow test`, so a stored delivery
// and a real one produce the same value: a rehearsal that decoded a form
// differently from production would be the rehearsal lying about production.
//
//   - `application/x-www-form-urlencoded` becomes a map of field to text, which
//     is what a Slack slash command is. A field that repeats is refused rather
//     than resolved, because two values for one name is a delivery that means
//     something other than what any one mapping would read. When the form
//     carries exactly one field, `payload`, its value is decoded as the JSON
//     document it is (Slack interactivity), so `event.body.actions[0]` reads
//     the same as it does for a JSON delivery.
//   - Anything else, including no media type, is one JSON document with nothing
//     after it, as before. The receiver never trusted the media type to select a
//     *looser* parser, and still does not.
func DecodeWebhookBody(contentType string, raw []byte) (any, error) {
	if IsWebhookFormContentType(contentType) {
		return decodeWebhookForm(raw)
	}

	return decodeWebhookJSON(raw, "the delivery body")
}

// IsWebhookFormContentType reports whether a Content-Type header names a form
// body.
func IsWebhookFormContentType(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)

	return err == nil && mediaType == "application/x-www-form-urlencoded"
}

func decodeWebhookJSON(raw []byte, what string) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("%s is not a JSON document: %w", what, err)
	}

	// And nothing after it. [json.Decoder.Decode] reads one value and stops, so
	// `{"id":"a"} {"id":"b"}` — or a document followed by arbitrary bytes — would
	// decode as the first value and silently discard the rest, starting a run
	// from a prefix that can mean something other than what the payload as a
	// whole says. A delivery is one document, so end of input is part of the
	// contract and is checked rather than assumed. Into a [json.RawMessage] so
	// that nothing is built from what follows: the question is only whether
	// anything does.
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s carries more than one JSON document: a delivery is a "+
			"single JSON value with nothing after it, so send one document per delivery", what)
	}

	return NormalizeDeliveryNumbers(decoded), nil
}

func decodeWebhookForm(raw []byte) (any, error) {
	values, err := url.ParseQuery(string(raw))
	if err != nil {
		return nil, fmt.Errorf("the delivery body is not a form: %w", err)
	}
	if len(values) > maxWebhookFormFields {
		return nil, fmt.Errorf("the delivery form carries more than %d fields", maxWebhookFormFields)
	}

	fields := make(map[string]any, len(values))
	for name, all := range values {
		if len(all) != 1 {
			return nil, fmt.Errorf("the delivery form repeats the field %q: a field with several values "+
				"has no single reading, so send each field once", name)
		}
		fields[name] = all[0]
	}

	if payload, only := fields[webhookFormPayloadField].(string); only && len(fields) == 1 {
		return decodeWebhookJSON([]byte(payload), "the delivery form's `payload` field")
	}

	return fields, nil
}

// maxSlackChallengeBytes bounds the challenge a handshake echoes. Slack's own is
// a few dozen characters; the bound is what keeps the echo from being a way to
// return an attacker-sized body.
const maxSlackChallengeBytes = 1024

// SlackURLVerificationChallenge reports the challenge a Slack Events API
// `url_verification` request asks to have echoed, and whether the delivery is
// one.
//
// Only a trigger that verifies with [WebhookSchemeSlack] has a handshake to
// answer, and the answer is for the receiver to give *after* verification, so an
// outsider learns nothing about the route from it. A handshake starts nothing:
// there is no run to bind, which is why this is decided before binding.
func SlackURLVerificationChallenge(trigger *WebhookTrigger, body any) (string, bool) {
	if _, slack := trigger.GetVerify()[WebhookSchemeSlack]; !slack {
		return "", false
	}

	document, ok := body.(map[string]any)
	if !ok {
		return "", false
	}
	if kind, _ := document["type"].(string); kind != "url_verification" {
		return "", false
	}

	challenge, ok := document["challenge"].(string)
	if !ok || challenge == "" || len(challenge) > maxSlackChallengeBytes || strings.ContainsAny(challenge, "\r\n") {
		return "", false
	}

	return challenge, true
}
