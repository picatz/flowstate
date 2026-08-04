package plugin

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// This file is Codex's two P1s on PR #160, addressed directly.
//
// P1 #1: taskError used to scrub err before classifying it —
// scrubber.ScrubError(err), then taskError(..., thatResult). [secrets.Scrubber
// .ScrubError] deliberately returns a value with no Unwrap and no errors.As
// (see scrub.go), so once a plugin's failure message happened to contain the
// resolved secret that caused it, kindForCode and verdictFromDetails could no
// longer reach the *connect.Error or its ExecuteResponse detail underneath —
// InvalidInput, OutcomeUnknown, and a Retry-After all silently degraded to a
// retryable Internal failure, at exactly the moment classification mattered
// most. Fixed by classifying the unscrubbed original first and only scrubbing
// the message that gets wrapped into the result.
//
// P1 #2: scrubLiteral had no case for Value_BytesValue, so a plugin returning
// a secret input decoded into a bytes output shipped it to workflow history
// unredacted.

// TestTaskErrorClassifiesBeforeScrubbing is the regression test for P1 #1: a
// plugin's failure message embeds the resolved secret that caused it, and
// classification must still work exactly as it would have on the clean
// message, while the secret itself never survives into anything the result
// can be formatted as.
func TestTaskErrorClassifiesBeforeScrubbing(t *testing.T) {
	t.Parallel()

	const material = "resolved-secret-embedded-in-a-plugin-error-message"

	scrubber := secrets.NewScrubber()
	scrubber.AddValue(material)

	t.Run("unknown outcome", func(t *testing.T) {
		t.Parallel()

		connErr := connect.NewError(connect.CodeInternal,
			fmt.Errorf("backend said: %s, and then the connection reset", material))
		detail, detailErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{UnknownOutcome: true})
		require.NoError(t, detailErr)
		connErr.AddDetail(detail)

		result := taskError("example.task", "example", connErr, scrubber)

		var taskErr *flowstatev1.TaskError
		require.ErrorAs(t, result, &taskErr)
		assert.Equal(t, flowstatev1.ErrorKindUpstreamUnknown, taskErr.Kind,
			"an unknown outcome must not degrade to a retryable Internal failure "+
				"just because its message happened to contain a secret")
		assert.False(t, taskErr.Retryable(),
			"retrying an operation whose outcome is unknown can perform it a second time")

		assertNoLeak(t, material, result)
	})

	t.Run("retryable with a retry-after", func(t *testing.T) {
		t.Parallel()

		connErr := connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("rate limited after seeing %s", material))
		detail, detailErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{
			Retryable:  true,
			RetryAfter: durationpb.New(30 * time.Second),
		})
		require.NoError(t, detailErr)
		connErr.AddDetail(detail)

		result := taskError("example.task", "example", connErr, scrubber)

		var taskErr *flowstatev1.TaskError
		require.ErrorAs(t, result, &taskErr)
		assert.Equal(t, flowstatev1.ErrorKindUpstream, taskErr.Kind)
		assert.True(t, taskErr.Retryable())
		assert.Equal(t, 30*time.Second, taskErr.RetryAfter,
			"the plugin's preferred delay did not survive scrubbing")

		assertNoLeak(t, material, result)
	})

	t.Run("permanent, despite a retryable code, with a secret in the message", func(t *testing.T) {
		t.Parallel()

		connErr := connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("permanently refused, saw %s", material))
		detail, detailErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{Retryable: false})
		require.NoError(t, detailErr)
		connErr.AddDetail(detail)

		result := taskError("example.task", "example", connErr, scrubber)

		var taskErr *flowstatev1.TaskError
		require.ErrorAs(t, result, &taskErr)
		assert.False(t, taskErr.Retryable())
		assertNoLeak(t, material, result)
	})

	// The negative direction this bug's own tests were silently passing
	// through: with no secret in the message, classification already worked,
	// scrubber present or not. Pinned so the two paths — a message that has
	// something to scrub and one that does not — cannot diverge again.
	t.Run("no secret in the message classifies the same way", func(t *testing.T) {
		t.Parallel()

		connErr := connect.NewError(connect.CodeInternal, errors.New("ordinary failure, nothing secret here"))
		detail, detailErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{UnknownOutcome: true})
		require.NoError(t, detailErr)
		connErr.AddDetail(detail)

		result := taskError("example.task", "example", connErr, scrubber)

		var taskErr *flowstatev1.TaskError
		require.ErrorAs(t, result, &taskErr)
		assert.Equal(t, flowstatev1.ErrorKindUpstreamUnknown, taskErr.Kind)
		assert.Contains(t, result.Error(), "ordinary failure",
			"a message with nothing to scrub should read unchanged")
	})
}

// assertNoLeak checks the containment matrix CLAUDE.md asks for: the value on
// the error itself, on a struct holding it, and on a slice of those, under
// every verb that could reach it.
func assertNoLeak(t *testing.T, material string, err error) {
	t.Helper()

	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		assert.NotContains(t, fmtSprint(verb, err), material, "leaked via "+verb)
	}

	holder := struct{ Err error }{Err: err}
	slice := []error{err, err}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		assert.NotContains(t, fmtSprint(verb, holder), material, "leaked via "+verb+" on a struct")
		assert.NotContains(t, fmtSprint(verb, slice), material, "leaked via "+verb+" on a slice")
	}
}

// TestScrubLiteralRedactsBytesValues is the regression test for P1 #2: a bytes
// output is not a string, so [scrubLiteral]'s original switch never looked at
// one at all.
func TestScrubLiteralRedactsBytesValues(t *testing.T) {
	t.Parallel()

	const material = "secret-material-that-must-not-survive-as-bytes"

	scrubber := secrets.NewScrubber()
	scrubber.AddValue(material)

	t.Run("the whole value, as raw bytes", func(t *testing.T) {
		t.Parallel()

		outputs := &flowstatev1.Node_Outputs{
			NamedValues: map[string]*flowstatev1.Value{
				"blob": literalBytes([]byte(material)),
			},
		}

		require.NoError(t, scrubPluginOutputs(scrubber, outputs))

		got := outputs.GetNamedValues()["blob"].GetLiteral().GetBytesValue()
		assert.NotContains(t, string(got), material)
		assert.Contains(t, string(got), secrets.Redacted)
	})

	t.Run("embedded mid-buffer", func(t *testing.T) {
		t.Parallel()

		raw := append(append([]byte("prefix-"), []byte(material)...), []byte("-suffix")...)
		outputs := &flowstatev1.Node_Outputs{
			NamedValues: map[string]*flowstatev1.Value{
				"blob": literalBytes(raw),
			},
		}

		require.NoError(t, scrubPluginOutputs(scrubber, outputs))

		got := outputs.GetNamedValues()["blob"].GetLiteral().GetBytesValue()
		assert.NotContains(t, string(got), material)
		assert.Contains(t, string(got), "prefix-")
		assert.Contains(t, string(got), "-suffix")
		assert.Contains(t, string(got), secrets.Redacted)
	})

	t.Run("bytes that do not contain the secret pass through unchanged", func(t *testing.T) {
		t.Parallel()

		original := []byte{0x00, 0x01, 0xff, 'h', 'e', 'l', 'l', 'o', 0x00}
		outputs := &flowstatev1.Node_Outputs{
			NamedValues: map[string]*flowstatev1.Value{
				"blob": literalBytes(original),
			},
		}

		require.NoError(t, scrubPluginOutputs(scrubber, outputs))

		got := outputs.GetNamedValues()["blob"].GetLiteral().GetBytesValue()
		assert.Equal(t, original, got, "bytes with nothing to scrub must come back byte-identical")
	})
}

// literalBytes wraps raw bytes as a task output value, the shape a plugin's
// BytesValue output arrives in. Built directly rather than through
// [flowstatev1.NewValue], which treats a []byte as a generic slice and would
// build a list of individual byte elements instead of a single BytesValue.
func literalBytes(b []byte) *flowstatev1.Value {
	return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{
		Literal: &expr.Value{Kind: &expr.Value_BytesValue{BytesValue: b}},
	}}
}
