package flowstatev1

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDigestSHA256(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		expr string
		data []byte
	}{
		{name: "empty string", expr: `digest.sha256("")`, data: []byte{}},
		{name: "string", expr: `digest.sha256("hello")`, data: []byte("hello")},
		{name: "UTF-8 string", expr: `digest.sha256("héllo")`, data: []byte("héllo")},
		{name: "bytes", expr: `digest.sha256(b"hello")`, data: []byte("hello")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := DefaultEvaluator().EvalString(t.Context(), test.expr, []string{"digest"}, map[string]any{})
			require.NoError(t, err)
			require.Equal(t, ContentDigest(test.data), got.Value())
		})
	}
}

func TestDigestSHA256RefusesOtherTypes(t *testing.T) {
	t.Parallel()

	_, err := DefaultEvaluator().EvalString(t.Context(), `digest.sha256(42)`, []string{"digest"}, map[string]any{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such overload")
}

func TestDigestSHA256SpendsCostByInputSize(t *testing.T) {
	t.Parallel()

	body := strings.Repeat("x", 50_000)
	items := make([]any, 100)
	for i := range items {
		items[i] = int64(i)
	}

	_, err := NewEvaluator(WithLimits(Limits{
		Cost:                    20_000,
		InterruptCheckFrequency: DefaultInterruptCheckFrequency,
	})).EvalString(t.Context(), `items.map(i, digest.sha256(body))`, []string{"digest"}, map[string]any{
		"body":  body,
		"items": items,
	})

	require.Error(t, err, "repeated hashing of a large input must spend the evaluation budget")
	require.Contains(t, err.Error(), "cost limit")
}
