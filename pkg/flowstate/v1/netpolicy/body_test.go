package netpolicy

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ReadLimited(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		limit int64
		check func(t *testing.T, body []byte, err error)
	}{
		{
			name:  "under the limit",
			body:  "hello",
			limit: 16,
			check: func(t *testing.T, body []byte, err error) {
				require.NoError(t, err)
				require.Equal(t, "hello", string(body))
			},
		},
		{
			name:  "exactly at the limit",
			body:  "hello",
			limit: 5,
			check: func(t *testing.T, body []byte, err error) {
				require.NoError(t, err, "a body that exactly fills the limit is not too large")
				require.Equal(t, "hello", string(body))
			},
		},
		{
			name:  "one byte over the limit",
			body:  "hello!",
			limit: 5,
			check: func(t *testing.T, body []byte, err error) {
				require.ErrorIs(t, err, ErrBodyTooLarge)
				require.Nil(t, body, "an oversized body must not be returned truncated")
			},
		},
		{
			name:  "far over the limit",
			body:  strings.Repeat("a", 1<<20),
			limit: 128,
			check: func(t *testing.T, body []byte, err error) {
				require.ErrorIs(t, err, ErrBodyTooLarge)
				require.Nil(t, body)

				var tooLarge *BodyTooLargeError
				require.ErrorAs(t, err, &tooLarge)
				require.Equal(t, int64(128), tooLarge.Limit)
			},
		},
		{
			name:  "empty body",
			body:  "",
			limit: 16,
			check: func(t *testing.T, body []byte, err error) {
				require.NoError(t, err)
				require.Empty(t, body)
			},
		},
		{
			name:  "a non-positive limit reads everything",
			body:  strings.Repeat("a", 4096),
			limit: 0,
			check: func(t *testing.T, body []byte, err error) {
				require.NoError(t, err)
				require.Len(t, body, 4096)
			},
		},
		{
			// Reading one byte past the limit must not overflow into a negative
			// length, which io.LimitReader reports as an immediate EOF: the body
			// would come back empty with no error at all.
			name:  "the largest possible limit still reads the body",
			body:  "hello",
			limit: math.MaxInt64,
			check: func(t *testing.T, body []byte, err error) {
				require.NoError(t, err)
				require.Equal(t, "hello", string(body))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, err := ReadLimited(strings.NewReader(test.body), test.limit)
			test.check(t, body, err)
		})
	}
}

func Test_Policy_ReadResponseBody(t *testing.T) {
	const big = 1 << 20 // 1 MiB, standing in for the response that used to be buffered whole

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/big":
			// Chunked, so Content-Length cannot short-circuit the check.
			for range big / 1024 {
				fmt.Fprint(w, strings.Repeat("a", 1024))
			}
		case "/declared":
			w.Header().Set("Content-Length", fmt.Sprint(big))
			fmt.Fprint(w, strings.Repeat("a", big))
		default:
			fmt.Fprint(w, "small")
		}
	}))
	t.Cleanup(server.Close)

	tests := []struct {
		name  string
		path  string
		limit int64
		check func(t *testing.T, body []byte, err error)
	}{
		{
			name:  "a small body is returned",
			path:  "/small",
			limit: 1024,
			check: func(t *testing.T, body []byte, err error) {
				require.NoError(t, err)
				require.Equal(t, "small", string(body))
			},
		},
		{
			name:  "a body over the cap is an error, not a truncated body",
			path:  "/big",
			limit: 4096,
			check: func(t *testing.T, body []byte, err error) {
				require.ErrorIs(t, err, ErrBodyTooLarge)
				require.Nil(t, body)
			},
		},
		{
			name:  "an oversized Content-Length is rejected without reading",
			path:  "/declared",
			limit: 4096,
			check: func(t *testing.T, body []byte, err error) {
				require.ErrorIs(t, err, ErrBodyTooLarge)
				require.Nil(t, body)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(WithAllowLoopback(), WithMaxResponseBytes(test.limit))
			require.NoError(t, err)

			resp, err := get(t, policy, server.URL+test.path)
			require.NoError(t, err)

			body, err := policy.ReadResponseBody(resp)
			test.check(t, body, err)
		})
	}

	t.Run("the cap applies even when the caller reads the body directly", func(t *testing.T) {
		policy, err := New(WithAllowLoopback(), WithMaxResponseBytes(4096))
		require.NoError(t, err)

		resp, err := get(t, policy, server.URL+"/big")
		require.NoError(t, err)

		// A caller reaching for io.ReadAll is still bounded, because the policy
		// wraps the body before the caller ever sees it.
		body, err := io.ReadAll(resp.Body)
		require.ErrorIs(t, err, ErrBodyTooLarge)
		require.LessOrEqual(t, len(body), 4096)
	})

	t.Run("a nil response reads nothing", func(t *testing.T) {
		policy, err := New()
		require.NoError(t, err)

		body, err := policy.ReadResponseBody(nil)
		require.NoError(t, err)
		require.Nil(t, body)
	})
}

func Test_limitedBody(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		limit int64
		check func(t *testing.T, read string, err error)
	}{
		{
			name:  "under the limit reads to EOF",
			body:  "hello",
			limit: 16,
			check: func(t *testing.T, read string, err error) {
				require.NoError(t, err)
				require.Equal(t, "hello", read)
			},
		},
		{
			name:  "exactly at the limit reads to EOF",
			body:  "hello",
			limit: 5,
			check: func(t *testing.T, read string, err error) {
				require.NoError(t, err)
				require.Equal(t, "hello", read)
			},
		},
		{
			name:  "over the limit errors and yields no more than the limit",
			body:  "hello world",
			limit: 5,
			check: func(t *testing.T, read string, err error) {
				require.ErrorIs(t, err, ErrBodyTooLarge)
				require.LessOrEqual(t, len(read), 5)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &limitedBody{
				body:  io.NopCloser(strings.NewReader(test.body)),
				limit: test.limit,
			}
			t.Cleanup(func() { require.NoError(t, body.Close()) })

			read, err := io.ReadAll(body)
			test.check(t, string(read), err)
		})
	}

	t.Run("the largest possible limit does not overflow the read window", func(t *testing.T) {
		// The limit is clamped when configured, so the wrapper installed on every
		// response of a shared client cannot panic slicing a negative length.
		policy, err := New(WithMaxResponseBytes(math.MaxInt64))
		require.NoError(t, err)

		body := &limitedBody{
			body:  io.NopCloser(strings.NewReader("hello")),
			limit: policy.MaxResponseBytes(),
		}
		t.Cleanup(func() { require.NoError(t, body.Close()) })

		read, err := io.ReadAll(body)
		require.NoError(t, err)
		require.Equal(t, "hello", string(read))
	})

	t.Run("a tripped body stays tripped", func(t *testing.T) {
		body := &limitedBody{
			body:  io.NopCloser(strings.NewReader("hello world")),
			limit: 2,
		}

		buf := make([]byte, 8)

		_, err := body.Read(buf)
		require.ErrorIs(t, err, ErrBodyTooLarge)

		n, err := body.Read(buf)
		require.ErrorIs(t, err, ErrBodyTooLarge)
		require.Zero(t, n)
	})
}
