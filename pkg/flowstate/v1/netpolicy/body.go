package netpolicy

import (
	"fmt"
	"io"
	"math"
	"net/http"
)

// ReadResponseBody reads the body of resp under the policy's response body limit.
// It is what a task should use in place of [io.ReadAll], which would buffer a
// response of any size in memory.
//
// Exceeding the limit is an error wrapping [ErrBodyTooLarge], never a truncated
// body: a caller that gets bytes back can rely on having all of them. A response
// that declares an oversized Content-Length is rejected without being read.
//
// It does not close the body; the caller keeps that responsibility.
func (p *Policy) ReadResponseBody(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}

	limit := p.cfg.maxResponseBytes

	if limit > 0 && resp.ContentLength > limit {
		return nil, &BodyTooLargeError{Limit: limit}
	}

	return ReadLimited(resp.Body, limit)
}

// ReadLimited reads from r until EOF, or fails with an error wrapping
// [ErrBodyTooLarge] once more than limit bytes are available. A non-positive
// limit reads everything.
//
// It reads one byte past the limit to tell a response that exactly fills the
// limit from one that exceeds it, so the limit is never mistaken for the end of
// the data.
func ReadLimited(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		body, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}
		return body, nil
	}

	// Read one byte past the limit, unless doing so would overflow, in which case
	// the limit is already larger than any body that could be buffered.
	probe := limit
	if probe < math.MaxInt64 {
		probe++
	}

	body, err := io.ReadAll(io.LimitReader(r, probe))
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if int64(len(body)) > limit {
		return nil, &BodyTooLargeError{Limit: limit}
	}

	return body, nil
}

// limitedBody wraps a response body so that reading past the policy's limit fails
// instead of returning an oversized body. It is installed by the policy's round
// tripper, which bounds any caller, including one that reaches for [io.ReadAll].
//
// Like the body it wraps, it is not safe for concurrent use.
type limitedBody struct {
	body    io.ReadCloser
	limit   int64
	read    int64
	tripped bool
}

// Read implements [io.Reader].
func (b *limitedBody) Read(p []byte) (int, error) {
	if b.tripped {
		return 0, &BodyTooLargeError{Limit: b.limit}
	}

	// Allow one byte past the limit to be read so that exceeding the limit is
	// distinguishable from ending exactly at it. The subtraction cannot overflow
	// because read never exceeds limit here, and the increment is guarded.
	remaining := b.limit - b.read
	if remaining < math.MaxInt64 {
		remaining++
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	n, err := b.body.Read(p)
	b.read += int64(n)

	if b.read > b.limit {
		b.tripped = true
		// Report only the bytes within the limit, so a caller that ignores the
		// error cannot come away with the byte that broke it.
		if over := int(b.read - b.limit); over <= n {
			n -= over
		} else {
			n = 0
		}
		return n, &BodyTooLargeError{Limit: b.limit}
	}

	return n, err
}

// Close implements [io.Closer], closing the underlying body.
func (b *limitedBody) Close() error {
	return b.body.Close()
}
