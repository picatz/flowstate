package vault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// do performs one request against the vault's /v1/ API and returns the status and
// the body.
//
// The status is returned rather than interpreted, because the same code means
// different things to a read and to a login. Everything that is not a status —
// building the request, reaching the host, reading the body — is classified here,
// since that part means the same thing wherever it happens.
//
// The token is passed in rather than fetched here so that the caller keeps hold of
// which token it used, which is what lets a 403 discard the right one.
func (p *Provider) do(ctx context.Context, method, apiPath, token string, body []byte) (int, []byte, error) {
	// The provider's own timeout, applied so that the earlier of it and the
	// caller's deadline wins: a resolution must not outlive the activity waiting
	// on it, and must not wait on an unresponsive vault forever if the activity
	// has no deadline of its own.
	requestCtx := ctx
	if p.timeout > 0 {
		var cancel context.CancelFunc
		requestCtx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	request, err := http.NewRequestWithContext(requestCtx, method, p.endpoint(apiPath), reader)
	if err != nil {
		return 0, nil, fmt.Errorf("building the request to %s: %w", p.addr, err)
	}

	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", userAgent)

	// Vault requires this header on requests that carry a body, and the Vault
	// Agent's API proxy requires it on all of them.
	request.Header.Set("X-Vault-Request", "true")

	if token != "" {
		// The credential travels in a header and nowhere else: not in the path, not
		// in a query parameter, so it cannot end up in an access log or in the URL
		// that an *url.Error prints.
		request.Header.Set("X-Vault-Token", token)
	}

	if p.vaultNS != "" {
		request.Header.Set("X-Vault-Namespace", p.vaultNS)
	}

	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := p.client.Do(request)
	if err != nil {
		// A caller that cancelled or ran out of time gets its own error back rather
		// than a classification. Calling that transient would invite the engine to
		// retry a step that has already been abandoned.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, nil, fmt.Errorf("%s %s: %w", method, p.addr, ctxErr)
		}

		// Everything else here is the backend being out of reach: a refused
		// connection, DNS, a failed TLS handshake, or this provider's own timeout.
		// That is the one classification worth retrying.
		return 0, nil, fmt.Errorf("%w: %w", secrets.ErrUnavailable, err)
	}
	defer response.Body.Close()

	// One byte past the limit, so that a body exactly at the limit is
	// distinguishable from one that exceeds it and exceeding it is an error rather
	// than a silently truncated secret.
	// The status is reported alongside a failure from here on, because the response
	// got far enough to have one: a body that could not be read from a vault that
	// had already said it was sealed is that failure, not a new one, and the caller
	// classifies from the status in preference to this error.
	contents, err := io.ReadAll(io.LimitReader(response.Body, p.maxBytes+1))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return response.StatusCode, nil, fmt.Errorf("reading the response from %s: %w", p.addr, ctxErr)
		}

		return response.StatusCode, nil, fmt.Errorf(
			"%w: reading the response from %s: %w", secrets.ErrUnavailable, p.addr, err,
		)
	}

	if int64(len(contents)) > p.maxBytes {
		return response.StatusCode, nil, fmt.Errorf(
			"%w: %s answered %q with more than %d bytes",
			secrets.ErrTooLarge, p.addr, apiPath, p.maxBytes,
		)
	}

	return response.StatusCode, contents, nil
}

// endpoint returns the absolute URL for an API path.
//
// JoinPath escapes each element it is given, and the elements here have been
// through validatePath or cleanMount, so the resulting path holds exactly the
// segments it reads as.
func (p *Provider) endpoint(apiPath string) string {
	return p.base.JoinPath("v1", apiPath).String()
}

// decodeJSON decodes a response body.
//
// Numbers are decoded as [json.Number] so that a secret stored as a number keeps
// the digits it was written with, rather than the ones a float64 can represent.
//
// A decode failure is reported without the decoder's own message. That message
// quotes the byte it choked on, and this error is bound for logs and workflow
// history: a single byte of a credential is still a byte of a credential, and the
// contract's "not truncated, not hashed" leaves no room for "just the one
// character". The offset is reported instead, which locates the problem without
// disclosing anything.
func decodeJSON(body []byte, into any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	err := decoder.Decode(into)
	if err == nil {
		return nil
	}

	if errors.Is(err, io.EOF) {
		return fmt.Errorf("an empty body")
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return fmt.Errorf("a body that is not JSON, at byte %d of %d", syntaxErr.Offset, len(body))
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return fmt.Errorf(
			"a body whose %q field is a JSON %s, at byte %d",
			typeErr.Field, typeErr.Value, typeErr.Offset,
		)
	}

	return fmt.Errorf("a body that could not be decoded (%T)", err)
}

// unavailable reports whether a status means the vault could not serve the
// request, as opposed to refusing it. Only these are worth another attempt.
func unavailable(status int) bool {
	switch {
	case status == http.StatusTooManyRequests:
		// A quota clears on its own.
		return true
	case status == http.StatusRequestTimeout:
		// Vault does not answer with this, but a proxy or load balancer in front
		// of one does, and it means the same thing a 5xx does here.
		return true
	case status == http.StatusPreconditionFailed:
		// A performance standby is behind the leader; Vault documents this as the
		// status to retry.
		return true
	case status >= 500:
		return true
	default:
		return false
	}
}

// unavailableStatus reports a status that means the vault could not serve the
// request, which is the one classification the engine retries.
//
// The hints matter more than they look: a sealed vault and a vault that is not
// answering are the same status to a client, and an operator reading a failed
// workflow needs to know which it was.
func unavailableStatus(addr string, status int, apiPath string) error {
	var hint string

	switch status {
	case http.StatusTooManyRequests:
		hint = " (a rate limit quota was exceeded)"
	case http.StatusPreconditionFailed:
		hint = " (a standby node is behind; Vault asks that this be retried)"
	case http.StatusNotImplemented:
		hint = " (the vault is not initialized)"
	case http.StatusServiceUnavailable:
		hint = " (the vault is sealed, or a standby cannot forward the request)"
	}

	return fmt.Errorf(
		"%w: %s answered %d for %q%s",
		secrets.ErrUnavailable, addr, status, apiPath, hint,
	)
}
