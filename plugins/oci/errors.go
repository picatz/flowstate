package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// Every task in this plugin reads. That is what lets this file be as precise as
// the registry's own answer allows: a GET that failed halfway left nothing
// behind to reconcile, so there is no [sdk.OutcomeUnknown] here and nothing
// this plugin refuses to retry out of caution. A write-side task would need the
// opposite posture, which is why one has not been added quietly beside these.

// classifyTransportError turns a failure that happened before any response into
// the SDK's classification.
func classifyTransportError(ref reference, err error) error {
	var limited *netpolicy.RateLimitedError
	if errors.As(err, &limited) {
		delay := boundedRetryAfter(limited.RetryAfter)
		return sdk.UnavailableAfter(delay, "operator egress policy rate-limited the request to %s; retry after %s", ref.Registry, delay)
	}

	var deny *netpolicy.DenyError
	if errors.As(err, &deny) {
		return sdk.PermissionDenied(
			"deployment egress policy denied reaching %s; a registry this plugin may read is one the policy permits",
			ref.Registry)
	}

	var tooLarge *netpolicy.BodyTooLargeError
	if errors.As(err, &tooLarge) {
		return sdk.Failed("%s returned more than the operator egress policy's %d-byte limit", ref.Registry, tooLarge.Limit)
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sdk.Unavailable("the request to %s did not complete in time", ref.Registry)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return sdk.Unavailable("%s could not be reached: %v", ref.Registry, netErr)
	}
	return sdk.Unavailable("the request to %s failed: %v", ref.Registry, err)
}

// classifyStatus turns a registry's own refusal into the SDK's classification.
//
// The distribution specification gives errors a body - a list of {code,
// message, detail} objects - and the code is more useful than the status for
// telling a caller what to fix, so it is read when it is there and bounded when
// it is used.
func classifyStatus(ref reference, response *http.Response, doing string) error {
	code := registryErrorCode(response)

	switch response.StatusCode {
	case http.StatusNotFound:
		return sdk.NotFound("%s has no such object while %s%s", ref.Registry, doing, code)
	case http.StatusUnauthorized, http.StatusForbidden:
		return sdk.PermissionDenied("%s refused the request while %s%s", ref.Registry, doing, code)
	case http.StatusTooManyRequests:
		delay := retryAfter(response.Header.Get("Retry-After"))
		return sdk.UnavailableAfter(delay, "%s rate-limited the request while %s; retry after %s", ref.Registry, doing, delay)
	case http.StatusRequestedRangeNotSatisfiable, http.StatusBadRequest:
		return sdk.InvalidInput("%s refused the request while %s%s", ref.Registry, doing, code)
	}

	if response.StatusCode >= 500 {
		// A read: another attempt is free, and a registry behind a load
		// balancer answers 502 for reasons that pass.
		return sdk.Unavailable("%s returned HTTP %d while %s%s", ref.Registry, response.StatusCode, doing, code)
	}
	return sdk.Failed("%s returned HTTP %d while %s%s", ref.Registry, response.StatusCode, doing, code)
}

// registryErrorCode reads the specification's error body, bounded, and renders
// it as a parenthetical. It never fails: a registry that answers an error with
// something else still gets its status reported.
func registryErrorCode(response *http.Response) string {
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<10))
	if err != nil || len(body) == 0 {
		return ""
	}

	var answer struct {
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &answer); err != nil || len(answer.Errors) == 0 {
		return ""
	}

	first := answer.Errors[0]
	if first.Code == "" && first.Message == "" {
		return ""
	}
	if first.Message == "" {
		return " (" + truncate(first.Code, maxErrorBytes) + ")"
	}
	return " (" + truncate(strings.TrimSpace(first.Code+": "+first.Message), maxErrorBytes) + ")"
}

// retryAfter reads the header a registry asks with, bounded.
func retryAfter(value string) time.Duration {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil || seconds <= 0 {
		return time.Second
	}
	return boundedRetryAfter(time.Duration(seconds) * time.Second)
}

// boundedRetryAfter keeps another party's requested delay inside what a step is
// willing to wait: a registry asking for an hour is asking this workflow to
// hold a worker for an hour.
func boundedRetryAfter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return time.Second
	}
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}
