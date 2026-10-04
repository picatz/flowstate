package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestRetryAllowsKindNeverNarrowsATimeout: Temporal retries an attempt that ran
// out its own deadline whatever the non-retryable types say, so the local driver
// must not stop what the durable one cannot.
func TestRetryAllowsKindNeverNarrowsATimeout(t *testing.T) {
	t.Parallel()

	only := &v1.RetryPolicy{Only: []string{"Upstream"}}
	assert.True(t, v1.RetryAllowsKind(only, v1.ErrorKindTimeout), "only: omitting Timeout still retries it")
	assert.False(t, v1.RetryAllowsKind(only, v1.ErrorKindRateLimited), "other omitted kinds are narrowed")
	assert.NotContains(t, v1.RetryExcludedKinds(only), "Timeout")
	assert.Contains(t, v1.RetryExcludedKinds(only), "RateLimited")

	except := &v1.RetryPolicy{Except: []string{"Timeout"}}
	assert.True(t, v1.RetryAllowsKind(except, v1.ErrorKindTimeout), "except: [Timeout] has no effect, which is why the validator refuses it")
}
