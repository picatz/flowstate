package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
)

func TestFetchRetriesWithBackoffThenSucceeds(t *testing.T) {
	calls := 0
	var slept []time.Duration
	path, err := fetch(t.Context(), func(context.Context, log.Logger) (string, error) {
		calls++
		if calls < 3 {
			return "", errors.New("context deadline exceeded")
		}
		return "/cache/cli", nil
	}, func(d time.Duration) { slept = append(slept, d) }, io.Discard)
	require.NoError(t, err)
	require.Equal(t, "/cache/cli", path)
	require.Equal(t, []time.Duration{5 * time.Second, 10 * time.Second}, slept)
}

func TestFetchGivesUpAfterTheBound(t *testing.T) {
	calls := 0
	var slept []time.Duration
	_, err := fetch(t.Context(), func(context.Context, log.Logger) (string, error) {
		calls++
		return "", errors.New("down")
	}, func(d time.Duration) { slept = append(slept, d) }, io.Discard)
	require.ErrorContains(t, err, "after 4 attempts")
	require.ErrorContains(t, err, "down")
	require.Equal(t, attempts, calls)
	require.Len(t, slept, attempts-1, "no sleep after the last failure")
}

func TestCacheKeyNamesSDKAndPlatform(t *testing.T) {
	require.Regexp(t, `^temporal-cli-[a-z]+-[a-z0-9]+-sdk-\d+\.\d+\.\d+`, cacheKey())
}
