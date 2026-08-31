package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-github/v75/github"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	pluginhost "github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

const errorWireHelperEnv = "FLOWSTATE_GITHUB_ERROR_WIRE_HELPER"

// TestMain lets the host launch this test binary as a real SDK plugin. The
// subprocess is selected by an explicit environment value that only the test
// host below supplies; an ordinary test process runs the suite as usual.
func TestMain(m *testing.M) {
	if os.Getenv(errorWireHelperEnv) == "1" {
		if err := runErrorWireHelper(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runErrorWireHelper() error {
	wait := 45 * time.Second
	tasks := []sdk.Task{
		errorWireTask("primary_rate_limit", func() error {
			return classifyReadError(&github.RateLimitError{
				Rate: github.Rate{Reset: github.Timestamp{Time: time.Now().Add(30 * time.Second)}},
			})
		}),
		errorWireTask("secondary_rate_limit", func() error {
			return classifyReadError(&github.AbuseRateLimitError{RetryAfter: &wait})
		}),
		errorWireTask("long_rate_limit", func() error {
			return classifyReadError(&github.AbuseRateLimitError{RetryAfter: durationPtr(10 * time.Minute)})
		}),
		errorWireTask("unknown_mutation", func() error {
			return classifyMutationError(context.DeadlineExceeded)
		}),
	}

	return sdk.Run(context.Background(), sdk.Plugin{
		Name:        "github-errors",
		Version:     "0.0.1",
		Description: "GitHub error-classification wire fixture",
		Tasks:       tasks,
	})
}

func errorWireTask(name string, classify func() error) sdk.Task {
	return sdk.Task{
		Name:    name,
		Summary: "Return one GitHub-classified error",
		// The task always fails before reading inputs, so use host-owned
		// descriptors and keep this fixture focused on the error wire path.
		Input:  &flowstatev1.Task_Log_Inputs{},
		Output: &flowstatev1.Task_Log_Outputs{},
		Fn: func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
			return nil, classify()
		},
	}
}

func durationPtr(d time.Duration) *time.Duration { return &d }

func TestGitHubErrorClassificationsReachTheHost(t *testing.T) {
	host := openErrorWireHost(t)

	defs := make(map[string]flowstatev1.TaskDef)
	for _, def := range host.TaskDefs() {
		defs[def.Name] = def
	}

	tests := []struct {
		name          string
		kind          flowstatev1.ErrorKind
		minRetryAfter time.Duration
		maxRetryAfter time.Duration
		wantRetryable bool
	}{
		{
			name:          "primary_rate_limit",
			kind:          flowstatev1.ErrorKindUpstream,
			minRetryAfter: 25 * time.Second,
			maxRetryAfter: 30 * time.Second,
			wantRetryable: true,
		},
		{
			name:          "secondary_rate_limit",
			kind:          flowstatev1.ErrorKindUpstream,
			minRetryAfter: 45 * time.Second,
			maxRetryAfter: 45 * time.Second,
			wantRetryable: true,
		},
		{
			name:          "long_rate_limit",
			kind:          flowstatev1.ErrorKindUpstream,
			minRetryAfter: 5 * time.Minute,
			maxRetryAfter: 5 * time.Minute,
			wantRetryable: true,
		},
		{
			name: "unknown_mutation",
			kind: flowstatev1.ErrorKindUpstreamUnknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			def, ok := defs["github-errors."+tc.name]
			if !ok {
				t.Fatalf("helper did not serve task %q", tc.name)
			}
			_, err := def.Fn(t.Context(), nil, nil)
			var taskErr *flowstatev1.TaskError
			if !errors.As(err, &taskErr) {
				t.Fatalf("error = %T %v, want *flowstatev1.TaskError", err, err)
			}
			if taskErr.Kind != tc.kind {
				t.Errorf("kind = %s, want %s", taskErr.Kind, tc.kind)
			}
			if taskErr.Retryable() != tc.wantRetryable {
				t.Errorf("retryable = %v, want %v", taskErr.Retryable(), tc.wantRetryable)
			}
			if taskErr.RetryAfter < tc.minRetryAfter || taskErr.RetryAfter > tc.maxRetryAfter {
				t.Errorf("retry_after = %v, want within [%v, %v]", taskErr.RetryAfter, tc.minRetryAfter, tc.maxRetryAfter)
			}
		})
	}
}

func openErrorWireHost(t *testing.T) *pluginhost.Host {
	t.Helper()

	dir := t.TempDir()
	src, err := os.Open(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	dstPath := filepath.Join(dir, pluginhost.BinaryPrefix+"github-errors")
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		t.Fatal(err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}

	host, err := pluginhost.NewHost(pluginhost.Config{
		SearchPath:          []string{dir},
		Only:                []string{"github-errors"},
		Env:                 []string{errorWireHelperEnv + "=1"},
		DisableHealthChecks: true,
		HandshakeTimeout:    5 * time.Second,
		DescribeTimeout:     5 * time.Second,
		CallTimeout:         5 * time.Second,
		ShutdownGrace:       time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("closing plugin host: %v", err)
		}
	})
	if err := host.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	return host
}
