package flowstatev1

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetryPermittedUsesStructuredAttemptOutcome(t *testing.T) {
	tests := []struct {
		name    string
		kind    ErrorKind
		outcome *AttemptOutcome
		want    bool
	}{
		{
			name: "legacy errors retain their kind projection",
			kind: ErrorKindUpstream,
			want: true,
		},
		{
			name: "an outcome can deny a retryable kind",
			kind: ErrorKindUpstream,
			outcome: &AttemptOutcome{
				RepeatSafety:    AttemptOutcome_REPEAT_SAFETY_REQUIRES_RECONCILIATION,
				RetryPermission: AttemptOutcome_RETRY_PERMISSION_DENIED,
			},
		},
		{
			name: "unspecified structured evidence fails closed",
			kind: ErrorKindUpstream,
			outcome: &AttemptOutcome{
				RepeatSafety:    AttemptOutcome_REPEAT_SAFETY_SAFE,
				RetryPermission: AttemptOutcome_RETRY_PERMISSION_UNSPECIFIED,
			},
		},
		{
			name: "permission cannot override unsafe repetition",
			kind: ErrorKindUpstream,
			outcome: &AttemptOutcome{
				RepeatSafety:    AttemptOutcome_REPEAT_SAFETY_UNSAFE,
				RetryPermission: AttemptOutcome_RETRY_PERMISSION_PERMITTED,
			},
		},
		{
			name: "policy denial cannot be widened",
			kind: ErrorKindPolicyDenied,
			outcome: &AttemptOutcome{
				RepeatSafety:    AttemptOutcome_REPEAT_SAFETY_SAFE,
				RetryPermission: AttemptOutcome_RETRY_PERMISSION_PERMITTED,
			},
		},
		{
			name: "permission cannot widen a permanent classification",
			kind: ErrorKindInvalidInput,
			outcome: &AttemptOutcome{
				RepeatSafety:    AttemptOutcome_REPEAT_SAFETY_SAFE,
				RetryPermission: AttemptOutcome_RETRY_PERMISSION_PERMITTED,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := NewTaskOutcomeError("test", test.kind, test.outcome, errors.New("failed"))
			require.Equal(t, test.want, RetryPermitted(err))
		})
	}
}
