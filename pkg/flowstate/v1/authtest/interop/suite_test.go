package interop_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest/interop"
	"github.com/stretchr/testify/require"
)

type adapter struct{ fail string }

func (a adapter) Name() string { return "flowstate-to-flowstate" }
func (a adapter) Run(_ context.Context, _ *interop.Environment, c interop.Case) error {
	if c.ID == a.fail {
		return errors.New("refused")
	}
	return nil
}

func TestCapabilityReportKeepsStandardsAndProviderExpectationsSeparate(t *testing.T) {
	env, err := interop.New(time.Unix(1_700_000_000, 0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Close()) })
	cases := []interop.Case{{ID: "8693-actor", Protocol: "exchange", Reference: "RFC 8693 §2.1", Subset: "actor token"}, {ID: "unreferenced"}}
	report := interop.Run(t.Context(), env, adapter{fail: "8693-actor"}, cases, []interop.ProviderExpectation{{Provider: "vendor", CaseID: "8693-actor", Expectation: "uses a vendor audience alias"}})
	require.Equal(t, interop.Fail, report.Results[0].Outcome)
	require.Equal(t, interop.Fail, report.Results[1].Outcome)
	markdown := report.Markdown()
	require.Contains(t, markdown, "RFC 8693 §2.1")
	require.Contains(t, markdown, "Provider-specific expectations (not conformance)")
	require.False(t, strings.Contains(strings.ToLower(markdown), "oidc compatible"))
}

func TestCoreCasesAlwaysNameExactReferences(t *testing.T) {
	for _, test := range interop.CoreCases {
		require.NotEmpty(t, test.Reference, test.ID)
		require.NotEmpty(t, test.Subset, test.ID)
	}
}
