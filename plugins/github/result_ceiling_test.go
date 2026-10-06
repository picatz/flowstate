package main

import (
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
	"google.golang.org/protobuf/proto"
)

// TestResultBudgetFitsATaskOutput pins #2168: a listing that spends the whole
// byte budget, as paginateBounded measures it, is within
// flowstatev1.MaxTaskOutputBytes once it is encoded as the step output the
// host measures. Short strings are the worst shape: the framing a record
// costs in the output is a larger share of a small record than a large one.
func TestResultBudgetFitsATaskOutput(t *testing.T) {
	record := func() *githubv1.IssueSummary {
		return &githubv1.IssueSummary{
			Number:    1,
			Title:     "x",
			State:     "open",
			Labels:    []string{"a"},
			HtmlUrl:   "u",
			CreatedAt: "2026-10-06T00:00:00Z",
			UpdatedAt: "2026-10-06T00:00:00Z",
		}
	}
	// Control characters are the widest spelling JSON has, so a title of
	// them is the largest an encoded record can be for its measured size.
	wide := record()
	wide.Title = strings.Repeat("\x01", 512)

	for name, rec := range map[string]*githubv1.IssueSummary{"small": record(), "wide": wide} {
		t.Run(name, func(t *testing.T) {
			var items []*githubv1.IssueSummary
			for total := 0; ; {
				size := proto.Size(rec)
				if total+size > maxResultBytes {
					break
				}
				total += size
				items = append(items, rec)
			}

			out, err := sdk.EncodeOutputs(&githubv1.IssueListOutputs{
				Issues:    issueSummaryValues(items),
				Truncated: true,
			})
			if err != nil {
				t.Fatalf("EncodeOutputs: %v", err)
			}
			if err := flowstatev1.CheckTaskOutputSize(out); err != nil {
				t.Fatalf("a listing at its own byte budget (%d bytes, %d items) is refused by the host: %v", maxResultBytes, len(items), err)
			}
		})
	}
}
