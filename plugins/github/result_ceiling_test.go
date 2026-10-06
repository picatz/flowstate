package main

import (
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	githubv1 "github.com/picatz/flowstate/plugins/github/gen/github/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// fillStrings sets every string field of m (and one element of every repeated
// string field) to value, so a record is as wide as value makes its strings.
func fillStrings(m proto.Message, value string) {
	msg := m.ProtoReflect()
	fields := msg.Descriptor().Fields()

	for i := range fields.Len() {
		fd := fields.Get(i)
		if fd.Kind() != protoreflect.StringKind {
			continue
		}

		switch {
		case fd.IsList():
			msg.Mutable(fd).List().Append(protoreflect.ValueOfString(value))
		default:
			msg.Set(fd, protoreflect.ValueOfString(value))
		}
	}
}

// listingAtItsBudget returns as many copies of a record as paginateBounded
// admits before its byte budget stops the walk.
func listingAtItsBudget[T proto.Message](record func() T) []T {
	var items []T

	for total := 0; ; {
		item := record()
		size := proto.Size(item) + recordFramingBytes
		if total+size > maxResultBytes {
			return items
		}

		total += size
		items = append(items, item)
	}
}

// TestResultBudgetFitsATaskOutput pins #2168: a listing of any of the three
// kinds that spends the whole byte budget, as paginateBounded measures it, is
// within flowstatev1.MaxTaskOutputBytes once it is encoded as the step output
// the host measures. Two shapes bracket the cost: records of short strings,
// where the framing the output spells around each one dominates, and records
// of control characters, which JSON spells in six bytes each.
func TestResultBudgetFitsATaskOutput(t *testing.T) {
	for shape, value := range map[string]string{"short": "x", "wide": strings.Repeat("\x01", 512)} {
		t.Run("issues/"+shape, func(t *testing.T) {
			items := listingAtItsBudget(func() *githubv1.IssueSummary {
				rec := &githubv1.IssueSummary{Number: 1}
				fillStrings(rec, value)

				return rec
			})
			checkListingFits(t, len(items), &githubv1.IssueListOutputs{Issues: issueSummaryValues(items), Truncated: true})
		})

		t.Run("pull_requests/"+shape, func(t *testing.T) {
			items := listingAtItsBudget(func() *githubv1.PullRequestSummary {
				rec := &githubv1.PullRequestSummary{Number: 1}
				fillStrings(rec, value)

				return rec
			})
			checkListingFits(t, len(items), &githubv1.PullRequestListOutputs{PullRequests: pullRequestSummaryValues(items), Truncated: true})
		})

		t.Run("pull_request_files/"+shape, func(t *testing.T) {
			items := listingAtItsBudget(func() *githubv1.PullRequestFile {
				rec := &githubv1.PullRequestFile{Additions: 1}
				fillStrings(rec, value)

				return rec
			})
			checkListingFits(t, len(items), &githubv1.PullRequestFilesOutputs{Files: pullRequestFileValues(items), Truncated: true})
		})
	}
}

func checkListingFits(t *testing.T, items int, outputs proto.Message) {
	t.Helper()

	out, err := sdk.EncodeOutputs(outputs)
	if err != nil {
		t.Fatalf("EncodeOutputs: %v", err)
	}
	if err := flowstatev1.CheckTaskOutputSize(out); err != nil {
		t.Fatalf("a listing at its own byte budget (%d bytes, %d items) is refused by the host: %v", maxResultBytes, items, err)
	}
}
