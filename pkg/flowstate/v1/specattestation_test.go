package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestRanSubmittedSpecificationTreatsSilenceAsRefusal is the three answers this
// field carries, and the reason it is `optional`.
//
// Only an explicit true is assent. False is a server saying its own copy ran, and
// an unset field is a server old enough to have no opinion — and both must reach a
// client as "do not trust the specification you hold", because a client that
// treated silence as assent would redact a substituted run against declarations
// that did not apply, which is #734 exactly.
func TestRanSubmittedSpecificationTreatsSilenceAsRefusal(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		run  *v1.RunResponse
		want bool
	}{
		{
			name: "attested as submitted",
			run:  &v1.RunResponse{SpecificationAsSubmitted: proto.Bool(true)},
			want: true,
		},
		{
			name: "attested as substituted",
			run:  &v1.RunResponse{SpecificationAsSubmitted: proto.Bool(false)},
			want: false,
		},
		{
			name: "a server that did not answer",
			run:  &v1.RunResponse{},
			want: false,
		},
		{
			name: "no response at all",
			run:  nil,
			want: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, test.run.RanSubmittedSpecification())
		})
	}
}
