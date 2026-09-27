package server

import (
	"regexp"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestScheduleIDInputsAreTheSchemaBounds holds the name lengths the
// schedule-fired id assertion in schedules.go sums to the bounds the schema
// enforces. The assertion proves every such id fits [v1.MaxWorkflowIDLen]
// only for the lengths it is given; a schema that loosened a name bound
// without this constant moving would let schedule-fired runs outgrow the
// bound every addressing request holds them to, and become unaddressable.
func TestScheduleIDInputsAreTheSchemaBounds(t *testing.T) {
	for _, tc := range []struct {
		msg  proto.Message
		want int
	}{
		{&v1.CreateScheduleRequest{}, maxScheduleNameLen},
		{&v1.Workflow{}, v1.MaxWorkflowNameLen},
	} {
		descriptor := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(descriptor.Name()), func(t *testing.T) {
			rules := nameRules(t, descriptor)
			require.Equal(t, uint64(tc.want), rules.GetMaxLen())
			// max_len counts code points; a name is as many bytes in the id
			// only because the pattern admits ASCII alone.
			pattern := regexp.MustCompile(rules.GetPattern())
			require.True(t, pattern.MatchString("nightly-etl_2"))
			require.False(t, pattern.MatchString("nightly-\u00e9tl"),
				"the pattern admits a non-ASCII name, so max_len no longer bounds its bytes")
		})
	}
}

func nameRules(t *testing.T, descriptor protoreflect.MessageDescriptor) *validate.StringRules {
	t.Helper()

	field := descriptor.Fields().ByName("name")
	require.NotNil(t, field, "name is gone from %s", descriptor.FullName())
	rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
	require.NotNil(t, rules, "%s.name carries no validation rules", descriptor.FullName())
	return rules.GetString()
}
