package sdk

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const callerModeFixtureDir = "testdata/caller-mode/"

func oldIdentityDescriptor(t *testing.T) *descriptorpb.FileDescriptorProto {
	t.Helper()
	raw, err := os.ReadFile(callerModeFixtureDir + "old-host-identity.descriptor.pbtxt")
	require.NoError(t, err)
	var descriptor descriptorpb.FileDescriptorProto
	require.NoError(t, prototext.Unmarshal(raw, &descriptor))
	return &descriptor
}

func TestNewSDKReadsOldHostIdentityAsUnknown(t *testing.T) {
	t.Parallel()

	rawHex, err := os.ReadFile(callerModeFixtureDir + "old-host-identity.hex")
	require.NoError(t, err)
	raw, err := hex.DecodeString(strings.TrimSpace(string(rawHex)))
	require.NoError(t, err)

	var identity flowstatev1.WorkloadIdentity
	require.NoError(t, proto.Unmarshal(raw, &identity))
	assert.Equal(t, "legacy", identity.GetSubject())
	assert.Equal(t, flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_UNSPECIFIED,
		(Caller{Identity: &identity}).Mode(),
		"absence from an old host must never be read as production")
}

func TestOldPluginDescriptorIgnoresNewHostMode(t *testing.T) {
	t.Parallel()

	oldFile, err := protodesc.NewFile(oldIdentityDescriptor(t), nil)
	require.NoError(t, err)
	oldIdentity := dynamicpb.NewMessage(oldFile.Messages().ByName("WorkloadIdentity"))

	raw, err := proto.Marshal(&flowstatev1.WorkloadIdentity{
		Subject: "new-host",
		Mode:    flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_REHEARSAL,
	})
	require.NoError(t, err)
	require.NoError(t, proto.Unmarshal(raw, oldIdentity))

	assert.Equal(t, "new-host", oldIdentity.Get(oldIdentity.Descriptor().Fields().ByName("subject")).String())
	assert.Nil(t, oldIdentity.Descriptor().Fields().ByName("mode"),
		"the old plugin descriptor must remain able to decode the message without understanding mode")
}

func TestCallerModeFailsClosedForMissingAndUnknownValues(t *testing.T) {
	t.Parallel()

	for _, caller := range []Caller{
		{},
		{Identity: &flowstatev1.WorkloadIdentity{}},
		{Identity: &flowstatev1.WorkloadIdentity{Mode: flowstatev1.WorkloadIdentityMode(99)}},
	} {
		assert.Equal(t, flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_UNSPECIFIED, caller.Mode())
		assert.NotEqual(t, flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION, caller.Mode(),
			"missing or unknown mode must not authorize production-only effects")
	}
}

func TestCallerModeRecognizesOnlyKnownHostFacts(t *testing.T) {
	t.Parallel()

	for _, mode := range []flowstatev1.WorkloadIdentityMode{
		flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION,
		flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_REHEARSAL,
	} {
		assert.Equal(t, mode, (Caller{Identity: &flowstatev1.WorkloadIdentity{Mode: mode}}).Mode())
	}
}
