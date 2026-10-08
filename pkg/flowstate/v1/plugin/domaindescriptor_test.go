package plugin

import (
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestADomainMessageIsResolvedFromTheEngineWithItsCELRulesIntact pins what
// docs/PLUGINS.md says of flowstate/<domain>/v1 files: a plugin names one with
// no descriptor bytes, the host resolves it from its own registry, and the
// expression rules the linker strips from plugin-shipped files (#2396) are still
// on it, because the file is the engine's and not the plugin's.
func TestADomainMessageIsResolvedFromTheEngineWithItsCELRulesIntact(t *testing.T) {
	t.Parallel()

	desc, err := messageDescriptor(nil, "flowstate.decision.v1.Decision", Config{}.withDefaults())
	require.NoError(t, err)
	require.NotNil(t, desc)

	rules, ok := proto.GetExtension(desc.Options(), validate.E_Message).(*validate.MessageRules)
	require.True(t, ok)
	require.NotEmpty(t, rules.GetCel(), "Decision ties an answer to its question with message-level cel rules; resolving it must keep them")
}

// TestTheChatFileIsResolvedFromTheEngineWithNoDescriptorBytes pins what
// flowstate.chat.v1 promises a chat plugin: it references
// flowstate/chat/v1/chat.proto with empty descriptor bytes, and the host
// resolves the message from its own registry with the protovalidate rules the
// plugin never had to copy.
func TestTheChatFileIsResolvedFromTheEngineWithNoDescriptorBytes(t *testing.T) {
	t.Parallel()

	desc, err := messageDescriptor(nil, "flowstate.chat.v1.Card", Config{}.withDefaults())
	require.NoError(t, err)
	require.NotNil(t, desc)
	require.Equal(t, "flowstate/chat/v1/chat.proto", desc.ParentFile().Path())

	oneofs := desc.Oneofs()
	require.Equal(t, 1, oneofs.Len())
	rules, ok := proto.GetExtension(oneofs.Get(0).Options(), validate.E_Oneof).(*validate.OneofRules)
	require.True(t, ok)
	require.True(t, rules.GetRequired(), "Card must name exactly one preset; resolving it must keep that rule")
}
