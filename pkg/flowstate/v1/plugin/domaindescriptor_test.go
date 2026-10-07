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
