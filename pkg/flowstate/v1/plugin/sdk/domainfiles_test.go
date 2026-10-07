package sdk

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// domainFilePath matches a domain package's file, flowstate/<domain>/v1/<name>.proto,
// and not flowstate/v1/, the core.
var domainFilePath = regexp.MustCompile(`^flowstate/[a-z0-9_]+/v[0-9]+/[^/]+\.proto$`)

// TestEveryDomainFileIsEngineProvided is the sibling of flowstatev1's
// TestEveryFileOfTheSchemaIsProvided for the packages the packaging rule keeps
// out of flowstate/v1 (docs/ARCHITECTURE.md, "Proto packages: core versus
// domain").
//
// A domain file the engine links but this SDK does not name is one the SDK ships
// a copy of, and the host then reads a descriptor whose `cel` rules the linker
// strips from plugin-shipped files (#2396): the file's validation silently stops
// applying. The list naming them is written out, because Go cannot enumerate a
// package's variables, so this walks the registry for every
// flowstate/<domain>/v1 file and fails on one describeMessage still ships.
func TestEveryDomainFileIsEngineProvided(t *testing.T) {
	t.Parallel()

	var domain []protoreflect.FileDescriptor
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if domainFilePath.MatchString(file.Path()) {
			domain = append(domain, file)
		}

		return true
	})

	// A registry holding no domain file would make the loop below pass by
	// finding nothing; this package links both of these.
	paths := make([]string, 0, len(domain))
	for _, file := range domain {
		paths = append(paths, file.Path())
	}
	require.Contains(t, paths, "flowstate/decision/v1/decision.proto")
	require.Contains(t, paths, "flowstate/plugin/v1/plugin.proto")

	for _, file := range domain {
		messages := file.Messages()
		require.Positive(t, messages.Len(), "%s declares no message to describe", file.Path())

		md := messages.Get(0)
		mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
		require.NoError(t, err, file.Path())

		var msg proto.Message = mt.New().Interface()
		raw, name, err := describeMessage(msg, nil)
		require.NoError(t, err, file.Path())
		require.Equal(t, string(md.FullName()), name)
		require.Empty(t, raw, "%s is a domain file the engine has, yet the SDK ships %d bytes of descriptor for %s; add its File_… root to the hook in describeMessage", file.Path(), len(raw), name)
	}
}
