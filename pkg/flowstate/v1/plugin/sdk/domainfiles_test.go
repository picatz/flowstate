package sdk

import (
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// domainProtoSources lists the proto files under proto/flowstate/<domain>/v1/
// in the repository, as the paths a descriptor would give them
// (flowstate/<domain>/v1/<name>.proto). It reads the sources rather than the
// registry, so a domain package that was written and generated but never
// linked into this binary is still found.
func domainProtoSources(t *testing.T) []string {
	t.Helper()

	root := filepath.Join("..", "..", "..", "..", "..", "proto")
	var paths []string
	err := filepath.WalkDir(filepath.Join(root, "flowstate"), func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".proto") {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		// flowstate/<domain>/v<N>/<name>.proto: four segments, the second a
		// domain. The core, flowstate/v1/..., has three and is not one.
		if parts := strings.Split(rel, "/"); len(parts) == 4 && strings.HasPrefix(parts[2], "v") {
			paths = append(paths, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reading the proto sources: %v", err)
	}
	slices.Sort(paths)
	return paths
}

// TestEveryDomainFileIsEngineProvided is the sibling of flowstatev1's
// TestEveryFileOfTheSchemaIsProvided for the packages the packaging rule keeps
// out of flowstate/v1 (docs/ARCHITECTURE.md, "Proto packages: core versus
// domain").
//
// A domain file the engine has but this SDK does not name is one the SDK ships a
// copy of, and the host then reads a descriptor whose `cel` rules the linker
// strips from plugin-shipped files (#2396): the file's validation silently stops
// applying. The expected set comes from the proto sources, not from the registry
// being checked, so a domain package that is not linked here fails the test
// instead of being absent from it.
func TestEveryDomainFileIsEngineProvided(t *testing.T) {
	t.Parallel()

	sources := domainProtoSources(t)
	// An empty walk would make the loop below pass by finding nothing.
	require.Contains(t, sources, "flowstate/chat/v1/chat.proto")
	require.Contains(t, sources, "flowstate/decision/v1/decision.proto")
	require.Contains(t, sources, "flowstate/plugin/v1/plugin.proto")

	for _, source := range sources {
		file, err := protoregistry.GlobalFiles.FindFileByPath(source)
		require.NoError(t, err, "%s is a domain file in proto/ but is not linked into the SDK; import its generated package where the others are, and add its File_… root to the hook in describeMessage", source)
		require.Equal(t, source, path.Clean(file.Path()))

		messages := file.Messages()
		require.Positive(t, messages.Len(), "%s declares no message to describe", source)

		md := messages.Get(0)
		mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
		require.NoError(t, err, source)

		var msg proto.Message = mt.New().Interface()
		raw, name, err := describeMessage(msg, nil)
		require.NoError(t, err, source)
		require.Equal(t, string(md.FullName()), name)
		require.Empty(t, raw, "%s is a domain file the engine has, yet the SDK ships %d bytes of descriptor for %s; add its File_… root to the hook in describeMessage", source, len(raw), name)
	}
}
