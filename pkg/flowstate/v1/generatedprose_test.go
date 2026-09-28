package flowstatev1

import (
	"iter"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	// The engine's own generated comments, registered at init: the largest
	// schema this repository has to check the path construction against.
	_ "github.com/picatz/flowstate/pkg/flowstate/v1/protodoc"
)

// Generated prose is attached by computing each declaration's SourceCodeInfo
// path by hand. protobuf's own SourceLocations().ByDescriptor computes the same
// path independently, so rebuilding every schema file with the generated info
// and asking it for each declaration's comment checks every path this package
// builds: a wrong index or field number would give some declaration another's
// comment, or none.
func TestLookedUpSourceInfoAttachesEveryCommentToItsDeclaration(t *testing.T) {
	var files, comments int
	protoregistry.GlobalFiles.RangeFiles(func(linked protoreflect.FileDescriptor) bool {
		if !strings.HasPrefix(linked.Path(), "flowstate/v1/") {
			return true
		}
		files++

		fdp := protodesc.ToFileDescriptorProto(linked)
		fdp.SourceCodeInfo = lookedUpSourceInfo(linked, registry.Lookup)
		rebuilt, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
		require.NoError(t, err, linked.Path())

		for d := range declarations(rebuilt) {
			want, path, ok := registry.Lookup(d.FullName())
			if !ok || path != linked.Path() {
				want = ""
			}
			got := rebuilt.SourceLocations().ByDescriptor(d).LeadingComments
			assert.Equal(t, want, got, "comment attached to %s", d.FullName())
			if got != "" {
				comments++
			}
		}
		return true
	})

	// Floors, so a walk that silently visited nothing cannot pass.
	assert.GreaterOrEqual(t, files, 17, "schema files checked")
	assert.GreaterOrEqual(t, comments, 900, "comments checked")
}

// A file nobody generated comments for travels without source info, exactly as
// it would with nil prose.
func TestLookedUpSourceInfoForAnUndocumentedFileIsNil(t *testing.T) {
	file, err := protoregistry.GlobalFiles.FindFileByPath("google/protobuf/struct.proto")
	require.NoError(t, err)
	assert.Nil(t, lookedUpSourceInfo(file, registry.Lookup))
	assert.Nil(t, lookedUpSourceInfo(nil, registry.Lookup))
	assert.Nil(t, DescriptorProseFrom(nil), "a nil lookup is nil prose")
}

// declarations yields every declaration of a file that a comment can attach to.
func declarations(file protoreflect.FileDescriptor) iter.Seq[protoreflect.Descriptor] {
	return func(yield func(protoreflect.Descriptor) bool) {
		var message func(protoreflect.MessageDescriptor) bool
		enum := func(e protoreflect.EnumDescriptor) bool {
			if !yield(e) {
				return false
			}
			for i := range e.Values().Len() {
				if !yield(e.Values().Get(i)) {
					return false
				}
			}
			return true
		}
		message = func(m protoreflect.MessageDescriptor) bool {
			if !yield(m) {
				return false
			}
			for i := range m.Fields().Len() {
				if !yield(m.Fields().Get(i)) {
					return false
				}
			}
			for i := range m.Oneofs().Len() {
				if !yield(m.Oneofs().Get(i)) {
					return false
				}
			}
			for i := range m.Extensions().Len() {
				if !yield(m.Extensions().Get(i)) {
					return false
				}
			}
			for i := range m.Messages().Len() {
				if !message(m.Messages().Get(i)) {
					return false
				}
			}
			for i := range m.Enums().Len() {
				if !enum(m.Enums().Get(i)) {
					return false
				}
			}
			return true
		}
		for i := range file.Messages().Len() {
			if !message(file.Messages().Get(i)) {
				return
			}
		}
		for i := range file.Enums().Len() {
			if !enum(file.Enums().Get(i)) {
				return
			}
		}
		for i := range file.Extensions().Len() {
			if !yield(file.Extensions().Get(i)) {
				return
			}
		}
		for i := range file.Services().Len() {
			s := file.Services().Get(i)
			if !yield(s) {
				return
			}
			for j := range s.Methods().Len() {
				if !yield(s.Methods().Get(j)) {
					return
				}
			}
		}
	}
}
