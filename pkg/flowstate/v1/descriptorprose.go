package flowstatev1

import (
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The comments a schema is written with, made available to a descriptor that
// travels.
//
// protoc strips SourceCodeInfo from what a .pb.go embeds, so the descriptor a
// process holds at run time carries shape and no prose. protoc-gen-flowstate-doc
// generates the comments as Go source instead, registered with
// [github.com/picatz/flowstate/pkg/flowstate/v1/protodoc/protodocimpl]; this
// repository's own schema is read that way, by
// [github.com/picatz/flowstate/pkg/flowstate/v1/protodoc]. A plugin has the same
// problem: its author's field comments could not reach an editor, however well
// the .proto was written (#723).
//
// Here the same comments are read by whoever is about to serialize a
// descriptor rather than by whoever renders it (#2148): they ride in the
// descriptor bytes the manifest already carried, so nothing new travels and no
// second channel exists to disagree with the first.

// The bounds on a serialized descriptor, in the package both sides of the
// plugin boundary import.
//
// They are the numbers [github.com/picatz/flowstate/pkg/flowstate/v1/plugin]'s
// Config applies to a descriptor arriving from a plugin, defined here so the
// side that *writes* one bounds it with the same value: a plugin whose
// descriptor, comments included, is too large for a host to accept is refused
// at its own startup rather than at the host. A second pair of constants would
// be one bound wearing two numbers.
const (
	// DefaultMaxDescriptorBytes bounds one serialized descriptor.
	DefaultMaxDescriptorBytes = 1 << 20 // 1 MiB

	// DefaultMaxDescriptorFiles bounds how many files one of those may carry.
	// Depth bounds do not stop breadth explosions, so this bounds breadth and
	// the reader's own depth bound bounds depth.
	DefaultMaxDescriptorFiles = 256
)

// CommentLookup returns the raw leading comment recorded for a declaration and
// the path of the .proto file that declared it, or false when none is.
// [github.com/picatz/flowstate/pkg/flowstate/v1/protodoc/protodocimpl.Lookup],
// where the files protoc-gen-flowstate-doc generates register their comments,
// is the one the plugin SDK uses.
//
// Comments are looked up by full name, never by position, so one that is
// present is attached to the declaration it was written for; and only when the
// lookup attributes it to the file being serialized, so a same-named
// declaration elsewhere cannot lend it prose.
type CommentLookup func(name protoreflect.FullName) (comment, path string, ok bool)

// lookedUpSourceInfo builds the SourceCodeInfo a linked file would have
// carried, from the comments lookup has for it, or nil when it has none.
//
// A location is only as precise as it needs to be for a reader to attach the
// comment: its path addresses the declaration, and its span is the smallest
// valid one, since no line numbers were generated. A comment is attached only
// when lookup attributes it to this file, so a same-named declaration elsewhere
// cannot lend it prose.
func lookedUpSourceInfo(file protoreflect.FileDescriptor, lookup CommentLookup) *descriptorpb.SourceCodeInfo {
	if file == nil || lookup == nil {
		return nil
	}

	var locations []*descriptorpb.SourceCodeInfo_Location
	add := func(d protoreflect.Descriptor, path []int32) {
		leading, from, ok := lookup(d.FullName())
		if !ok || from != file.Path() {
			return
		}
		locations = append(locations, &descriptorpb.SourceCodeInfo_Location{
			Path:            slices.Clone(path),
			Span:            []int32{0, 0, 0},
			LeadingComments: proto.String(leading),
		})
	}

	enum := func(e protoreflect.EnumDescriptor, path []int32) {
		add(e, path)
		for i := range e.Values().Len() {
			add(e.Values().Get(i), append(path, sourcePath.enumValue, int32(i)))
		}
	}
	var message func(m protoreflect.MessageDescriptor, path []int32)
	message = func(m protoreflect.MessageDescriptor, path []int32) {
		add(m, path)
		for i := range m.Fields().Len() {
			add(m.Fields().Get(i), append(path, sourcePath.messageField, int32(i)))
		}
		for i := range m.Oneofs().Len() {
			add(m.Oneofs().Get(i), append(path, sourcePath.messageOneof, int32(i)))
		}
		for i := range m.Extensions().Len() {
			add(m.Extensions().Get(i), append(path, sourcePath.messageExtension, int32(i)))
		}
		for i := range m.Messages().Len() {
			message(m.Messages().Get(i), append(path, sourcePath.messageNested, int32(i)))
		}
		for i := range m.Enums().Len() {
			enum(m.Enums().Get(i), append(path, sourcePath.messageEnum, int32(i)))
		}
	}

	for i := range file.Messages().Len() {
		message(file.Messages().Get(i), []int32{sourcePath.fileMessage, int32(i)})
	}
	for i := range file.Enums().Len() {
		enum(file.Enums().Get(i), []int32{sourcePath.fileEnum, int32(i)})
	}
	for i := range file.Extensions().Len() {
		add(file.Extensions().Get(i), []int32{sourcePath.fileExtension, int32(i)})
	}
	for i := range file.Services().Len() {
		service := file.Services().Get(i)
		path := []int32{sourcePath.fileService, int32(i)}
		add(service, path)
		for j := range service.Methods().Len() {
			add(service.Methods().Get(j), append(path, sourcePath.serviceMethod, int32(j)))
		}
	}

	if len(locations) == 0 {
		return nil
	}
	return &descriptorpb.SourceCodeInfo{Location: locations}
}

// sourcePath holds the descriptor.proto field numbers a SourceCodeInfo path is
// built from, read off descriptorpb's own descriptors rather than written out,
// so they cannot disagree with the schema that defines them.
var sourcePath = struct {
	fileMessage, fileEnum, fileService, fileExtension                        int32
	messageField, messageNested, messageEnum, messageExtension, messageOneof int32
	enumValue, serviceMethod                                                 int32
}{
	fileMessage:      fieldNumber(&descriptorpb.FileDescriptorProto{}, "message_type"),
	fileEnum:         fieldNumber(&descriptorpb.FileDescriptorProto{}, "enum_type"),
	fileService:      fieldNumber(&descriptorpb.FileDescriptorProto{}, "service"),
	fileExtension:    fieldNumber(&descriptorpb.FileDescriptorProto{}, "extension"),
	messageField:     fieldNumber(&descriptorpb.DescriptorProto{}, "field"),
	messageNested:    fieldNumber(&descriptorpb.DescriptorProto{}, "nested_type"),
	messageEnum:      fieldNumber(&descriptorpb.DescriptorProto{}, "enum_type"),
	messageExtension: fieldNumber(&descriptorpb.DescriptorProto{}, "extension"),
	messageOneof:     fieldNumber(&descriptorpb.DescriptorProto{}, "oneof_decl"),
	enumValue:        fieldNumber(&descriptorpb.EnumDescriptorProto{}, "value"),
	serviceMethod:    fieldNumber(&descriptorpb.ServiceDescriptorProto{}, "method"),
}

func fieldNumber(m proto.Message, name protoreflect.Name) int32 {
	return int32(m.ProtoReflect().Descriptor().Fields().ByName(name).Number())
}
