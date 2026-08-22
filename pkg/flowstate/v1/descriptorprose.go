package flowstatev1

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The comments a schema is written with, made available to a descriptor that
// travels.
//
// protoc strips SourceCodeInfo from what a .pb.go embeds, so the descriptor a
// process holds at run time carries shape and no prose. This repository's own
// schema works around that host-side by embedding a second artifact built with
// `buf build --exclude-imports`, read by
// [github.com/picatz/flowstate/pkg/flowstate/v1/protodoc]. A plugin has the same
// problem and had no equivalent: its author's field comments could not reach an
// editor, however well the .proto was written (#723).
//
// This is that same artifact, read by whoever is about to serialize a descriptor
// rather than by whoever renders it. The prose rides in the descriptor bytes the
// manifest already carried, so nothing new travels and no second channel exists
// to disagree with the first.

// The bounds on a serialized descriptor, in the package both sides of the
// plugin boundary import.
//
// They are the numbers [github.com/picatz/flowstate/pkg/flowstate/v1/plugin]'s
// Config applies to a descriptor arriving from a plugin, defined here so the
// side that *writes* one bounds it with the same value. The host's bound is not
// a substitute for this one — it is applied after these bytes have already been
// unmarshaled in the plugin's own process — and a second pair of constants would
// be one bound wearing two numbers.
const (
	// DefaultMaxDescriptorBytes bounds one serialized descriptor: the bytes a
	// plugin sends, and the descriptor set it reads for the comments to attach
	// to them.
	DefaultMaxDescriptorBytes = 1 << 20 // 1 MiB

	// DefaultMaxDescriptorFiles bounds how many files one of those may carry.
	// Depth bounds do not stop breadth explosions, so this bounds breadth and
	// the reader's own depth bound bounds depth.
	DefaultMaxDescriptorFiles = 256
)

// DescriptorProse holds the source comments of a `buf build` descriptor set,
// keyed by the file path they describe.
//
// The zero value and a nil pointer are both usable and both mean "no prose",
// which is the documented fallback rather than an error: a build that ships no
// descriptor set behaves exactly as every build did before this existed.
type DescriptorProse struct {
	// byPath holds only what this type is for. The shapes in the set are
	// deliberately dropped: the descriptor that travels is the one this process
	// linked and decodes with, and a set read off disk is a *documentation*
	// source, never a source of truth about a type's fields.
	byPath map[string]*descriptorpb.FileDescriptorProto
}

// ParseDescriptorProse reads a serialized FileDescriptorSet built with source
// info retained — the output of `buf build --exclude-imports -o <file>` — into
// the prose [MessageDescriptorBytesWithProse] attaches.
//
// Empty bytes parse to nil prose and no error, so an embedded artifact a build
// left empty degrades to the fallback rather than failing a plugin at startup.
// Bytes that are not a descriptor set, or a set carrying no file, are an error:
// that is an author's build being wrong about its own schema, and it is worth
// hearing about once at startup rather than never.
//
// This is exported, and an exported parser of a serialized descriptor is a
// parser somebody will hand bytes it did not build — a file discovered at run
// time, an artifact a build system fetched, a set from elsewhere. So it is
// bounded in the two resources those bytes decide: size, before anything is
// unmarshaled, and file count after, which the size bound is what makes finite.
// The numbers are [DefaultMaxDescriptorBytes] and [DefaultMaxDescriptorFiles] —
// the ones a host applies to the descriptor these comments end up inside — so a
// set too large to be accepted at the far end is refused here, at the author's
// own startup, rather than carried that far (#874 review).
func ParseDescriptorProse(raw []byte) (*DescriptorProse, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	if len(raw) > DefaultMaxDescriptorBytes {
		return nil, fmt.Errorf(
			"reading a descriptor set for its comments: it is %d bytes, over the %d byte limit",
			len(raw), DefaultMaxDescriptorBytes,
		)
	}

	set := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(raw, set); err != nil {
		return nil, fmt.Errorf("reading a descriptor set for its comments: %w", err)
	}
	if len(set.GetFile()) == 0 {
		return nil, fmt.Errorf("reading a descriptor set for its comments: it holds no files")
	}
	if len(set.GetFile()) > DefaultMaxDescriptorFiles {
		return nil, fmt.Errorf(
			"reading a descriptor set for its comments: it holds %d files, over the %d file limit",
			len(set.GetFile()), DefaultMaxDescriptorFiles,
		)
	}

	prose := &DescriptorProse{byPath: make(map[string]*descriptorpb.FileDescriptorProto, len(set.GetFile()))}
	for _, file := range set.GetFile() {
		if file.GetName() == "" {
			continue
		}
		prose.byPath[file.GetName()] = file
	}

	return prose, nil
}

// sourceInfoFor returns the comments describing a file, or nil when this prose
// has nothing to say about it.
//
// Nil is returned for three distinct reasons, and all three are the same answer
// on purpose: there is no prose at all, there is none for this path, or the file
// it describes is no longer the file being serialized. The last is the one worth
// having. A SourceCodeInfo location addresses a declaration by *index* — message
// 0, field 2 — so grafting one file's comments onto another's shape does not
// fail, it silently attributes a sentence to the wrong field, and a comment
// describing a neighbouring field is worse than no comment at all. Prose built
// from a .proto that has since moved on is therefore dropped rather than
// applied.
func (p *DescriptorProse) sourceInfoFor(fdp *descriptorpb.FileDescriptorProto) *descriptorpb.SourceCodeInfo {
	if p == nil || fdp == nil {
		return nil
	}

	described, ok := p.byPath[fdp.GetName()]
	if !ok {
		return nil
	}
	if !sameDeclarations(described, fdp) {
		return nil
	}

	return described.GetSourceCodeInfo()
}

// sameDeclarations reports whether two descriptions of one file declare the same
// things in the same order.
//
// Not [proto.Equal] on the pair, which is the obvious spelling and answers a
// different question: `buf build` records its own metadata in an unknown field
// and populates json_name where a linked descriptor need not, so two faithful
// descriptions of one unchanged .proto are routinely unequal. What matters here
// is narrower and is exactly what a SourceCodeInfo path walks — the ordered tree
// of messages, nested messages, enums, enum values and fields — so that is what
// is compared. Anything a comment cannot be attached to is not compared, because
// a difference there cannot misplace one.
func sameDeclarations(a, b *descriptorpb.FileDescriptorProto) bool {
	if a.GetPackage() != b.GetPackage() {
		return false
	}
	if len(a.GetMessageType()) != len(b.GetMessageType()) || len(a.GetEnumType()) != len(b.GetEnumType()) {
		return false
	}
	for i, msg := range a.GetMessageType() {
		if !sameMessage(msg, b.GetMessageType()[i]) {
			return false
		}
	}
	for i, enum := range a.GetEnumType() {
		if !sameEnum(enum, b.GetEnumType()[i]) {
			return false
		}
	}

	return true
}

// sameMessage is [sameDeclarations] for one message, including everything nested
// inside it.
func sameMessage(a, b *descriptorpb.DescriptorProto) bool {
	if a.GetName() != b.GetName() {
		return false
	}
	if len(a.GetField()) != len(b.GetField()) ||
		len(a.GetNestedType()) != len(b.GetNestedType()) ||
		len(a.GetEnumType()) != len(b.GetEnumType()) {
		return false
	}
	for i, field := range a.GetField() {
		other := b.GetField()[i]
		if field.GetName() != other.GetName() || field.GetNumber() != other.GetNumber() {
			return false
		}
	}
	for i, nested := range a.GetNestedType() {
		if !sameMessage(nested, b.GetNestedType()[i]) {
			return false
		}
	}
	for i, enum := range a.GetEnumType() {
		if !sameEnum(enum, b.GetEnumType()[i]) {
			return false
		}
	}

	return true
}

// sameEnum is [sameDeclarations] for one enum.
func sameEnum(a, b *descriptorpb.EnumDescriptorProto) bool {
	if a.GetName() != b.GetName() || len(a.GetValue()) != len(b.GetValue()) {
		return false
	}
	for i, value := range a.GetValue() {
		other := b.GetValue()[i]
		if value.GetName() != other.GetName() || value.GetNumber() != other.GetNumber() {
			return false
		}
	}

	return true
}
