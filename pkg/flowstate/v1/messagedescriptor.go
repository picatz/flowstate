package flowstatev1

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Serializing a message descriptor so it can cross a boundary, in one place.
//
// A [protoreflect.MessageDescriptor] is a resolved view of a type registry and
// does not survive serialization — the reason [TaskDef] is a Go struct rather
// than a schema message. What does survive is the file descriptors it was
// linked from, which is how a plugin ships its task's schema to a host
// (flowstate.plugin.v1.TaskManifest.input_descriptor) and, since #710, how a
// host ships a task's schema to a reader that cannot launch one
// ([TaskDescription.input_descriptor]).
//
// Both directions want the identical serialization, so there is one of it here
// rather than one per direction. The plugin SDK's own describeMessage is this
// function with plugin.proto named as a file the host also has.

// MessageDescriptorBytes serializes a message's file descriptor and everything
// it imports that the reader is not already known to have, and returns that
// alongside the message's full name.
//
// Dependencies any Flowstate build has are left out deliberately, and the set
// of those is derived rather than listed: it is the transitive imports of
// flowstate's own schema files, which anything speaking this schema has
// compiled in. That keeps a descriptor small — a task whose input references a
// flowstate type would otherwise carry protobuf's, protovalidate's and CEL's
// descriptors along with it — without hardcoding an assumption about the reader
// that could quietly stop being true. A caller that knows its reader has more
// than that names the extra roots in alsoProvided.
//
// Empty bytes with a name returned is the ordinary case and not a failure: it
// says every file the message needs is one the reader already has, so there is
// nothing to send but the name. That is one of the three shapes the
// reconstruction side accepts (see pkg/flowstate/v1/plugin's messageDescriptor).
//
// The bytes are marshaled deterministically, because a catalog document built
// from them is a thing people check in and diff: two runs over one unchanged
// task must produce one file.
func MessageDescriptorBytes(md protoreflect.MessageDescriptor, alsoProvided ...protoreflect.FileDescriptor) ([]byte, string, error) {
	return MessageDescriptorBytesWithProse(md, nil, alsoProvided...)
}

// MessageDescriptorBytesWithProse is [MessageDescriptorBytes] with the schema's
// own comments carried along.
//
// The compiled-in descriptor a caller holds has none: protoc strips
// SourceCodeInfo from what a .pb.go embeds, which is why this repository's host
// side keeps its prose in a separate `buf build --exclude-imports` artifact
// (see [github.com/picatz/flowstate/pkg/flowstate/v1/protodoc]). A plugin had no
// equivalent, so a plugin author's field comments reached nobody's editor however
// well the .proto was written (#723). Prose here is that same artifact, read from
// the plugin's own build, grafted onto the descriptors this function was already
// sending — the bytes were always able to carry comments; nothing was putting any
// in.
//
// A nil prose is the documented fallback and behaves exactly as
// [MessageDescriptorBytes] does: shape travels, prose does not, and the reader
// renders one paragraph fewer rather than an error. So does a prose that
// describes some other file, or one whose file has drifted from the compiled-in
// one — see [DescriptorProse.sourceInfoFor].
func MessageDescriptorBytesWithProse(md protoreflect.MessageDescriptor, prose *DescriptorProse, alsoProvided ...protoreflect.FileDescriptor) ([]byte, string, error) {
	if md == nil {
		return nil, "", nil
	}

	fullName := string(md.FullName())

	provided := engineProvidedFiles()
	if len(alsoProvided) > 0 {
		// Copied rather than added to: the cached set is shared by every
		// caller, and a caller naming an extra root must not widen what the
		// next one leaves out.
		widened := make(map[string]struct{}, len(provided)+len(alsoProvided))
		for path := range provided {
			widened[path] = struct{}{}
		}
		for _, root := range alsoProvided {
			walkImports(root, widened)
		}
		provided = widened
	}

	set := &descriptorpb.FileDescriptorSet{}
	seen := make(map[string]struct{})

	var collect func(file protoreflect.FileDescriptor)
	collect = func(file protoreflect.FileDescriptor) {
		path := file.Path()
		if _, done := seen[path]; done {
			return
		}
		seen[path] = struct{}{}

		if _, known := provided[path]; known {
			return
		}

		imports := file.Imports()
		for i := range imports.Len() {
			collect(imports.Get(i).FileDescriptor)
		}

		fdp := protodesc.ToFileDescriptorProto(file)

		// Set rather than assigned: a file that already carries source info is
		// one reconstructed from bytes that carried it — a plugin's descriptor
		// on its way into a catalog document (#854) — and overwriting that with
		// a nil this caller has no prose for would strip on the second hop what
		// survived the first.
		if info := prose.sourceInfoFor(fdp); info != nil {
			fdp.SourceCodeInfo = info
		}

		set.File = append(set.File, fdp)
	}

	collect(md.ParentFile())

	if len(set.File) == 0 {
		return nil, fullName, nil
	}

	// One file goes back as a bare FileDescriptorProto rather than as a
	// FileDescriptorSet holding one, which is the second of the two shapes the
	// reconstruction side accepts and the shape a plugin shipping a single file
	// sends.
	//
	// Not a preference about spelling: the set's field tag and length prefix
	// make the same descriptor two bytes larger, and those bytes are measured
	// against the same MaxDescriptorBytes on both sides. A plugin whose raw
	// FileDescriptorProto was accepted at launch with the bound set to exactly
	// its encoded length would then have its own descriptor refused as a
	// catalog entry — a round trip the framing broke rather than the descriptor
	// (#854 review). Re-serializing a linked file is otherwise byte-stable, so
	// dropping the wrapper is what makes the bound mean the same thing in both
	// directions.
	var (
		raw []byte
		err error
	)
	if len(set.File) == 1 {
		raw, err = (proto.MarshalOptions{Deterministic: true}).Marshal(set.File[0])
	} else {
		raw, err = (proto.MarshalOptions{Deterministic: true}).Marshal(set)
	}
	if err != nil {
		return nil, "", fmt.Errorf("serializing the descriptor of %s: %w", fullName, err)
	}

	return raw, fullName, nil
}

// engineProvidedFiles returns the descriptor paths anything speaking this
// schema has, computed as the transitive imports of flowstate's own schema.
var engineProvidedFiles = sync.OnceValue(func() map[string]struct{} {
	provided := make(map[string]struct{})

	// Every file of flowstate/v1, not merely the ones one of them imports: a
	// build links the whole generated package, so the set it has is the whole
	// schema. This list is the twelve files flowstate/v1 is spelled in; a
	// thirteenth belongs here the day it exists.
	//
	// flowstate/plugin/v1 is deliberately absent, and not by oversight: this
	// package cannot name it (plugin.proto imports these files, so the Go
	// package importing this one), and a reader of a catalog is not
	// necessarily a plugin host. The SDK, which is on the other side of that
	// import and does talk to a host, names it through alsoProvided.
	for _, file := range []protoreflect.FileDescriptor{
		File_flowstate_v1_catalog_proto,
		File_flowstate_v1_diagnostics_proto,
		File_flowstate_v1_identity_proto,
		File_flowstate_v1_reports_proto,
		File_flowstate_v1_run_proto,
		File_flowstate_v1_schedule_proto,
		File_flowstate_v1_service_proto,
		File_flowstate_v1_signal_proto,
		File_flowstate_v1_task_proto,
		File_flowstate_v1_trigger_proto,
		File_flowstate_v1_value_proto,
		File_flowstate_v1_workflow_proto,
	} {
		walkImports(file, provided)
	}

	return provided
})

// walkImports records a file's path and every path it transitively imports.
func walkImports(file protoreflect.FileDescriptor, into map[string]struct{}) {
	if _, done := into[file.Path()]; done {
		return
	}
	into[file.Path()] = struct{}{}

	imports := file.Imports()
	for i := range imports.Len() {
		walkImports(imports.Get(i).FileDescriptor, into)
	}
}
