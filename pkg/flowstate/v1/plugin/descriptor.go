package plugin

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// maxDescriptorDepth bounds how deep an import graph may go.
//
// Breadth is bounded separately, by Config.MaxDescriptorFiles, because the two
// are different attacks: a depth bound does nothing about a file importing five
// hundred siblings, and a breadth bound does nothing about a chain five hundred
// long. Ask which resource the attacker controls, then bound that one.
const maxDescriptorDepth = 32

// messageDescriptor reconstructs the descriptor of the message a task manifest
// names, from the descriptors the plugin serialized alongside it.
//
// This is what makes a plugin task indistinguishable from a built-in one: the
// engine validates a workflow's inputs, an editor completes its fields, and
// generated documentation describes it, all from a descriptor — and a
// reconstructed one behaves exactly like a compiled-in one. Nothing about the
// plugin's code is loaded to get it.
//
// Three shapes are accepted, because a plugin may reasonably produce any of
// them:
//
//   - No descriptor bytes and no message name: the task declares no schema for
//     that side, which [flowstatev1.TaskDef] permits.
//   - No descriptor bytes but a message name: the plugin is reusing a type the
//     engine already has, such as one from flowstate's own schema.
//   - Descriptor bytes: either a serialized FileDescriptorProto, or a
//     FileDescriptorSet carrying the file and its dependencies.
//
// The last two forms cannot be told apart by inspecting the bytes — protobuf's
// wire format is permissive enough that each parses as the other — so both are
// attempted and the one that actually resolves the named message wins. Requiring
// a complete resolution rather than a plausible parse is what keeps that from
// being a guess.
func messageDescriptor(raw []byte, fullName string, cfg Config) (protoreflect.MessageDescriptor, error) {
	if fullName == "" {
		if len(raw) > 0 {
			return nil, fmt.Errorf("%w: carries a descriptor but names no message within it", ErrDescriptor)
		}
		return nil, nil
	}

	name := protoreflect.FullName(fullName)
	if !name.IsValid() {
		return nil, fmt.Errorf("%w: %q is not a valid message name", ErrDescriptor, truncate(fullName, 128))
	}

	if len(raw) == 0 {
		// A message the engine already knows. This is the case worth supporting
		// deliberately: a plugin whose task takes a flowstate.v1 type does not
		// have to ship a copy of a schema the engine compiled in.
		desc, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: names message %q with no descriptor, and this engine does not know that message",
				ErrDescriptor, truncate(fullName, 128),
			)
		}
		message, ok := desc.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, fmt.Errorf("%w: %q is not a message", ErrDescriptor, truncate(fullName, 128))
		}
		return message, nil
	}

	if len(raw) > cfg.MaxDescriptorBytes {
		return nil, fmt.Errorf(
			"%w: descriptor for %q is %d bytes, over the %d byte limit",
			ErrDescriptor, truncate(fullName, 128), len(raw), cfg.MaxDescriptorBytes,
		)
	}

	var attempts []error

	if files, err := parseDescriptorSet(raw, cfg.MaxDescriptorFiles); err == nil {
		message, err := resolveMessage(files, name, cfg)
		if err == nil {
			return message, nil
		}
		attempts = append(attempts, fmt.Errorf("as a FileDescriptorSet: %w", err))
	} else {
		attempts = append(attempts, fmt.Errorf("as a FileDescriptorSet: %w", err))
	}

	if files, err := parseDescriptorFile(raw); err == nil {
		message, err := resolveMessage(files, name, cfg)
		if err == nil {
			return message, nil
		}
		attempts = append(attempts, fmt.Errorf("as a FileDescriptorProto: %w", err))
	} else {
		attempts = append(attempts, fmt.Errorf("as a FileDescriptorProto: %w", err))
	}

	return nil, fmt.Errorf(
		"%w: could not reconstruct %q: %w",
		ErrDescriptor, truncate(fullName, 128), errors.Join(attempts...),
	)
}

// parseDescriptorSet reads the bytes as a FileDescriptorSet.
func parseDescriptorSet(raw []byte, maxFiles int) ([]*descriptorpb.FileDescriptorProto, error) {
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &set); err != nil {
		return nil, err
	}

	switch {
	case len(set.GetFile()) == 0:
		return nil, fmt.Errorf("no files")
	case len(set.GetFile()) > maxFiles:
		return nil, fmt.Errorf("%d files, over the %d file limit", len(set.GetFile()), maxFiles)
	}

	for _, file := range set.GetFile() {
		if file.GetName() == "" {
			return nil, fmt.Errorf("a file has no path")
		}
	}

	return set.GetFile(), nil
}

// parseDescriptorFile reads the bytes as a single FileDescriptorProto.
func parseDescriptorFile(raw []byte) ([]*descriptorpb.FileDescriptorProto, error) {
	var file descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(raw, &file); err != nil {
		return nil, err
	}
	if file.GetName() == "" {
		return nil, fmt.Errorf("the file has no path")
	}
	return []*descriptorpb.FileDescriptorProto{&file}, nil
}

// resolveMessage links a set of file descriptors and finds the named message in
// them.
func resolveMessage(files []*descriptorpb.FileDescriptorProto, name protoreflect.FullName, cfg Config) (protoreflect.MessageDescriptor, error) {
	linker := &linker{
		pending: make(map[string]*descriptorpb.FileDescriptorProto, len(files)),
		linked:  new(protoregistry.Files),
		linking: make(map[string]struct{}),
		max:     cfg.MaxDescriptorFiles,
	}

	for _, file := range files {
		if _, dup := linker.pending[file.GetName()]; dup {
			return nil, fmt.Errorf("file %q appears twice", truncate(file.GetName(), 128))
		}
		linker.pending[file.GetName()] = file
	}

	for _, file := range files {
		if _, err := linker.link(file.GetName(), 0); err != nil {
			return nil, err
		}
	}

	desc, err := linker.linked.FindDescriptorByName(name)
	if err != nil {
		return nil, fmt.Errorf("the descriptor does not define %q", truncate(string(name), 128))
	}

	message, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is a %T rather than a message", truncate(string(name), 128), desc)
	}

	return message, nil
}

// linker turns file descriptor protos into linked descriptors, resolving imports
// as it goes.
//
// The registry it builds is private to this one reconstruction and is never the
// global one. That is the important property: registering a plugin's descriptors
// globally would let a plugin add to — or collide with — the type registry the
// engine itself resolves against, which is a plugin reaching outside its process
// through the one channel that is supposed to be inert data.
type linker struct {
	pending map[string]*descriptorpb.FileDescriptorProto
	linked  *protoregistry.Files
	linking map[string]struct{}
	max     int
	count   int
}

// FindFileByPath implements [protodesc.Resolver], which is how protodesc asks
// for a file's imports while linking it.
//
// The engine's own files win over a plugin's for a path they share. A plugin
// that shipped its own copy of google/protobuf/timestamp.proto or of flowstate's
// schema gets the engine's definition, so it cannot redefine a type the engine
// validates against — and if the plugin's file genuinely disagrees with the
// engine's, linking fails and the plugin is refused, which is the right outcome
// for a plugin built against a schema this engine does not have.
func (l *linker) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if file, err := protoregistry.GlobalFiles.FindFileByPath(path); err == nil {
		return file, nil
	}
	return l.linked.FindFileByPath(path)
}

// FindDescriptorByName implements the rest of [protodesc.Resolver], which uses
// it to resolve field and extension types by name. It layers the same way
// [linker.FindFileByPath] does, and for the same reason.
func (l *linker) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	if desc, err := protoregistry.GlobalFiles.FindDescriptorByName(name); err == nil {
		return desc, nil
	}
	return l.linked.FindDescriptorByName(name)
}

// link resolves one file and everything it imports, registering the result.
func (l *linker) link(path string, depth int) (protoreflect.FileDescriptor, error) {
	if depth > maxDescriptorDepth {
		return nil, fmt.Errorf("imports nest more than %d deep", maxDescriptorDepth)
	}

	if file, err := l.FindFileByPath(path); err == nil {
		return file, nil
	}

	file, ok := l.pending[path]
	if !ok {
		return nil, fmt.Errorf(
			"imports %q, which is neither included in the descriptor nor known to this engine",
			truncate(path, 128),
		)
	}

	if _, cycle := l.linking[path]; cycle {
		return nil, fmt.Errorf("imports form a cycle through %q", truncate(path, 128))
	}
	l.linking[path] = struct{}{}
	defer delete(l.linking, path)

	if l.count++; l.count > l.max {
		return nil, fmt.Errorf("links more than %d files", l.max)
	}

	for _, dep := range file.GetDependency() {
		if _, err := l.link(dep, depth+1); err != nil {
			return nil, fmt.Errorf("%q: %w", truncate(path, 128), err)
		}
	}

	linked, err := protodesc.NewFile(file, l)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", truncate(path, 128), err)
	}

	if err := l.linked.RegisterFile(linked); err != nil {
		return nil, fmt.Errorf("%q: %w", truncate(path, 128), err)
	}

	return linked, nil
}
