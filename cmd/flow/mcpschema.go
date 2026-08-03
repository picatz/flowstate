package main

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The MCP tool schemas are derived, not written.
//
// docs/DSL.md decided this before the surface existed: "MCP is generated, not
// written — `flow mcp` serves the same services as MCP tools with schemas
// derived from the protos, so there is no hand-maintained tool list to fall
// behind the engine." This file is that derivation: a request message's
// descriptor rendered as the JSON Schema of its protojson encoding, which is
// the encoding the handler feeds the arguments to.
//
// Derivation is what makes the surface honest. A field added to a request
// appears in the tool the day it is generated; a hand-kept schema would be the
// README task table before the test that pinned it to the registry — right
// until the first change, then quietly wrong for everyone.

// runLocalInputSchema is the one schema on this surface that is written rather
// than derived, because the tool it describes is the one that is not an RPC.
//
// flowstate_run_local is the local driver — a process executing a file — and
// deliberately has no request message: giving it one would make it a service
// capability, which is the thing it exists not to be. So there is no descriptor
// to derive from, and this is written out in the same shape [messageSchema]
// produces so a client sees one style across the tool list.
//
// It stays small on purpose, and the fields that are missing are the design.
// There is no `vars` — workflow variables are written in the file, where an
// author would put them and where `flow validate` can see them. There is nothing
// naming a URL, a secret, or a policy: what a run may reach is decided by the
// flags `flow mcp` was started with, so no argument here can widen it.
//
// `inputs` is not an exception to that, which is why it is here and `vars` is
// not. An input is a name the *submitted source* declared, of a type it named,
// checked against those declarations before the run starts — so this argument
// widens nothing the file did not already offer, and a name the file does not
// declare is refused rather than quietly bound.
func runLocalInputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"source": map[string]any{
				"type": "string",
				"description": "The Flowfile YAML to execute, exactly as it would be written to disk — " +
					"including the `edition:` line. It is parsed and validated first; a file with any " +
					"diagnostic is reported and not executed.",
			},
			"inputs": map[string]any{
				"type": "object",
				"additionalProperties": map[string]any{
					"description": "The value for one declared input, as JSON of the declared type.",
				},
				"description": "Arguments the run is started with, keyed by the name the source declares " +
					"under `inputs:` — the same thing `flow run local --input-file` supplies. Values are " +
					"JSON of the declared type. A name the source does not declare, a value of the wrong " +
					"type, or a required input left out is refused before any step runs, with the declared " +
					"names listed.",
			},
			"signals": map[string]any{
				"type": "object",
				"additionalProperties": map[string]any{
					"type":        "object",
					"description": "The payload a wait_for_signal step reads as ${steps.<id>.payload.<key>}.",
				},
				"description": "Answers for wait_for_signal steps, by signal name, delivered before the run " +
					"starts — the same thing `flow run local --signal name=json` does. A gate reached " +
					"later still finds its answer waiting. A gate with no answer here blocks until its " +
					"own timeout, or until the call's.",
			},
		},
		"required": []any{"source"},
		// Refused rather than ignored, for the reason [messageSchema] gives: a
		// misspelled argument silently dropped is a tool that "worked" and did
		// something other than what was asked.
		"additionalProperties": false,
	}
}

// schemaForMessage renders a message descriptor as a 2020-12 JSON Schema object
// describing the message's protojson encoding: camelCase names, enums by name,
// bytes as base64, 64-bit integers as strings.
func schemaForMessage(md protoreflect.MessageDescriptor) map[string]any {
	return messageSchema(md, map[protoreflect.FullName]bool{})
}

// messageSchema is the recursion, carrying the messages already on the path.
//
// The schema graph is genuinely cyclic — a Value holds Values — and JSON Schema
// rendered this way has no back-references, so a cycle is cut with a permissive
// object rather than followed. That trades precision an agent rarely needs at
// that depth for a schema that terminates.
func messageSchema(md protoreflect.MessageDescriptor, visiting map[protoreflect.FullName]bool) map[string]any {
	properties := map[string]any{}
	var required []string

	visiting[md.FullName()] = true
	defer delete(visiting, md.FullName())

	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		properties[fd.JSONName()] = fieldSchema(fd, visiting)

		// From the same protovalidate rules the server enforces — required
		// fields and repeated fields with a minimum — so a client validating
		// against this schema refuses {} where the tool boundary would. One
		// definition of "required", read here as the task schema already reads
		// it, rather than a judgement this file makes on its own.
		if v1.RequiredInput(fd) {
			required = append(required, fd.JSONName())
		}
	}

	schema := map[string]any{
		"type":       "object",
		"properties": properties,
		// Refused rather than ignored, because the caller is a model: a
		// misspelled argument silently dropped is a tool that "worked" and did
		// something other than what was asked, which is the failure mode a
		// schema exists to prevent.
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}

	return schema
}

func fieldSchema(fd protoreflect.FieldDescriptor, visiting map[protoreflect.FullName]bool) map[string]any {
	if fd.IsMap() {
		return map[string]any{
			"type":                 "object",
			"additionalProperties": valueSchema(fd.MapValue(), visiting),
		}
	}

	if fd.IsList() {
		return map[string]any{
			"type":  "array",
			"items": valueSchema(fd, visiting),
		}
	}

	return valueSchema(fd, visiting)
}

func valueSchema(fd protoreflect.FieldDescriptor, visiting map[protoreflect.FullName]bool) map[string]any {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return map[string]any{"type": "boolean"}

	case protoreflect.StringKind:
		return map[string]any{"type": "string"}

	case protoreflect.BytesKind:
		return map[string]any{"type": "string", "contentEncoding": "base64"}

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return map[string]any{"type": "integer"}

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		// protojson writes 64-bit integers as strings, so the schema says what
		// the decoder will actually accept.
		return map[string]any{"type": "string", "pattern": "^-?[0-9]+$"}

	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return map[string]any{"type": "number"}

	case protoreflect.EnumKind:
		values := fd.Enum().Values()
		names := make([]any, 0, values.Len())
		for i := 0; i < values.Len(); i++ {
			names = append(names, string(values.Get(i).Name()))
		}

		return map[string]any{"type": "string", "enum": names}

	case protoreflect.MessageKind, protoreflect.GroupKind:
		// The two well-known types protojson spells as strings.
		switch fd.Message().FullName() {
		case "google.protobuf.Timestamp":
			return map[string]any{"type": "string", "format": "date-time"}
		case "google.protobuf.Duration":
			return map[string]any{"type": "string"}
		}

		if visiting[fd.Message().FullName()] {
			return map[string]any{"type": "object"}
		}

		return messageSchema(fd.Message(), visiting)

	default:
		return map[string]any{}
	}
}
