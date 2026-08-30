package mcp

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
				"description": "The Flowfile YAML to execute, exactly as it would be written to disk, " +
					"including the `edition:` line. It is parsed and validated first; a file with any " +
					"diagnostic is reported and not executed.",
			},
			"inputs": map[string]any{
				"type": "object",
				"additionalProperties": map[string]any{
					"description": "The value for one declared input, as JSON of the declared type.",
				},
				"description": "Arguments the run is started with, keyed by the name the source declares " +
					"under `inputs:`: the same thing `flow run local --input-file` supplies. Values are " +
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
					"starts: the same thing `flow run local --signal name=json` does. A gate reached " +
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

// testInputSchema is flowstate_test's schema, written for the same reason
// [runLocalInputSchema] is: the tool it describes is not an RPC, so there is
// no request message to derive it from.
//
// Two fields, deliberately the same shape as [runLocalInputSchema]'s single
// `source`: `workflow` is the Flowfile under test and `tests` is a
// `*.test.yaml` document naming the cases to run against it — bytes standing
// in for the two paths `flow test` would otherwise take, in from the same
// call. Nothing here widens what a run may reach, because nothing a stubbed
// run does can reach anything: see [testToolDescription].
func testInputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"workflow": map[string]any{
				"type": "string",
				"description": "The Flowfile YAML under test, exactly as it would be written to disk, " +
					"including the `edition:` line.",
			},
			"tests": map[string]any{
				"type": "string",
				"description": "A `*.test.yaml` document: `tests:` names one or more cases, each with an " +
					"optional `inputs:`, `stubs:` replacing task behavior, `signals:` scripting what a " +
					"wait_for_signal step receives and when (each with an optional `sender:` naming who " +
					"it stands in for), an optional `starter:` naming who the run starts as - what a " +
					"`signals:` policy's `distinct_from_starter:` compares a sender against - and an " +
					"`expect:` the run must satisfy. A " +
					"case's own `workflow:` field is accepted, for compatibility with a file written to " +
					"disk, but is never consulted here: every case runs against the `workflow` argument " +
					"above, not a sibling file.",
			},
		},
		"required": []any{"workflow", "tests"},
		// Refused rather than ignored, for the reason [messageSchema] gives: a
		// misspelled argument silently dropped is a tool that "worked" and did
		// something other than what was asked.
		"additionalProperties": false,
	}
}

// maxSchemaNodes bounds the total number of schema objects one projection
// emits, across the whole walk rather than along any one path.
//
// The cycle cut below bounds *recursion*, which is not the same thing and does
// not imply it. `visiting` holds the messages on the current path and forgets
// each one on the way back out, so a descriptor whose type graph is a DAG — no
// cycle anywhere — re-expands every shared message once per distinct path that
// reaches it.
//
// Measured on the unbounded projection, over descriptors of N messages each
// holding four fields of the next message's type — every one of them acyclic,
// so the cut never fires once:
//
//	depth  6   601 B descriptor     3.6ms
//	depth  8   789 B descriptor    89.6ms
//	depth  9   883 B descriptor   208.0ms
//	depth 10   982 B descriptor     1.15s
//
// Four to five times per level, from descriptors that grow by about a hundred
// bytes each — 4^depth objects, which is a projection whose *memory* leaves the
// machine before its running time becomes interesting. That is CLAUDE.md's
// billion-laughs shape exactly: breadth multiplying at every level, at a depth
// of nothing. It is why the bound counts nodes.
//
// A node is one JSON object in the rendered schema, which is deliberately the
// unit a *client* can count rather than one this file keeps privately. An
// earlier draft budgeted expanded messages instead, and the two numbers were
// not the same number — 955 messages for SignalWithStart's request against
// 5,068 objects in the schema it produces — so a test asserting the bound over
// the returned artifact disagreed with a budget that believed it was holding.
// A bound only its own accounting can see is not one anybody else can check.
//
// The number is read off what real input needs, the way the catalog bounds in
// [plugin.Config] are. The largest schema this surface advertises today is
// SignalWithStart's request at 5,068 nodes, and
// [TestTheAdvertisedSchemasStayWellUnderTheNodeBound] fails once any advertised
// schema passes a quarter of the bound. That is the direction the failure has
// to point: exhausting the budget truncates, so a *real* schema growing into
// the bound must break a test here rather than quietly ship a tool whose
// arguments are half described.
//
// Descriptors reaching [SchemaForMessage] are this binary's own today — the
// service methods in [WorkflowServiceMethods] — and the bound is here anyway,
// because "the input is trusted" is a property of today's callers rather than
// of this function, and the neighbouring surface that already admits
// third-party descriptors under bounds ([plugin.TaskDefsFromCatalog], #854) is
// one call away from being one of them.
const maxSchemaNodes = 50_000

// schemaBudget carries what the walk must remember: the messages on the current
// path, and how many more schema objects the whole projection may emit.
type schemaBudget struct {
	visiting map[protoreflect.FullName]bool
	left     int
}

// take reserves n nodes, reporting whether there were n to reserve. Every map
// this file puts into a schema is reserved before it is built, so the objects
// in the returned schema are exactly the ones the budget paid for — which is
// what lets the bound be asserted over the artifact.
func (b *schemaBudget) take(n int) bool {
	if b.left < n {
		return false
	}
	b.left -= n
	return true
}

// SchemaForMessage renders a message descriptor as a 2020-12 JSON Schema object
// describing the message's protojson encoding: camelCase names, enums by name,
// bytes as base64, 64-bit integers as strings.
//
// The projection is bounded by [maxSchemaNodes] and is a pure function of the
// descriptor: fields are walked in declaration order, so the same descriptor
// yields the same schema, budget exhaustion included.
func SchemaForMessage(md protoreflect.MessageDescriptor) map[string]any {
	return messageSchema(md, &schemaBudget{
		visiting: map[protoreflect.FullName]bool{},
		left:     maxSchemaNodes,
	})
}

// messageSchema is the recursion, carrying the messages already on the path and
// the projection's remaining node budget. The cycle cut lives in [valueSchema],
// beside the budget check it shares its shape with.
//
// A nil return anywhere in this file means the budget is spent: the caller
// leaves that field out rather than describing it, and the message holding it
// stops claiming to describe itself completely.
func messageSchema(md protoreflect.MessageDescriptor, budget *schemaBudget) map[string]any {
	// Two objects: this schema and its properties map. Reserved before either
	// is built, so nothing is emitted that the budget did not pay for.
	if !budget.take(2) {
		return nil
	}

	properties := map[string]any{}
	var required []string
	truncated := false

	visiting := budget.visiting
	visiting[md.FullName()] = true
	defer delete(visiting, md.FullName())

	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)

		value := fieldSchema(fd, budget)
		if value == nil {
			// Out of budget partway through. Stop here rather than emitting
			// some fields and not others silently: the flag below is what
			// turns this into an honest, permissive schema.
			truncated = true
			break
		}
		properties[fd.JSONName()] = value

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
	}
	if !truncated {
		// Refused rather than ignored, because the caller is a model: a
		// misspelled argument silently dropped is a tool that "worked" and did
		// something other than what was asked, which is the failure mode a
		// schema exists to prevent.
		//
		// A truncated schema does not say this, and that is the whole point of
		// tracking it: `additionalProperties: false` over a properties map
		// missing fields the message really has would make a client refuse
		// arguments the server accepts. Truncation must lose precision, never
		// invent a refusal.
		schema["additionalProperties"] = false
	}
	if len(required) > 0 {
		schema["required"] = required
	}

	return schema
}

func fieldSchema(fd protoreflect.FieldDescriptor, budget *schemaBudget) map[string]any {
	if fd.IsMap() {
		if !budget.take(1) {
			return nil
		}

		value := valueSchema(fd.MapValue(), budget)
		if value == nil {
			return nil
		}

		return map[string]any{
			"type":                 "object",
			"additionalProperties": value,
		}
	}

	if fd.IsList() {
		if !budget.take(1) {
			return nil
		}

		items := valueSchema(fd, budget)
		if items == nil {
			return nil
		}

		return map[string]any{
			"type":  "array",
			"items": items,
		}
	}

	return valueSchema(fd, budget)
}

func valueSchema(fd protoreflect.FieldDescriptor, budget *schemaBudget) map[string]any {
	// Every branch below either returns one object or delegates to
	// [messageSchema], which reserves its own — so one node covers this call,
	// taken before any of them build anything.
	if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
		if !budget.take(1) {
			return nil
		}
	}

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
		// The two well-known types protojson spells as strings, and the cycle
		// cut below, are each one object of their own.
		wellKnown := fd.Message().FullName() == "google.protobuf.Timestamp" ||
			fd.Message().FullName() == "google.protobuf.Duration"

		if wellKnown || budget.visiting[fd.Message().FullName()] {
			if !budget.take(1) {
				return nil
			}

			switch fd.Message().FullName() {
			case "google.protobuf.Timestamp":
				return map[string]any{"type": "string", "format": "date-time"}
			case "google.protobuf.Duration":
				return map[string]any{"type": "string"}
			}

			// The schema graph is genuinely cyclic — a Value holds Values — and
			// JSON Schema rendered this way has no back-references, so a cycle
			// is cut with a permissive object rather than followed. That trades
			// precision an agent rarely needs at that depth for a schema that
			// terminates.
			return map[string]any{"type": "object"}
		}

		return messageSchema(fd.Message(), budget)

	default:
		return map[string]any{}
	}
}

// debugInputSchema is the debug tool's input surface: the same two documents
// flowstate_test takes, plus which case to hold and the script to drive it
// with.
//
// A script rather than a session handle, and that is the whole design. MCP is
// request/response with no console attached, so an interactive prompt has
// nowhere to live — but a [flowdebug.Session] already reads its commands as a
// *stream* (#928 slice 1 built it that way so a session could be recorded and
// replayed), and a stream is exactly what an agent can submit. One call is one
// session: the commands drive it, the transcript comes back, and the script
// that produced it is echoed so the same session can be re-run or extended.
func debugInputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"workflow": map[string]any{
				"type": "string",
				"description": "The Flowfile YAML to debug, exactly as it would be written to disk, " +
					"including the `edition:` line.",
			},
			"tests": map[string]any{
				"type": "string",
				"description": "A `*.test.yaml` document whose case supplies the run being debugged: its " +
					"`inputs:`, its `stubs:` standing in for every task, and its `signals:` scripting " +
					"any wait. The case's `expect:` is still judged, and a case that fails is held " +
					"open once more afterward so the remaining commands can question the finished " +
					"run. A case's own `workflow:` is accepted but never consulted: the `workflow` " +
					"argument above is what runs.",
			},
			"case": map[string]any{
				"type": "string",
				"description": "Which case to debug, by exact name. Required when `tests` declares more " +
					"than one: a session drives one run, and a script driving several would be " +
					"answering about a run it cannot name.",
			},
			"commands": map[string]any{
				"type":  "array",
				"items": map[string]any{"type": "string"},
				"description": "The debug script, one command per entry, in order: `step` (run this step " +
					"and stop at the next), `continue`, `until <step-id>`, `break <step-id>`, " +
					"`break <step-id> if <cel-expression>` (stop there only when the expression " +
					"holds, which is how a step inside a `for_each` is reached at one iteration " +
					"rather than every one), " +
					"`delete <step-id>`, `breakpoints`, `inspect <cel-expression>` (evaluate against " +
					"the paused run's own scope), `complete <partial-command>` (list what could be " +
					"written at the end of that text, over the paused run's own names — the same " +
					"answer a terminal gives for a tab press), " +
					"`scope` (list what it can name), `info` (describe " +
					"the step it is stopped at), `backtrace` (list that step and the `call:` " +
					"chain that reached it), " +
					"and `quit` (abandon the run, which fails the case). " +
					"The run starts held before its first step. When the script runs out the run " +
					"continues to the end on its own, so a script that only inspects is safe.",
			},
		},
		"required": []any{"workflow", "tests", "commands"},
		// Refused rather than ignored, for the reason [messageSchema] gives: a
		// misspelled argument silently dropped is a tool that "worked" and did
		// something other than what was asked.
		"additionalProperties": false,
	}
}
