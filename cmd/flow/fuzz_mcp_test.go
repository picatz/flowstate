package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
)

// FuzzMCPToolArguments fuzzes the decode step every MCP tool call goes
// through before anything it names gets executed: the untrusted JSON bytes an
// MCP client sends as a tool's arguments, decoded against the schema that tool
// advertised.
//
// Two decoders sit on this surface and both are fuzzed here, against the same
// bytes, because both are reached by the same kind of caller:
//
//   - mcpHandler's protojson.Unmarshal(raw, in) — one call site, run here
//     against every proto message a workflowServiceMethods() row names, since
//     the decoder is shared and the schema is the only thing that varies.
//   - runLocalToolHandler's encoding/json decode of runLocalArguments, the one
//     tool that is not an RPC and so is not in workflowServiceMethods.
//
// Neither call executes a workflow or dials a server on this path — decoding
// arguments is upstream of method.call and of v1.Run — so this is bounded to
// decode cost alone, which is what makes it fast enough to fuzz.
//
// The invariant: decoding untrusted bytes must never panic or hang, whatever
// the shape — including the one general risk a recursive-descent JSON decoder
// carries that a schema built from cel-go or YAML depth bounds says nothing
// about, since neither of those bounds is in this path: deeply nested JSON,
// which is what the corpus's "deep" seed is for.
func FuzzMCPToolArguments(f *testing.F) {
	for _, seed := range []string{
		// Empty arguments, which every tool accepts.
		``,
		`{}`,
		// A well-formed Validate call, the smallest real RPC argument.
		`{"file":{"path":"a.flow.yaml","content":"edition: v2026.2\nname: a\nsteps: []\n"}}`,
		// A well-formed run_local call, including the signals map that is this
		// tool's one extra piece of untrusted structure.
		`{"source":"edition: v2026.2\nname: a\nsteps: []\n","signals":{"go":{"ok":true}}}`,
		// An unknown field. DiscardUnknown is deliberately false on both
		// decoders, so this must be reported rather than silently dropped —
		// exactly the diagnostics principle CLAUDE.md states for the DSL
		// applied to this surface's own schema.
		`{"nosuchfield":1}`,
		// Type confusion: a field the schema declares as a message, given a
		// scalar, and a repeated field given a string.
		`{"file":"not an object"}`,
		`{"signals":"not an object"}`,
		// A huge number, to check the decoder rejects it rather than blocking on
		// an expensive parse of a big.Int/big.Float value.
		`{"pageSize":1e400000}`,
		// Deeply nested JSON, well past anything a human would write and past
		// where a recursive-descent parser might recurse itself out of stack
		// rather than reporting a clean parse error. This is what a
		// document-shaped depth bound (like flowfile's maxDepth) does not
		// cover, because this decode has no such bound at all — the finding, if
		// there is one, is that it needs one.
		"{" + jsonDeepArray(20000) + "}",
	} {
		f.Add(seed)
	}

	methods := workflowServiceMethods()

	f.Fuzz(func(t *testing.T, arguments string) {
		raw := []byte(arguments)

		// The RPC surface: the same bytes against every advertised schema, since
		// a real client picks the tool and this fuzzes the decoder rather than
		// the choice.
		for _, method := range methods {
			in := newMessage(method.input)
			if len(raw) == 0 {
				continue
			}
			_ = protojson.Unmarshal(raw, in)
		}

		// The one tool that is not an RPC, decoded the way runLocalToolHandler
		// decodes it.
		if len(raw) > 0 {
			var args runLocalArguments
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			_ = decoder.Decode(&args)
		}
	})
}

// jsonDeepArray renders a JSON value nested n arrays deep, as the value of a
// field named "pageSize" so it is a structurally plausible fragment for both
// decoders to reject or accept without help from a human-shaped seed.
func jsonDeepArray(n int) string {
	var b bytes.Buffer
	b.WriteString(`"pageSize":`)
	for range n {
		b.WriteByte('[')
	}
	b.WriteByte('0')
	for range n {
		b.WriteByte(']')
	}
	return b.String()
}
