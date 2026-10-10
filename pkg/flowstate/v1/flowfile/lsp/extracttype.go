package lsp

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/sourcegraph/go-lsp"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Extract type is the type-system sibling of the other quick fixes: a rule that
// several inputs, outputs or record fields spell out inline (`type: string` with
// the same `must:`) is declared once under `types:` and used by name.
//
// # Why it computes a text edit and then checks it
//
// Unlike a validator-suggested edit, nothing measured these ranges for us, so the
// action reads the positional model only to *propose* edits and then refuses to
// offer them until it has re-parsed the buffer they would produce. The proposal is
// accepted only when the edited document compiles to the same specification as the
// original once the one thing the rewrite is meant to add is set aside: the
// `type_source`/`must_source` spellings that remember the name, and the new type's
// own declaration. Everything the engine executes (each input's, output's and
// field's lowered type and rule) has to be equal. A shape this file does not
// understand therefore costs an action, never a wrong edit.
//
// # What it deliberately does not do
//
//   - Flow-style declarations (`a: {type: string, must: ...}`), multi-line rules,
//     trailing comments on the lines it would delete or replace, and anchors,
//     aliases, tags or merge keys anywhere near the declaration. An identical
//     occurrence it cannot rewrite makes the whole group unofferable: rewriting
//     only some of them would leave a file that says the same thing two ways.
//   - A name that is not obviously free. A derived name that collides with any
//     type, error, function or `use:` alias is not adjusted; nothing is offered.
//   - Other files. A `use:`d module is not searched or edited.
const (
	// maxExtractSites bounds how many declarations are read. The count is chosen
	// by the document; past it the action is not offered, because a group could
	// be missing members.
	maxExtractSites = 512

	// maxExtractOccurrences bounds one group, and with it the size of the edit and
	// the work of the verification compile.
	maxExtractOccurrences = 32

	// maxExtractActions bounds how many groups one request is answered with.
	maxExtractActions = 4

	// maxExtractVerifications bounds the candidate documents one request parses,
	// whether or not they verify, so a range over many groups costs a fixed number
	// of compiles rather than one per declaration.
	maxExtractVerifications = 8
)

// extractParse is how a document is compiled and validated; a parameter so a test
// can count the parses a request pays for.
type extractParse func(data []byte, path string) (*v1.Workflow, *flowfile.Positions, flowfile.Diagnostics, error)

// extractableBases are the built-in scalar bases a constrained scalar type may have.
var extractableBases = map[string]bool{
	"string": true, "int": true, "double": true, "bool": true,
	"timestamp": true, "duration": true, "bytes": true,
}

var (
	// extractTypeLine and extractMustLine match a whole source line holding only
	// that key, with nothing after the value (no comment, no second key).
	extractTypeLine = regexp.MustCompile(`^(\s*)type:[ \t]+([a-z0-9_]+)[ \t]*\r?$`)
	extractMustLine = regexp.MustCompile(`^(\s*)must:[ \t]+(\S.*?)[ \t]*\r?$`)
)

// An extractSite is one input, output or record field carrying an inline scalar
// type and rule.
type extractSite struct {
	name               string // the declaration's own name, for deriving a type name
	startLine, endLine int    // the declaration's lines, for matching the request

	base, rule string // the decoded base type and rule: the group key
	ruleRaw    string // the rule as written on its line, for the new declaration

	typeLine, mustLine int // 0-based
	typeStart, typeEnd int // byte columns of the type word on typeLine

	safe bool // every line the edit would touch is a plain single line
}

type extractKey struct{ base, rule string }

// extractTypeActions offers `Extract type 'X'` for each group of identical inline
// scalar types and rules the request touches.
func extractTypeActions(doc *document, params codeActionParams) []codeAction {
	return extractTypeActionsWith(doc, params, flowfile.ParseAndValidateSourceAt)
}

func extractTypeActionsWith(doc *document, params codeActionParams, parse extractParse) []codeAction {
	if doc.parsed == nil || strings.Contains(doc.text, "<<:") {
		return nil
	}
	sites, ok := extractSites(doc)
	if !ok || len(sites) == 0 {
		return nil
	}

	groups := map[extractKey][]*extractSite{}
	for _, s := range sites {
		k := extractKey{s.base, s.rule}
		groups[k] = append(groups[k], s)
	}

	// Compiled once, and only when some group is in range: a document that does not
	// compile offers nothing, at the cost of this one parse.
	path, _ := doc.filesystemPath()
	var (
		before      *v1.Workflow
		beforeDiags flowfile.Diagnostics
		compiled    bool
	)

	taken := declaredNames(doc)
	var actions []codeAction
	attempts := 0
	done := map[extractKey]bool{}
	for _, s := range sites { // document order, so the menu is stable
		k := extractKey{s.base, s.rule}
		if done[k] || s.endLine < params.Range.Start.Line || s.startLine > params.Range.End.Line {
			continue
		}
		done[k] = true
		group := groups[k]
		if len(group) > maxExtractOccurrences || slices.ContainsFunc(group, func(g *extractSite) bool { return !g.safe }) {
			continue
		}
		name := typeNameFor(s.name)
		if name == "" || taken[strings.ToLower(name)] {
			continue
		}
		edits, ok := extractEdits(doc, group, name)
		if !ok {
			continue
		}
		if !compiled {
			compiled = true
			var err error
			before, _, beforeDiags, err = parse([]byte(doc.text), path)
			if err != nil || before == nil {
				return nil
			}
		}
		if attempts++; attempts > maxExtractVerifications {
			break
		}
		if !extractVerified(doc, edits, name, path, parse, before, beforeDiags) {
			continue
		}
		actions = append(actions, codeAction{
			Title: fmt.Sprintf("Extract type '%s'", name),
			Kind:  lsp.CAKQuickFix,
			Edit:  &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{string(doc.uri): edits}},
		})
		if len(actions) == maxExtractActions {
			break
		}
	}

	return actions
}

// extractSites reads every input, output and record field with a scalar `type:`
// and a `must:`. False when the document has more declarations than the bound.
func extractSites(doc *document) ([]*extractSite, bool) {
	var (
		sites []*extractSite
		count int
	)
	visit := func(decls []*entry) bool {
		for _, d := range decls {
			if count++; count > maxExtractSites {
				return false
			}
			if site := extractSiteOf(doc, d); site != nil {
				sites = append(sites, site)
			}
		}
		return true
	}
	for _, e := range doc.parsed.entries {
		switch e.key {
		case "inputs", "outputs":
			if !visit(nestedEntries(e)) {
				return nil, false
			}
		case "types":
			for _, t := range nestedEntries(e) {
				for _, f := range nestedEntries(t) {
					if f.key == "fields" && !visit(nestedEntries(f)) {
						return nil, false
					}
				}
			}
		}
	}
	slices.SortFunc(sites, func(a, b *extractSite) int { return cmp.Compare(a.startLine, b.startLine) })

	return sites, true
}

// extractSiteOf reads one declaration, or nil when it holds no inline scalar rule.
func extractSiteOf(doc *document, d *entry) *extractSite {
	if d.value == nil || d.value.kind != kindMapping {
		return nil
	}
	var typeE, mustE *entry
	for _, f := range d.value.entries {
		switch f.key {
		case "type":
			typeE = f
		case "must":
			mustE = f
		}
	}
	if typeE == nil || mustE == nil || typeE.value == nil || mustE.value == nil ||
		typeE.value.kind != kindScalar || mustE.value.kind != kindScalar {
		return nil
	}
	base, rule := typeE.valueText(), mustE.valueText()
	if !extractableBases[base] || rule == "" {
		return nil
	}

	site := &extractSite{
		name:      d.key,
		startLine: d.keyRange.Start.Line,
		endLine:   deepEndLine(d),
		base:      base,
		rule:      rule,
		typeLine:  typeE.keyRange.Start.Line,
		mustLine:  mustE.keyRange.Start.Line,
	}
	site.safe = extractSafe(doc, d, typeE, mustE, site)

	return site
}

// extractSafe reports whether the lines the rewrite touches are the plain block
// shape it knows how to edit.
func extractSafe(doc *document, d, typeE, mustE *entry, site *extractSite) bool {
	// The declaration's key line carries nothing after its colon but a comment:
	// no anchor, tag, alias or flow mapping.
	keyLine := doc.index.line(d.keyRange.Start.Line)
	after := keyLine[min(len(keyLine), doc.index.byteOfUTF16(d.keyRange.Start.Line, d.keyRange.End.Character)):]
	if rest, ok := strings.CutPrefix(strings.TrimSpace(after), ":"); !ok || (strings.TrimSpace(rest) != "" && !strings.HasPrefix(strings.TrimSpace(rest), "#")) {
		return false
	}
	if site.typeLine == site.mustLine || site.typeLine == d.keyRange.Start.Line || site.mustLine == d.keyRange.Start.Line {
		return false
	}
	for _, e := range []*entry{typeE, mustE} {
		if e.value.rng.Start.Line != e.keyRange.Start.Line || e.value.rng.End.Line != e.keyRange.Start.Line {
			return false
		}
	}
	tm := extractTypeLine.FindStringSubmatchIndex(doc.index.line(site.typeLine))
	mm := extractMustLine.FindStringSubmatch(doc.index.line(site.mustLine))
	if tm == nil || mm == nil {
		return false
	}
	site.typeStart, site.typeEnd = tm[4], tm[5]
	site.ruleRaw = mm[2]
	if strings.ContainsAny(site.ruleRaw[:1], "&*!|>#") || strings.Contains(site.ruleRaw, " #") || strings.Contains(site.ruleRaw, "\t#") {
		return false
	}

	return true
}

// deepEndLine is the last line an entry's value occupies.
func deepEndLine(e *entry) int {
	end := e.keyRange.End.Line
	if e.value != nil {
		end = max(end, e.value.rng.End.Line)
		for _, c := range e.value.entries {
			end = max(end, deepEndLine(c))
		}
	}

	return end
}

// declaredNames is every name a new type must not take, lower-cased: the types,
// errors and functions the file declares and the aliases of its `use:` entries.
func declaredNames(doc *document) map[string]bool {
	names := map[string]bool{}
	for _, e := range doc.parsed.entries {
		if e.key == "types" || e.key == "errors" || e.key == "functions" || e.key == "use" {
			for _, d := range nestedEntries(e) {
				names[strings.ToLower(d.key)] = true
			}
		}
	}

	return names
}

// typeNameFor derives a type name from a declaration's name: its words
// capitalised and joined (`customer_id` is `CustomerId`). Empty when the result is
// not a valid declaration name, so a non-ASCII or empty name offers nothing.
func typeNameFor(decl string) string {
	var b strings.Builder
	upper := true
	for _, r := range decl {
		switch {
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9':
			if upper && r >= 'a' && r <= 'z' {
				r -= 'a' - 'A'
			}
			b.WriteRune(r)
			upper = false
		case r == '_' || r == '-' || r == ' ' || r == '.':
			upper = true
		default:
			return ""
		}
	}
	name := b.String()
	if !v1.IsModuleName(v1.QualifiedName("a", name)) || flowfile.IsCELReservedIdentifier(name) {
		return ""
	}

	return name
}

// extractEdits builds the edits: one declaration, then for each occurrence the
// `type:` word replaced and the `must:` line removed.
func extractEdits(doc *document, group []*extractSite, name string) ([]lsp.TextEdit, bool) {
	eol := "\n"
	if strings.Contains(doc.text, "\r\n") {
		eol = "\r\n"
	}
	decl, ok := declarationEdit(doc, group[0], name, eol)
	if !ok {
		return nil, false
	}
	edits := []lsp.TextEdit{decl}
	for _, s := range group {
		line := doc.index.line(s.typeLine)
		edits = append(edits,
			lsp.TextEdit{
				Range: lsp.Range{
					Start: lsp.Position{Line: s.typeLine, Character: utf16Len(line[:s.typeStart])},
					End:   lsp.Position{Line: s.typeLine, Character: utf16Len(line[:s.typeEnd])},
				},
				NewText: name,
			},
			removeLine(doc, s.mustLine),
		)
	}
	slices.SortStableFunc(edits, func(a, b lsp.TextEdit) int {
		return cmp.Or(cmp.Compare(a.Range.Start.Line, b.Range.Start.Line), cmp.Compare(a.Range.Start.Character, b.Range.Start.Character))
	})
	// Two edits meeting at a point have no defined order in the protocol.
	for i := 1; i < len(edits); i++ {
		if edits[i-1].Range.End.Line > edits[i].Range.Start.Line ||
			(edits[i-1].Range.End == edits[i].Range.Start) {
			return nil, false
		}
	}

	return edits, true
}

// removeLine deletes a whole line with its terminator. The last line of a file
// with no trailing newline has no terminator and no next line to end at, so only
// its content goes; an end position past the document is not a position.
func removeLine(doc *document, line int) lsp.TextEdit {
	if line+1 >= doc.index.lineCount() {
		return lsp.TextEdit{Range: lsp.Range{
			Start: lsp.Position{Line: line},
			End:   lsp.Position{Line: line, Character: utf16Len(doc.index.line(line))},
		}}
	}

	return lsp.TextEdit{Range: lsp.Range{Start: lsp.Position{Line: line}, End: lsp.Position{Line: line + 1}}}
}

// declarationEdit is the insertion of the new type as the first entry of `types:`,
// or a new `types:` block after `name:`. The first position is used so the
// insertion never meets an edit to a later line, and so a comment above an
// existing type stays with it.
func declarationEdit(doc *document, first *extractSite, name, eol string) (lsp.TextEdit, bool) {
	var block *entry
	for _, e := range doc.parsed.entries {
		if e.key == "types" {
			block = e
		}
	}
	indent, inner := "  ", "    "
	var at lsp.Position
	switch entries := nestedEntries(block); {
	case block == nil:
		after := doc.parsed.nameEntry
		if after == nil || after.value == nil {
			return lsp.TextEdit{}, false
		}
		at = lsp.Position{Line: after.value.rng.End.Line + 1}
		if at.Line >= doc.index.lineCount() {
			return lsp.TextEdit{}, false
		}
		return lsp.TextEdit{Range: lsp.Range{Start: at, End: at}, NewText: fmt.Sprintf(
			"types:%[1]s%[2]s%[3]s:%[1]s%[4]stype: %[5]s%[1]s%[4]smust: %[6]s%[1]s", eol, indent, name, inner, first.base, first.ruleRaw)}, true
	case len(entries) == 0 || block.keyRange.Start.Line == entries[0].keyRange.Start.Line:
		return lsp.TextEdit{}, false // `types:` empty or in flow style
	default:
		indent = strings.Repeat(" ", entries[0].keyRange.Start.Character)
		inner = indent + "  "
		if nested := nestedEntries(entries[0]); len(nested) > 0 && nested[0].keyRange.Start.Line != entries[0].keyRange.Start.Line {
			inner = strings.Repeat(" ", nested[0].keyRange.Start.Character)
		}
		at = lsp.Position{Line: block.keyRange.Start.Line + 1}

		return lsp.TextEdit{Range: lsp.Range{Start: at, End: at}, NewText: fmt.Sprintf(
			"%[2]s%[3]s:%[1]s%[4]stype: %[5]s%[1]s%[4]smust: %[6]s%[1]s", eol, indent, name, inner, first.base, first.ruleRaw)}, true
	}
}

// extractVerified re-parses what the edits would produce and compares the two
// documents' specifications. The original has to compile, and the edited document
// has to report the same diagnostics and the same specification apart from the
// spelling that remembers the type's name and the new declaration itself.
func extractVerified(doc *document, edits []lsp.TextEdit, name, path string, parse extractParse, before *v1.Workflow, beforeDiags flowfile.Diagnostics) bool {
	text := doc.text
	for _, e := range slices.Backward(edits) {
		start := doc.index.offsetOfPosition(e.Range.Start)
		end := doc.index.offsetOfPosition(e.Range.End)
		if end < start {
			return false
		}
		text = text[:start] + e.NewText + text[end:]
	}
	if len(text) > maxDocumentBytes {
		return false
	}

	after, _, afterDiags, err := parse([]byte(text), path)
	if err != nil || after == nil {
		return false
	}
	// Both documents have to be clean. Equal diagnostics would only prove the
	// rewrite preserves an existing fault (a literal default that breaks its own
	// rule keeps breaking it), which is no reason to offer it. A module is told it
	// cannot be run, and that one refusal is not a fault of its declarations.
	clean := func(ds flowfile.Diagnostics) bool {
		return !slices.ContainsFunc(ds, func(d flowfile.Diagnostic) bool { return d.Message != v1.ErrModule.Error() })
	}
	if !clean(beforeDiags) || !clean(afterDiags) {
		return false
	}

	// The new declaration is the only addition; it must exist, once, and is then
	// set aside along with the source spellings.
	declared := 0
	after = proto.Clone(after).(*v1.Workflow)
	after.DeclaredTypes = slices.DeleteFunc(after.DeclaredTypes, func(t *v1.TypeDeclaration) bool {
		if t.GetName() == name {
			declared++
			return true
		}

		return false
	})
	if declared != 1 {
		return false
	}
	before = proto.Clone(before).(*v1.Workflow)
	for _, wf := range []*v1.Workflow{before, after} {
		wf.SourceDigest = "" // a hash of the bytes, which differ by construction
		for _, in := range wf.DeclaredInputs {
			in.TypeSource, in.MustSource = nil, nil
		}
		for _, out := range wf.DeclaredOutputs {
			out.TypeSource, out.MustSource = nil, nil
		}
		for _, t := range wf.DeclaredTypes {
			for _, f := range t.Fields {
				f.TypeSource, f.MustSource = nil, nil
			}
		}
	}

	return proto.Equal(before, after)
}
