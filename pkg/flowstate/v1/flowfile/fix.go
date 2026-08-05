package flowfile

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/goccy/go-yaml/token"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Fix rewrites a Flowfile written in an older edition into the current one.
//
// This is the other half of a decision recorded in docs/DSL.md: surface syntax
// gets no deprecation window, because carrying two spellings costs the parser,
// the validator, the language server, the marshaller, and every test matrix that
// crosses them, for as long as the window lasts — and windows do not close on
// schedule. What makes that decision affordable rather than merely strict is
// that the migration is a program someone runs in a second.
//
// # Why this is not the marshaller
//
// The obvious implementation is parse-then-marshal, and it is wrong twice over.
// The old grammar no longer parses, which is the entire point; and a formatter
// rewrites a whole document, so an author fixing one retired key would get every
// comment moved and every string requoted in the same diff. A migration is a
// thing people read in review, and a diff that touches every line is a diff
// nobody reads.
//
// So this works on the document's own token stream, edits the ranges it must,
// and copies the rest through byte for byte. A file with nothing to fix comes
// back identical, which is what makes running it on a whole directory safe.
//
// # Refusing
//
// It refuses rather than guesses. Flow style — `task: {name: echo, inputs: {…}}`
// — has no line structure to rewrite, and a rewriter that reflows it is a
// rewriter that reformats an author's file in ways they did not ask for. An
// alias standing in for a task block cannot be rewritten at all without knowing
// what it will contain. In both cases the file is left alone and the position is
// reported, which is worth more than a mangled file that looks fixed.

// A FixResult reports what [Fix] did to one document.
type FixResult struct {
	// Source is the rewritten document, or the original when nothing changed.
	Source []byte

	// Changes describes each edit made, in source order, for a caller that wants
	// to tell an author what happened rather than only that something did.
	Changes []FixChange

	// Refusals are the places the rewriter could not act on safely. A document
	// with refusals is still rewritten everywhere else — stopping entirely would
	// mean one unrewritable step blocks the other nine — but it is not finished,
	// and a caller must say so.
	Refusals []Diagnostic

	// Notes are places worth a human's eye that are not problems. A file with
	// notes and no refusals is finished, and a caller must not fail on them.
	//
	// The distinction earns its keep with comments. A comment mentioning
	// `${a.result}` is prose about code that has moved, so leaving it is wrong and
	// rewriting it is guessing at what someone meant — and it is not a refusal
	// either, because nothing about the file is broken. Saying where it is costs a
	// line and saves a reader finding it a year later.
	Notes []Diagnostic
}

// Changed reports whether the rewrite altered the document.
func (r FixResult) Changed() bool { return len(r.Changes) > 0 }

// A FixChange is one edit the rewriter made.
type FixChange struct {
	// Line is the 1-based line the change was made at.
	Line int

	// Message says what changed, in the terms an author wrote the file in.
	Message string
}

// maxFixRounds bounds how many times [Fix] rewrites a document.
//
// A bound rather than a `for {}`, for the reason everything else here is bounded: the
// loop's termination argument rests on every rule making progress toward a document it
// no longer changes, and a rule that ever rewrites A to B while another rewrites B to A
// would spin forever on a file someone else wrote. Eight is far more rounds than any
// chain of rules has needed — the longest today is two — so reaching it means a cycle
// rather than a deep file, which is why it is reported rather than silently accepted.
const maxFixRounds = 8

// Fix rewrites data into the current edition.
//
// A document that already compiles is returned unchanged with no changes
// recorded, so this is safe to run over a directory that is mostly current.
//
// An error is returned only when the document is not YAML at all. Everything
// else — a shape that cannot be rewritten, a key that means nothing — is
// reported through [FixResult] so that a caller can rewrite what it can and say
// what it could not.
//
// # Why this runs to a fixed point
//
// One pass is not enough, and the reason generalises. The walk dispatches on the key it
// *found*, so a rule that rewrites a step into a key another rule owns hands that key to
// a walk which has already gone past: `task:` / `name: echo` becomes `echo:`, and the
// retirement rewriter never sees it. The file came out changed, exit 0, and refused by
// `flow validate` — with a diagnostic saying "run `flow fix`", which had just run.
//
// Chaining the second rule onto the first would fix that pair and leave the next one to
// be discovered the same way. Rewriting until nothing changes is the rule that does not
// need revisiting when a rule is added, and it costs a re-parse of a file that was
// already rewritten once.
//
// Changes accumulate across rounds because each is an edit genuinely made. Refusals and
// notes come from the final round alone: they describe the document as it now stands,
// and an earlier round's refusal may be about a step a later round went on to rewrite.
func Fix(data []byte) (FixResult, error) {
	out := FixResult{Source: data}

	source := data
	for round := 1; ; round++ {
		result, err := fixOnce(source)
		if err != nil {
			return FixResult{}, err
		}

		out.Source = result.Source
		out.Changes = append(out.Changes, result.Changes...)
		out.Refusals, out.Notes = result.Refusals, result.Notes

		if !result.Changed() {
			return out, nil
		}
		source = result.Source

		if round == maxFixRounds {
			out.Refusals = append(out.Refusals, Diagnostic{
				Line:   1,
				Column: 1,
				Message: fmt.Sprintf(
					"this file was still changing after %d rewrites, which means two rules are undoing "+
						"each other rather than that the file is large; what is written out is the "+
						"last round's result, and the difference between it and the round before is "+
						"the pair at fault",
					maxFixRounds),
			})

			return out, nil
		}
	}
}

// fixOnce is one rewriting pass over a document. See [Fix], which runs it to a fixed
// point.
func fixOnce(data []byte) (FixResult, error) {
	if len(data) > maxBytes {
		return FixResult{}, Diagnostics{{
			Line:   1,
			Column: 1,
			Message: fmt.Sprintf(
				"file is %d bytes, larger than the %d byte limit a Flowfile is read up to; nothing was rewritten",
				len(data), maxBytes),
		}}
	}

	file, err := parser.ParseBytes(data, parser.ParseComments)
	if err != nil {
		return FixResult{}, err
	}

	if refusal, refused := refuseUnrecognizedDocument(file); refused {
		// Left byte for byte alone: no anchors are collected, no expression is
		// rooted, no edition is stamped. Everything below this exists to migrate a
		// Flowfile, and a document that is not one gets none of it — see
		// [refuseUnrecognizedDocument] for why an edition stamp in particular is the
		// one edit this repo cannot afford to make by mistake.
		return FixResult{Source: data, Refusals: []Diagnostic{refusal}}, nil
	}

	f := &fixer{
		lines:           splitLines(data),
		trailingNewline: bytes.HasSuffix(data, []byte("\n")),
		terminator:      lineTerminator(data),
	}
	for _, doc := range file.Docs {
		// Anchors first, before anything that has to resolve an alias.
		//
		// This used to run after the expression pass, which meant the expression pass
		// could not follow an alias — so a loop writing `as: *name` looked like a loop
		// with no `as:` at all, and the rewriter subtracted `item` instead of the name
		// the file actually binds. Both directions of that are wrong at once: the real
		// binding is rooted inside the body, and a legacy reference to a step called
		// `item` is left bare.
		//
		// Nothing in the expression pass writes an anchor, and a Flowfile holds one
		// document (`compile` says so), so hoisting this changes only what the walks
		// below can see.
		f.collectAnchors(doc.Body)
	}
	for _, doc := range file.Docs {
		// Expressions are rooted first, and written straight into the lines rather
		// than recorded as an edit.
		//
		// The two rewrites are not the same shape and cannot share one mechanism.
		// Rooting substitutes inside a line and moves nothing, so applying it up
		// front is safe and every later pass simply reads the corrected text. The
		// step rewrite *replaces a run of lines*, copying the ones it keeps through
		// verbatim — so if a `${a.result}` inside a `task:` block were only recorded
		// as a competing edit, the block replacement would step over it and the
		// reference would come out unrooted. That is not hypothetical; it is what
		// happened, and it showed up as a rewritten file the validator refused.
		f.expressions(doc.Body, stepIDs(doc.Body))
	}
	for _, doc := range file.Docs {
		f.collectRetirementContext(doc.Body)
		f.workflow(doc.Body)
	}
	f.rewriteMovedReferences()

	if len(f.changes) > 0 {
		f.noteCommentsMentioningExpressions()
	}

	if len(f.edits) == 0 && !f.substituted {
		return FixResult{Source: data, Refusals: f.refusals, Notes: f.notes}, nil
	}
	return FixResult{
		Source:   f.apply(),
		Changes:  f.changes,
		Refusals: f.refusals,
		Notes:    f.notes,
	}, nil
}

// A fixer accumulates line edits over one document.
//
// Edits are recorded against line numbers and applied at the end, so that
// rewriting one step cannot move the lines another step was located at. A
// rewriter that edits as it walks has to keep an offset, and an offset is a
// thing to get wrong.
type fixer struct {
	lines []string

	// trailingNewline records whether the source ended with one, so that a file
	// that did not gets one back the same way. It is a byte nobody asked to have
	// changed, and a migration that quietly adds it puts a line in the diff of
	// every file it touches that has nothing to do with the migration.
	trailingNewline bool

	// terminator is how this document ends its lines. A rewritten line has to end
	// the same way the copied ones do, or a CRLF file comes back with LF on the
	// lines that changed — mixed endings in a file the tool promised to leave
	// alone except where it had to act.
	terminator string

	edits    map[int]lineEdit
	changes  []FixChange
	refusals []Diagnostic
	notes    []Diagnostic

	// substituted records that a line was rewritten in place, which the edit map
	// does not capture and which still means the document changed.
	substituted bool

	// deferredValueLines are the lines holding a deferred input's value, which is
	// expression source whether or not it carries a fence.
	//
	// Recorded during the expression walk, which is the only pass that knows which
	// inputs a task evaluates itself. [fixer.rewriteMovedReferences] rewrites
	// inside `${...}` everywhere else — because outside a fence a step reference is
	// prose — and a deferred input is the one place that rule is wrong: an input
	// that *is* expression source is written bare, since a fence there would be a
	// fence around a fence. Left out, a rooted reference in one survived a step
	// being migrated and named something that no longer existed, in a file that
	// validated, because deferred inputs are deliberately not reference-checked.
	deferredValueLines map[int]bool

	// anchors maps an anchor's name to what it holds, so a merge key's alias can be
	// followed. See [fixer.mergedDeclaresEdition].
	anchors map[string]ast.Node

	// referenced holds the step ids something in the document reads a result from.
	//
	// Collected before the walk, because whether a retired step can be migrated is a
	// question about the *rest* of the file: a value someone reads has a name to move
	// it to, and a value nobody reads is intent this cannot see.
	referenced map[string]bool

	// declaredVars are the names the workflow's `vars:` block already holds, so a
	// step moving into it cannot land on one.
	declaredVars map[string]bool

	// movedVars are the retired steps whose values are on their way into `vars:`.
	movedVars []movedVar
}

// A lineEdit replaces a run of source lines with new text.
type lineEdit struct {
	// through is the last line the edit consumes, 1-based and inclusive.
	through int

	// replacement is the lines written in their place, already indented. Empty
	// deletes the run.
	replacement []string
}

// record adds an edit, keeping the first when two overlap.
func (f *fixer) record(line, through int, replacement []string, message string) {
	if f.edits == nil {
		f.edits = make(map[int]lineEdit)
	}
	if _, taken := f.edits[line]; taken {
		return
	}
	f.edits[line] = lineEdit{through: through, replacement: replacement}
	f.changes = append(f.changes, FixChange{Line: line, Message: message})
}

// refuse records a place the rewriter would have had to guess.
func (f *fixer) refuse(n ast.Node, format string, args ...any) {
	span := spanOfNode(n)
	f.refusals = append(f.refusals, Diagnostic{
		Line:    span.Start.Line,
		Column:  span.Start.Column,
		Message: fmt.Sprintf(format, args...),
	})
}

// unwrapAnchor returns the node an anchor names, or the node itself.
//
// An anchor is written *on* a value — `- &first` above a step's keys — so every
// walker below has to look through one or it sees a shape it does not recognise
// and returns. It did, and the result was the worst outcome this command can
// produce: `flow fix` reported "already current" and exited zero on a file that
// `flow validate` refuses, which is precisely the "`flow fix . && git commit`
// must not succeed" property it exists to hold.
//
// An *alias* is deliberately not followed. It is a reference to a value written
// elsewhere, and that value is rewritten where it was declared; following it
// would send the rewriter at lines belonging to another step.
func unwrapAnchor(n ast.Node) ast.Node {
	for {
		anchor, ok := n.(*ast.AnchorNode)
		if !ok {
			return n
		}
		n = anchor.Value
	}
}

// note records something worth looking at that is not a problem.
func (f *fixer) note(line, column int, format string, args ...any) {
	f.notes = append(f.notes, Diagnostic{
		Line:    line,
		Column:  column,
		Message: fmt.Sprintf(format, args...),
	})
}

// recognizedTopLevelKeys are the top-level keys that mark a document as one
// `flow fix` knows how to act on.
//
// A Flowfile declares `steps:`. A Flowfile *test* — a `*.test.yaml` — declares
// `tests:` instead and has no `steps:` of its own; it is in fact the file whose
// missing `edition:` motivated this list existing at all (see issue #203). Both
// take an edition stamp, so both belong here.
var recognizedTopLevelKeys = []string{"steps", "tests"}

// distinctiveWorkflowKeys are the top-level keys that, in a document made
// entirely of [workflowKeys], are evidence it is a Flowfile rather than a
// coincidence — see [hasRecognizedKey], which requires one of these before it
// will accept a document that declares neither `steps:` nor `tests:`.
//
// `name:` and `description:` are deliberately absent. Both are spelled by
// nearly every configuration format there is, so a document declaring only
// those two is not a Flowfile missing its steps; it is a stranger whose two
// keys happen to collide with ours. Everything listed here means something
// specific to this language and would be an odd thing for another format to
// say.
var distinctiveWorkflowKeys = []string{"edition", "steps", "tests", "inputs", "outputs", "vars", "triggers"}

// refuseUnrecognizedDocument reports whether a parsed file is something `flow
// fix` has no business rewriting, and builds the diagnostic saying so.
//
// # Why a positive allowlist
//
// The obvious rule — refuse a document with no `steps:` — is wrong, and wrong in
// the direction that matters: a Flowfile test has no `steps:` at all, so that
// rule refuses to fix the one file issue #203 reports as already drifted, while
// doing nothing to stop an edition stamp landing in an egress policy or an
// auth/trust policy. Both of those are parsed `yaml.Strict()` specifically
// because they are fail-closed security controls, so the added key does not
// just look wrong — `ParseConfig` refuses to load the file afterward, silently,
// because `flow fix` exits 0.
//
// So this is a positive allowlist instead: a document is recognized because it
// declares one of [recognizedTopLevelKeys], not because it fails to look like
// something else. A new document shape that lands in this tree tomorrow is
// refused until someone teaches this list about it, rather than silently
// edited the moment it appears — the same fail-closed direction every other
// policy surface in this repo takes.
//
// # Why not the filename
//
// `.test.yaml` is a convention an author chose, not a contract the parser
// enforces, so this reads the document's own top-level keys and never the path
// it was read from. Deciding what a file *is* from what it is *called* is the
// same class of mistake CLAUDE.md already records twice: the rewriter knowing
// less about the document than the language does.
//
// # Why this cannot also refuse a malformed Flowfile
//
// The test is deliberately loose in one direction: a document need only declare
// the key, not have it well-formed. A Flowfile with a `steps:` that is empty,
// wrongly typed, or otherwise broken is still a Flowfile — fixing what can be
// fixed in a file that does not yet compile is the entire point of this
// command, and a stricter test here would start refusing the files `flow fix`
// exists to help.
func refuseUnrecognizedDocument(file *ast.File) (Diagnostic, bool) {
	for _, doc := range file.Docs {
		if doc.Body == nil {
			continue
		}

		mapping := asMapping(doc.Body)
		if mapping == nil {
			return Diagnostic{
				Line: 1, Column: 1,
				Message: fmt.Sprintf(
					"this document is %s at the top level, neither `steps:` (a Flowfile) nor `tests:` "+
						"(a Flowfile test); flow fix only rewrites those, and has left it untouched",
					describeNode(doc.Body)),
			}, true
		}

		if hasRecognizedKey(mapping) {
			continue
		}

		span := Span{}
		if len(mapping.Values) > 0 {
			span = spanOfNode(mapping.Values[0].Key)
		}
		line, column := 1, 1
		if span.IsValid() {
			line, column = span.Start.Line, span.Start.Column
		}

		return Diagnostic{
			Line: line, Column: column,
			Message: fmt.Sprintf(
				"this document has neither `steps:` nor `tests:` at the top level (its own keys are %s), "+
					"so it does not look like a Flowfile or a Flowfile test; flow fix only rewrites those, "+
					"and has left it untouched",
				describeTopLevelKeys(mapping)),
		}, true
	}

	return Diagnostic{}, false
}

// LooksLikeFlowfile reports whether data's top-level document shape is one
// [Fix] would act on: a Flowfile (`steps:`) or a Flowfile test (`tests:`),
// using the same allowlist [refuseUnrecognizedDocument] enforces from inside
// Fix itself.
//
// It exists for a caller that has to decide, before ever handing a file to
// Fix, which files under a directory are worth sweeping — `flow fix --check`
// walking a whole examples/ tree, say, that holds Flowfiles beside an egress
// policy, an auth policy, and unrelated YAML (docker-compose.yaml, Grafana
// provisioning) that were never Flowfiles to begin with. Reading the shape
// here lets that walk select only the files this package migrates, silently,
// rather than handing every one of them to Fix and collecting a refusal for
// each — which is correct for a sweep but wrong for a file named explicitly,
// which is why this is not how a named path is decided; a named path always
// reaches Fix directly, and Fix's own refusal is what tells its author it was
// not a Flowfile at all.
//
// Malformed YAML, and anything larger than [maxBytes], answers false rather
// than erroring: a sweep silently passing over a file it cannot parse is the
// same "not for me" answer as passing over one that parses into some other
// shape. A caller that wants the parse error reported hands the file to Fix
// instead, which returns it.
//
// Only the first document is read, matching every other place in this package
// that a Flowfile is one document (see [Fix]'s comment on why compile assumes
// it): a file this sweep should select is the ordinary single-document case,
// and a multi-document oddity is exactly the kind of shape this function
// answers false on rather than guesses about.
func LooksLikeFlowfile(data []byte) bool {
	if len(data) > maxBytes {
		return false
	}
	file, err := parser.ParseBytes(data, 0)
	if err != nil || len(file.Docs) == 0 || file.Docs[0].Body == nil {
		return false
	}
	mapping := asMapping(file.Docs[0].Body)
	return mapping != nil && hasRecognizedKey(mapping)
}

// IsMalformedYAML reports whether data is small enough to have been a
// candidate for [LooksLikeFlowfile] at all, and fails to parse as YAML
// outright — as opposed to parsing fine into some other, unrecognized shape.
//
// It exists to give a directory walk a second question to ask, because
// [LooksLikeFlowfile] answers false for two situations a sweep must not treat
// alike: a document that parses into something that is legitimately not a
// Flowfile (an egress policy, a docker-compose file), which a sweep is right
// to pass over in silence, and a document that does not parse at all, which
// is the one shape a directory walk must never quietly drop — an author who
// broke their workflow.yaml's syntax is exactly who needs told. A file named
// directly on the command line always reaches [Fix], which reports this same
// failure; this function lets a sweep give an unparseable file that chance
// too, rather than filtering it out before Fix ever sees it.
//
// Oversized input answers false, matching [LooksLikeFlowfile]: a file too
// large to be a candidate is left to whatever glob named it explicitly, not
// reported here, so the two functions never disagree about the boundary
// they share.
func IsMalformedYAML(data []byte) bool {
	if len(data) > maxBytes {
		return false
	}
	_, err := parser.ParseBytes(data, 0)
	return err != nil
}

// asMapping returns a node as a mapping, unwrapping an anchor and normalizing
// the single-entry shape the parser hands back for a document with one key.
func asMapping(n ast.Node) *ast.MappingNode {
	switch node := unwrapAnchor(n).(type) {
	case *ast.MappingNode:
		return node
	case *ast.MappingValueNode:
		return &ast.MappingNode{Values: []*ast.MappingValueNode{node}}
	default:
		return nil
	}
}

// hasRecognizedKey reports whether a mapping declares one of
// [recognizedTopLevelKeys] directly, or declares nothing but keys the Flowfile
// grammar itself allows at the top level.
//
// The second half exists for a real edge case, not a hypothetical one: a
// Flowfile that declares no steps at all — `edition: v2026.2\nname: t\n` and
// nothing else — is a legal, if useless, workflow ([compiler] reads `steps:`
// with `fields.get`, so its absence is not an error), and
// TestFixLeavesACurrentFileByteForByte already fixed one before this allowlist
// existed. `steps:`/`tests:` alone as the only signal would refuse it, because
// a file this small carries neither. So a document also passes when every key
// it declares is drawn from [workflowKeys] — the schema's own list of what a
// Flowfile may say at the top level — even though none of the keys present
// happens to be `steps:` or `tests:`.
//
// This does not reopen the hole the allowlist exists to close: none of
// `workflowKeys` overlaps a single key any policy file in this tree declares
// (`egress:`, `issuers:`, `secrets:`, `federation:`), so an egress policy or an
// auth/trust policy still has no key that qualifies it.
//
// It is narrowed one step further than that, though, because `name:` and
// `description:` are not evidence of anything. Every configuration format in
// the world spells those, so a document declaring only those two would qualify
// under a bare "all keys are workflow keys" test and be stamped — a smaller
// version of the same mistake, waiting for a config file nobody has written
// yet. So the second branch additionally requires at least one key that no
// other format would use in this combination ([distinctiveWorkflowKeys]): the
// zero-step file above carries `edition:` and still passes, and a document of
// nothing but `name:` and `description:` is refused like any other stranger.
//
// A merge key is not resolved here the way [fixer.mergedDeclaresEdition] resolves
// one for `edition:`. That method exists because failing to find a merged
// edition would let the rewriter *downgrade* a file — stamping in a spelling it
// should have left alone. The failure mode here runs the other way: this
// function decides whether to touch the file at all, so a merge key is treated
// as neutral — it neither qualifies a document by itself (this walk cannot see
// what it resolves to) nor disqualifies one that is otherwise all recognized
// keys, since a merge could easily be bringing in nothing more exotic than
// shared `vars:`.
func hasRecognizedKey(mapping *ast.MappingNode) bool {
	sawDistinctive := false
	allWorkflowKeys := true
	for _, v := range mapping.Values {
		if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
			continue
		}
		name, ok := keyNameOf(v.Key)
		if !ok {
			allWorkflowKeys = false
			continue
		}
		if slices.Contains(recognizedTopLevelKeys, name) {
			return true
		}
		if slices.Contains(distinctiveWorkflowKeys, name) {
			sawDistinctive = true
		}
		if !slices.Contains(workflowKeys, name) {
			allWorkflowKeys = false
		}
	}
	return sawDistinctive && allWorkflowKeys
}

// describeTopLevelKeys names a mapping's own keys the way an author wrote them,
// for a diagnostic that says what a refused document looks like instead of a
// Flowfile.
func describeTopLevelKeys(mapping *ast.MappingNode) string {
	if len(mapping.Values) == 0 {
		return "none"
	}
	var names []string
	for _, v := range mapping.Values {
		if name, ok := keyNameOf(v.Key); ok {
			names = append(names, "`"+name+":`")
			continue
		}
		if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
			names = append(names, "`<<:`")
		}
	}
	if len(names) == 0 {
		return "none this can name"
	}
	return strings.Join(names, ", ")
}

// workflow walks a document body, rewriting its steps and its edition marker.
func (f *fixer) workflow(n ast.Node) {
	n = unwrapAnchor(n)
	mapping, ok := n.(*ast.MappingNode)
	if !ok {
		if single, isOne := n.(*ast.MappingValueNode); isOne {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			return
		}
	}
	declared := false
	for _, v := range mapping.Values {
		name, ok := keyNameOf(v.Key)
		if !ok {
			// A merge key names nothing itself and brings in whatever it points at —
			// which may include an edition. Not looking was a way to *downgrade* a
			// file: with no direct `edition:` this stamped the current one in, and a
			// direct key beats a merged one, so a document declaring a grammar this
			// build refuses came back declaring one it compiles. Fail-closed, undone
			// by the command that exists to keep files honest.
			if f.mergedDeclaresEdition(v) {
				declared = true
			}

			continue
		}
		switch name {
		case "steps":
			// The top of the file binds nothing: a workflow `vars:` value is not a
			// bare name anywhere, it is `vars.<name>`, and the rewriter's refusal for
			// reading one is separate.
			f.steps(v.Value, stepScope{})
		case "edition":
			declared = true
			f.edition(v)
		}
	}

	if !declared {
		f.stampEdition(mapping)
	}

	// Last, because it collects what the step walk found. Several steps may move into
	// one block, and the block is one place in the document — writing each as it was
	// found would mean several edits recorded at one line, of which the map keeps only
	// the first.
	f.writeMovedVars(mapping)
}

// mergedDeclaresEdition reports whether a `<<:` entry brings an `edition:` with it.
//
// Only *whether*, not which. Bringing a merged edition forward would mean editing the
// anchor it came from, which may be shared with other keys or other documents — so the
// answer to finding one is to leave the file alone and let `flow validate` say what is
// wrong with it. What this prevents is the rewriter deciding an edition is absent when
// it is merely written somewhere this walk does not look.
//
// Every "cannot tell" answers *yes*. An alias it cannot read, an anchor it cannot find,
// a merge of something that is not a mapping: each means the edition might be in there,
// and the cost of being wrong runs one way only. Saying yes leaves a file unstamped and
// an author told to write the line; saying no silently rewrites what a file declares.
func (f *fixer) mergedDeclaresEdition(entry *ast.MappingValueNode) bool {
	if _, isMerge := entry.Key.(*ast.MergeKeyNode); !isMerge {
		return false
	}

	merged := unwrapAnchor(entry.Value)
	if alias, isAlias := merged.(*ast.AliasNode); isAlias {
		// An alias names an anchor; its own Value is that *name* rather than what the
		// anchor holds, so it has to be resolved through the document.
		name, ok := scalarText(alias.Value)
		if !ok {
			return true
		}
		anchored, known := f.anchors[name]
		if !known {
			return true
		}
		merged = unwrapAnchor(anchored)
	}

	mapping, ok := merged.(*ast.MappingNode)
	if !ok {
		return true
	}
	for _, v := range mapping.Values {
		if name, ok := keyNameOf(v.Key); ok && name == "edition" {
			return true
		}
	}

	return false
}

// collectAnchors records every anchor in a document, so an alias can be resolved
// wherever it appears.
//
// The compiler keeps its own map for the same reason; this one exists because the
// rewriter walks the raw AST rather than the compiler's entries, and an alias is a
// reference the raw AST does not follow.
func (f *fixer) collectAnchors(n ast.Node) {
	switch node := n.(type) {
	case nil:
		return
	case *ast.AnchorNode:
		if name, ok := scalarText(node.Name); ok {
			if f.anchors == nil {
				f.anchors = map[string]ast.Node{}
			}
			f.anchors[name] = node.Value
		}
		f.collectAnchors(node.Value)
	case *ast.MappingNode:
		for _, v := range node.Values {
			f.collectAnchors(v)
		}
	case *ast.MappingValueNode:
		f.collectAnchors(node.Key)
		f.collectAnchors(node.Value)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			f.collectAnchors(v)
		}
	}
}

// stampEdition writes an `edition:` into a file that has none.
//
// # This reverses what the comment below used to say
//
// It used to be that only a written marker was updated, because "a file with no
// `edition:` is a file that has not asked to be pinned, and stamping one in would be
// the rewriter adding an opinion the author did not have."
//
// The opinion turned out to be the *absence*. Making the key required — see
// [missingEdition] — means an unmarked file no longer says "any grammar"; it says
// nothing, and this build refuses it. Declining to stamp would leave `flow fix` unable
// to fix the one thing every pre-sweep file now needs, which is the failure the comment
// above [fixer.edition] describes: a migration tool that does not migrate the thing
// whose diagnostic names it.
//
// Written at the very top, above `name:`, because that is where an author writes it and
// where a reader looks for it — a statement about the whole document belongs before the
// document.
func (f *fixer) stampEdition(mapping *ast.MappingNode) {
	// Anchored on the first key rather than on line 1, so a file opening with a
	// comment block keeps it above the marker. Prepending blindly would put the
	// edition above a header comment that reads as being about the file.
	if len(mapping.Values) == 0 {
		return
	}
	span := spanOfNode(mapping.Values[0].Key)
	if !span.IsValid() {
		return
	}

	line := span.Start.Line
	indent := strings.Repeat(" ", span.Start.Column-1)

	// The first key's own line is replaced by two: the marker, then the line as it
	// was. Inserting is not something the edit map can express — it replaces runs —
	// and expressing it this way means the rest of the file is still copied through
	// untouched.
	f.record(line, line,
		[]string{indent + "edition: " + CurrentEdition, f.line(line)},
		"`edition: "+CurrentEdition+"` added, which is now required")
}

// edition brings a declared edition marker up to the current one.
//
// Without this the two halves of the design contradict each other: an older
// edition is refused with "run `flow fix` to rewrite the file", and `flow fix`
// would answer "already current" while leaving the marker that caused the
// refusal. A migration tool that does not migrate the thing whose diagnostic
// names it is a migration tool nobody will trust twice.
//
// Only a marker that is written is updated. A file with no `edition:` is a file
// that has not asked to be pinned, and stamping one in would be the rewriter
// adding an opinion the author did not have.
func (f *fixer) edition(entry *ast.MappingValueNode) {
	declared, ok := editionText(entry.Value)
	if !ok || declared == CurrentEdition {
		return
	}

	// Only editions this build knows how to bring forward. A marker from the
	// future is a file a newer `flow` wrote, and rewriting it to an older edition
	// would be this build claiming to understand a grammar it does not have.
	if !slices.Contains(knownEditions, declared) {
		f.refuse(entry.Value,
			"edition %q is not one this build knows, so there is nothing to rewrite it to; a newer flow wrote this file",
			declared)
		return
	}

	keySpan := spanOfNode(entry.Key)
	if !keySpan.IsValid() {
		return
	}
	// The key's own line and no more. An edition is a scalar written beside its
	// key, so taking the block under it would consume anything indented on the next
	// line — a comment, most likely — and delete it while claiming to have updated
	// a version number.
	//
	// And within that line, only the *scalar* is replaced: everything after it is
	// copied through. Rebuilding the line from the key and the new value was losing a
	// trailing comment — `edition: 2026.1 # pinned deliberately, see RFC-14` came back
	// without the sentence explaining it. That path became reachable the moment
	// `2026.1` turned into an edition this build actually upgrades, which is now.
	//
	// Written unquoted, matching what [fixer.stampEdition] writes. The quotes were
	// there because an unprefixed `2026.1` is a YAML float and had to be forced to a
	// string; a v-prefixed edition is a string already, so quoting it buys nothing and
	// makes one command produce two spellings of the same value.
	valueSpan := spanOfNode(entry.Value)
	line := f.line(keySpan.Start.Line)
	if !valueSpan.IsValid() || valueSpan.Start.Line != keySpan.Start.Line {
		return
	}

	from, through := valueSpan.Start.Column-1, valueSpan.End.Column-1
	if from < 0 || through > len(line) || from > through {
		return
	}

	f.record(keySpan.Start.Line, keySpan.Start.Line,
		[]string{line[:from] + CurrentEdition + line[through:]},
		fmt.Sprintf("edition %q updated to %q", declared, CurrentEdition))
}

// A stepScope is what the walk knows about a step that the step itself cannot see.
//
// Both fields exist for the retirement rewriter, and both are about whether a step's
// value can be *lifted out* of where it was written. Neither is discoverable from the
// step: one is a property of the list holding it and the other of every block above it.
type stepScope struct {
	// alone reports that the step is the only one in its list, so deleting it would
	// leave `- steps:` with nothing under it — a branch or a loop body that is no
	// longer a document.
	alone bool

	// bound are the bare names in scope where the step is written: an enclosing
	// loop's iterator, and the keys of any `vars:` block above it or on it.
	//
	// A workflow `vars:` block sees none of them, so a value mentioning one cannot be
	// lifted there. Nothing rooted gives this away — `${person}` is just a word — so
	// without carrying the scope down the rewriter reads it as a constant and moves a
	// loop-local expression to the top of the file.
	bound []string
}

// with returns the scope extended by the names a block binds for what is inside it.
func (s stepScope) with(names ...string) stepScope {
	if len(names) == 0 {
		return s
	}

	return stepScope{alone: s.alone, bound: append(slices.Clone(s.bound), names...)}
}

// steps walks a sequence of steps, at any nesting depth.
func (f *fixer) steps(n ast.Node, scope stepScope) {
	seq, ok := unwrapAnchor(n).(*ast.SequenceNode)
	if !ok {
		return
	}
	// Whether a step is alone is a property of the list, so it is set here rather
	// than discovered below.
	scope.alone = len(seq.Values) == 1
	for _, step := range seq.Values {
		f.step(step, scope)
	}
}

// step rewrites one step, and descends into any steps nested inside it.
func (f *fixer) step(n ast.Node, scope stepScope) {
	var (
		step   *ast.MappingNode
		values []*ast.MappingValueNode
	)
	switch node := unwrapAnchor(n).(type) {
	case *ast.MappingNode:
		step, values = node, node.Values
	case *ast.MappingValueNode:
		step = &ast.MappingNode{Values: []*ast.MappingValueNode{node}}
		values = step.Values
	default:
		return
	}

	// A step's own `vars:` bind names for its inputs and for anything nested inside
	// it, so they are in scope before any key is looked at — including for the step
	// itself, whose value may name one.
	inner := scope.with(mappingKeys(blockOf(step, varsKey))...)

	for _, v := range values {
		name, ok := keyNameOf(v.Key)
		if !ok {
			continue
		}

		// A step running a retired task is either migrated whole or refused whole, so
		// nothing else on it is worth looking at either way.
		if f.retiredStep(step, v, name, inner) {
			return
		}

		switch name {
		case "task":
			f.taskBlock(v)
		case forEachKey:
			f.renamedKey(v.Value, "iterator", forEachAsKey)
			f.nested(v.Value, forEachStepsKey, inner.with(iteratorOf(v.Value)))
		case "parallel":
			f.branches(v.Value, inner)
		}
	}
}

// blockOf returns the mapping written under one of a step's keys.
func blockOf(step *ast.MappingNode, key string) *ast.MappingNode {
	for _, v := range step.Values {
		if name, ok := keyNameOf(v.Key); ok && name == key {
			if mapping, isMapping := unwrapAnchor(v.Value).(*ast.MappingNode); isMapping {
				return mapping
			}
		}
	}

	return nil
}

// mappingKeys returns a mapping's keys, skipping any this package cannot read as a
// name.
func mappingKeys(mapping *ast.MappingNode) []string {
	if mapping == nil {
		return nil
	}

	names := make([]string, 0, len(mapping.Values))
	for _, v := range mapping.Values {
		if name, ok := keyNameOf(v.Key); ok {
			names = append(names, name)
		}
	}

	return names
}

// iteratorOf returns the name a loop binds each item to, falling back to the engine's
// own default rather than a copy of it.
func iteratorOf(forEach ast.Node) string {
	mapping, ok := unwrapAnchor(forEach).(*ast.MappingNode)
	if !ok {
		return v1.DefaultIterator
	}
	for _, v := range mapping.Values {
		if name, keyed := keyNameOf(v.Key); keyed && name == "as" {
			if text, scalar := scalarText(v.Value); scalar && text != "" {
				return text
			}
		}
	}

	return v1.DefaultIterator
}

// renamedKey rewrites one key of a mapping, leaving its value and the rest of the line
// exactly as written.
//
// The whole edit is the token before the colon, which is what makes a rename safe to do
// on lines at all: the value keeps its quoting, an inline comment keeps its column, and
// a multi-line value underneath is not touched because the key's own line is the only
// one consumed. That last part matters — taking the block under the key would delete a
// comment written beneath it while claiming to have renamed something.
//
// Silent when the new spelling is already there, so running this twice changes nothing
// the first run did not.
func (f *fixer) renamedKey(n ast.Node, was, now string) {
	n = unwrapAnchor(n)
	mapping, ok := n.(*ast.MappingNode)
	if !ok {
		if single, isOne := n.(*ast.MappingValueNode); isOne {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			return
		}
	}

	for _, v := range mapping.Values {
		name, ok := keyNameOf(v.Key)
		if !ok || name != was {
			continue
		}

		span := spanOfNode(v.Key)
		if !span.IsValid() || span.Start.Line > len(f.lines) {
			continue
		}

		// Rewritten from the source line rather than reassembled from the parsed
		// value, because reassembling loses whatever the author wrote after it. The
		// key is replaced in place and the remainder of the line — the colon, the
		// spacing, the value, any trailing comment — is copied through.
		line := f.lines[span.Start.Line-1]
		at := span.Start.Column - 1
		if at < 0 || at+len(was) > len(line) || line[at:at+len(was)] != was {
			// The key is not where the parser said it was, which means something
			// about this line is not the shape assumed here — a quoted key, most
			// likely. Refusing beats editing at an offset that is off by one.
			f.refuse(v.Key, "`%s:` is now `%s:`, but this line is not shaped so it can be rewritten safely; change it by hand", was, now)

			continue
		}

		f.record(span.Start.Line, span.Start.Line,
			[]string{line[:at] + now + line[at+len(was):]},
			fmt.Sprintf("`%s:` renamed to `%s:`", was, now))
	}
}

// nested descends into a named key holding a step sequence.
func (f *fixer) nested(n ast.Node, key string, scope stepScope) {
	n = unwrapAnchor(n)
	mapping, ok := n.(*ast.MappingNode)
	if !ok {
		if single, isOne := n.(*ast.MappingValueNode); isOne {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			return
		}
	}
	for _, v := range mapping.Values {
		if name, ok := keyNameOf(v.Key); ok && name == key {
			f.steps(v.Value, scope)
		}
	}
}

// branches descends into a parallel's list of branches.
func (f *fixer) branches(n ast.Node, scope stepScope) {
	seq, ok := unwrapAnchor(n).(*ast.SequenceNode)
	if !ok {
		return
	}
	for _, branch := range seq.Values {
		f.nested(branch, "steps", scope)
	}
}

// taskBlock rewrites `task:` / `name:` / `inputs:` into the task's own key.
//
// The transformation is a deletion and a rename, which is why it can be done on
// lines at all: `task:` and `name:` go away, `inputs:` becomes the task's name,
// and everything under `inputs:` dedents by one level. A description moves up to
// the step, which is where prose about a step lives now.
func (f *fixer) taskBlock(entry *ast.MappingValueNode) {
	keySpan := spanOfNode(entry.Key)
	if !keySpan.IsValid() {
		return
	}

	block, ok := entry.Value.(*ast.MappingNode)
	if !ok {
		if single, isOne := entry.Value.(*ast.MappingValueNode); isOne {
			block = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			f.refuse(entry.Value,
				"`task:` here is %s rather than a mapping of `name:` and `inputs:`, so there is no task name to rewrite it to; fix this step by hand",
				describeNode(entry.Value))
			return
		}
	}
	if block.IsFlowStyle {
		f.refuse(entry.Value,
			"`task:` is written in flow style, which has no line structure to rewrite; write it across lines and run this again")
		return
	}

	var (
		nameNode        ast.Node
		inputsKey       ast.Node
		inputsNode      ast.Node
		descriptionNode ast.Node
	)
	for _, v := range block.Values {
		key, ok := keyNameOf(v.Key)
		if !ok {
			continue
		}
		switch key {
		case "name":
			nameNode = v.Value
		case "inputs":
			inputsKey, inputsNode = v.Key, v.Value
		case "description":
			descriptionNode = v.Value
		}
	}

	taskName, ok := scalarText(nameNode)
	if !ok {
		f.refuse(entry.Value,
			"`task:` has no `name:` written as a plain value, so the key to rewrite it to is not known here; fix this step by hand")
		return
	}

	// The whole `task:` block is replaced, so the run of lines it covers has to be
	// exact — and it is read from indentation rather than from the tokens beneath
	// it. A node's span reaches its values and not the comments among them, so a
	// span-derived end drops any comment written inside the block, and this is a
	// migration: dropping an author's comment is losing their work.
	taskIndent := keySpan.Start.Column - 1
	through := f.blockEnd(keySpan.Start.Line, taskIndent)
	indent := strings.Repeat(" ", taskIndent)

	var replacement []string

	// Comments written among the keys that are going away — above `name:`, after
	// the inputs, anywhere in the block that is not among the inputs themselves —
	// described the task, and the task is still here. Carried up to sit above its
	// key rather than deleted with the lines they were on.
	replacement = append(replacement, f.commentsOutsideInputs(keySpan.Start.Line, through, inputsKey, indent)...)

	if descriptionNode != nil {
		text, ok := scalarText(descriptionNode)
		if !ok {
			f.refuse(descriptionNode,
				"the task's `description:` is not a plain value, so it cannot be moved to the step; write it as one and run this again")
			return
		}
		replacement = append(replacement, indent+"description: "+quoteScalar(text))
	}

	inputLines, ok := f.inputLines(inputsKey, inputsNode, taskIndent)
	if !ok {
		return
	}
	if len(inputLines) == 0 {
		// A task with no inputs is written `echo: {}` on one line. The empty
		// mapping is deliberate — `echo:` alone reads as an unfinished line — but
		// putting it on a line of its own reads as one too.
		replacement = append(replacement, indent+taskName+": {}")
	} else {
		replacement = append(replacement, indent+taskName+":")
		replacement = append(replacement, inputLines...)
	}

	f.record(keySpan.Start.Line, through, replacement,
		fmt.Sprintf("`task:` naming %q rewritten to `%s:`", taskName, taskName))
}

// inputLines renders a task's inputs dedented by one level under the task's key.
//
// The source text is copied rather than re-rendered, so a comment, a block
// scalar, or a hand-chosen quoting style survives. Only the indentation changes,
// and only by the fixed amount `inputs:` used to add.
func (f *fixer) inputLines(inputsKey, inputs ast.Node, taskIndent int) ([]string, bool) {
	// No inputs at all, and `inputs:` written with nothing under it, are the same
	// task. The caller writes both as `echo: {}`.
	if inputs == nil {
		return nil, true
	}
	if _, empty := inputs.(*ast.NullNode); empty {
		return nil, true
	}

	mapping, ok := inputs.(*ast.MappingNode)
	if !ok {
		if single, isOne := inputs.(*ast.MappingValueNode); isOne {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}, IsFlowStyle: false}
		} else {
			f.refuse(inputs,
				"`inputs:` is %s rather than a mapping, so it cannot be moved under the task's name; fix this step by hand",
				describeNode(inputs))
			return nil, false
		}
	}
	if mapping.IsFlowStyle {
		f.refuse(inputs,
			"`inputs:` is written in flow style, which has no line structure to rewrite; write it across lines and run this again")
		return nil, false
	}

	keySpan := spanOfNode(inputsKey)
	if !keySpan.IsValid() {
		f.refuse(inputs, "`inputs:` has no source position, so it cannot be moved; fix this step by hand")
		return nil, false
	}

	// The block is the lines under `inputs:`, read by indentation rather than from
	// the nodes beneath it — so a comment written among the inputs is carried along
	// with them instead of falling into a gap between two token spans.
	inputsIndent := keySpan.Start.Column - 1
	first := keySpan.Start.Line + 1
	last := f.blockEnd(keySpan.Start.Line, inputsIndent)
	if last < first {
		return nil, true
	}

	// How far every line moves left. The values sat two levels in from `task:` —
	// once for `inputs:` and once for themselves — and end up one level in from the
	// task's own key, which is where a task's inputs go.
	//
	// Measured from the first line with something on it. A blank line straight
	// under `inputs:` is legal and common, and its indent is zero, so measuring
	// from whatever line came first refused a perfectly good file — telling an
	// author their indentation was wrong when it was not, which is the kind of
	// diagnostic that teaches people to stop reading them.
	shift := indentWidth(f.line(f.firstContentLine(first, last))) - (taskIndent + 2)
	if shift < 0 {
		f.refuse(inputs,
			"the values under `inputs:` are indented less than the key they belong to, so dedenting them would change what they nest under; fix this step by hand")
		return nil, false
	}

	out := make([]string, 0, last-first+1)
	for n := first; n <= last; n++ {
		line := f.line(n)
		if strings.TrimSpace(line) == "" {
			out = append(out, "")
			continue
		}
		if indentWidth(line) < shift {
			f.refuse(inputs,
				"line %d is indented less than the values under `inputs:` it sits among, so this block cannot be dedented as a whole; fix this step by hand", n)
			return nil, false
		}
		out = append(out, line[shift:])
	}
	return out, true
}

// commentsOutsideInputs returns the comment-only lines of a `task:` block that do
// not belong to its inputs, re-indented to the given level.
//
// They are the comments about the task itself: the ones explaining `name:`, and
// any written after the inputs at the level of the keys being removed. A comment
// among the *inputs* travels with them and is not collected here — which is why
// this is a whole-block scan with a hole in it rather than a scan that stops at
// the inputs. Stopping there dropped every comment written below them.
func (f *fixer) commentsOutsideInputs(taskLine, through int, inputsKey ast.Node, indent string) []string {
	inputsFirst, inputsLast := 0, -1
	if span := spanOfNode(inputsKey); span.IsValid() {
		inputsFirst = span.Start.Line
		inputsLast = f.blockEnd(span.Start.Line, span.Start.Column-1)
	}

	var out []string
	for n := taskLine; n <= through; n++ {
		if n > taskLine && n >= inputsFirst && n <= inputsLast {
			continue
		}
		text := strings.TrimSpace(f.line(n))
		if strings.HasPrefix(text, "#") {
			out = append(out, indent+text)
			continue
		}
		// A comment written at the end of a key that is going away goes with the
		// rest of that key's line otherwise. `name: echo # the greeting one` says
		// something about the task, and the task is still here.
		if comment := trailingComment(text); comment != "" {
			out = append(out, indent+comment)
		}
	}
	if inputsFirst > 0 {
		if comment := trailingComment(strings.TrimSpace(f.line(inputsFirst))); comment != "" {
			out = append(out, indent+comment)
		}
	}
	return out
}

// trailingComment returns the comment at the end of a line, or the empty string
// when there is none.
//
// Only called on the structural lines a task block is made of — `task:`,
// `name: <task>`, `inputs:` — which is what makes a simple rule safe. Two of them
// have no value at all, and a task name is `[A-Za-z][A-Za-z0-9_-]*`, so a `#`
// after the colon on any of them is a comment and cannot be part of a value.
//
// A line carrying a quote is left alone regardless. Deciding whether a `#` inside
// a string is a comment means lexing YAML, and a rewriter that guesses wrong there
// truncates an author's value — which is worse than dropping the comment it was
// trying to save.
func trailingComment(line string) string {
	if strings.ContainsAny(line, `"'`) {
		return ""
	}
	i := strings.Index(line, " #")
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(line[i+1:])
}

// firstContentLine returns the first line in a range with something other than
// whitespace on it, or the range's start when there is none.
func (f *fixer) firstContentLine(first, last int) int {
	for n := first; n <= last; n++ {
		if strings.TrimSpace(f.line(n)) != "" {
			return n
		}
	}
	return first
}

// blockEnd returns the last line belonging to the block a key opens.
//
// A block is its key's line plus every following line indented further. Unlike a
// node's token span it takes in the comments written among the values, which a
// rewriter must carry rather than drop — a comment is the part of a file a tool
// can least afford to lose.
//
// A comment's own indentation says nothing about YAML's structure — people dedent
// one to the margin all the time — so a comment never *ends* the block. Only a
// content line at or left of the key does that. A comment indented past the key is
// still part of the block and extends it, which is what keeps a note written under
// the last input travelling with the inputs.
//
// Treating a dedented comment as the end was a real defect and the worst-shaped
// kind: the replacement consumed only the lines above it, the `name:` and
// `inputs:` below it were left where they were, and the rewriter reported success
// on a document it had just mangled.
func (f *fixer) blockEnd(keyLine, indent int) int {
	last := keyLine
	for n := keyLine + 1; n <= len(f.lines); n++ {
		line := f.line(n)
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		comment := strings.HasPrefix(trimmed, "#")
		if indentWidth(line) <= indent {
			if comment {
				// Neither in the block nor the end of it. A comment at the key's own
				// level after the block belongs to whatever comes next.
				continue
			}
			break
		}
		last = n
	}
	return last
}

// line returns a 1-based source line, or empty past the end.
func (f *fixer) line(n int) string {
	if n < 1 || n > len(f.lines) {
		return ""
	}
	return f.lines[n-1]
}

// apply writes the edits into the source.
func (f *fixer) apply() []byte {
	var b strings.Builder
	for n := 1; n <= len(f.lines); n++ {
		edit, edited := f.edits[n]
		if !edited {
			b.WriteString(f.lines[n-1])
			b.WriteString(f.terminator)
			continue
		}
		for _, line := range edit.replacement {
			b.WriteString(line)
			b.WriteString(f.terminator)
		}
		n = edit.through
	}

	out := b.String()
	if !f.trailingNewline {
		out = strings.TrimSuffix(out, f.terminator)
	}
	return []byte(out)
}

// splitLines splits source into lines without their terminators.
//
// A carriage return is stripped with the newline rather than left on the end of
// the line, so that every line this package measures, compares, or re-indents is
// the line's text and nothing else. [fixer.terminator] puts it back.
func splitLines(data []byte) []string {
	text := strings.TrimSuffix(string(data), "\n")
	text = strings.TrimSuffix(text, "\r")
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSuffix(line, "\r")
	}
	return lines
}

// lineTerminator reports how a document ends its lines.
//
// The first one decides, because a document with both is already inconsistent and
// this is not the tool to normalise it: a rewriter that changed every line ending
// in a file it was asked to fix one step of would be doing something nobody asked
// for.
func lineTerminator(data []byte) string {
	if i := bytes.IndexByte(data, '\n'); i > 0 && data[i-1] == '\r' {
		return "\r\n"
	}
	return "\n"
}

// indentWidth returns how many leading spaces a line has.
func indentWidth(line string) int {
	for i, r := range line {
		if r != ' ' {
			return i
		}
	}
	return len(line)
}

// scalarText returns the text of a plain or quoted scalar.
func scalarText(n ast.Node) (string, bool) {
	switch node := n.(type) {
	case *ast.StringNode:
		return node.Value, true
	case *ast.LiteralNode:
		return blockText(node), true
	default:
		return "", false
	}
}

// quoteScalar writes a string as YAML, quoting only when it would otherwise read
// as something else.
//
// A rewriter that quotes everything produces a diff full of changes nobody asked
// for, and one that quotes nothing eventually writes a description of `yes` and
// turns it into a boolean.
func quoteScalar(s string) string {
	if s == "" {
		return `""`
	}
	if strings.ContainsAny(s, ":#\n\"'{}[]&*!|>%@`") || s != strings.TrimSpace(s) {
		return strconv.Quote(s)
	}
	if slices.Contains(yamlReservedScalars, strings.ToLower(s)) {
		return strconv.Quote(s)
	}
	if _, isNumber := parseYAMLNumber(s); isNumber {
		return strconv.Quote(s)
	}
	return s
}

// yamlReservedScalars are the unquoted words YAML reads as something other than
// a string.
var yamlReservedScalars = []string{
	"y", "n", "yes", "no", "true", "false", "on", "off", "null", "~",
}

// parseYAMLNumber reports whether a scalar would be read as a number.
func parseYAMLNumber(s string) (string, bool) {
	tok := token.New(s, s, nil)
	switch tok.Type {
	case token.IntegerType, token.FloatType, token.InfinityType, token.NanType:
		return s, true
	default:
		return "", false
	}
}

// stepIDs returns every step id written in a document, at any depth.
//
// Read from the YAML rather than from a compiled workflow, because a file being
// migrated is a file that does not compile yet — which is the whole reason it is
// here.
func stepIDs(n ast.Node) map[string]bool {
	ids := make(map[string]bool)
	var walk func(ast.Node)
	walk = func(n ast.Node) {
		switch node := unwrapAnchor(n).(type) {
		case *ast.MappingNode:
			for _, v := range node.Values {
				walk(v)
			}
		case *ast.MappingValueNode:
			if name, ok := keyNameOf(node.Key); ok && name == "id" {
				if text, ok := scalarText(node.Value); ok {
					ids[text] = true
				}
			}
			walk(node.Value)
		case *ast.SequenceNode:
			for _, v := range node.Values {
				walk(v)
			}
		}
	}
	walk(n)
	return ids
}

// expressions roots every bare step reference written in the document.
//
// A `${...}` is the whole of a value wherever it appears — the DSL has no string
// interpolation — so each rewrite is bounded to one scalar on one line, and a
// shape that is not is refused rather than guessed at.
//
// # Deferred inputs are not this rewriter's to touch
//
// The walk has to know when it is inside a task, because some inputs are not
// expressions over the workflow at all. The http task evaluates `expect:` and
// `outputs:` against the *response*, so `${status_code == 200}` names a field of
// what came back — and if a step in the same file happens to be called
// `status_code`, a rewriter that treats every fence alike roots it, and a correct
// expression silently starts meaning a step's outputs.
//
// That is the same reason the validator refuses to check references in these
// inputs. The registry declares which ones they are, and both surfaces ask it
// rather than each keeping a list.
func (f *fixer) expressions(n ast.Node, steps map[string]bool) {
	if len(steps) == 0 {
		return
	}
	// steps arrives as the ids in the document and shrinks on the way down, because
	// a name the *grammar* binds bare is that binding and not a step that happens to
	// share its spelling.
	//
	// [collectStepIdents] already tracks what CEL binds — a comprehension's iteration
	// variable — and says why: "rooting it would change what the expression means
	// rather than how it is written." It has no way to know what the grammar binds,
	// and the grammar binds three things bare: a loop's `as:`, a step's own `vars:`
	// keys, and `now` inside a wait. All three were made legal alongside a step of
	// the same id on purpose, so all three could be written and then silently
	// rewritten into a reference to that step.
	//
	// Subtracted from the candidate set rather than added to a bound set, so the
	// notes about deferred inputs narrow with it: suggesting the `steps.` spelling
	// for a name the author bound in a loop would send them to make the corruption
	// by hand.
	//
	// workflow marks the one mapping in the document that is not a step: the
	// workflow's own. Its `vars:` are not bound bare — they are read as
	// `vars.<name>` — so subtracting them there suppressed the rooting of a step
	// sharing a top-level var's name, which is the opposite failure and just as
	// bad. Only the document's body is the workflow; everything reached from it is
	// a step, a task, or something inside one.
	var walk func(n ast.Node, task taskScope, steps map[string]bool, workflow bool)
	walk = func(n ast.Node, task taskScope, steps map[string]bool, workflow bool) {
		switch node := unwrapAnchor(n).(type) {
		case *ast.MappingNode:
			// The bindings a mapping introduces are written as siblings of the
			// expressions that see them — `as:` beside `steps:`, `vars:` beside the
			// task — so they are read off the whole mapping first, and then applied
			// per key, because each is in scope for some of its siblings and not
			// others.
			vars, iterator, unresolvable := f.boundBareNames(node, workflow)
			if unresolvable != nil && len(steps) > 0 {
				// A binding written through an alias this walk cannot follow. Both
				// guesses corrupt — see [fixer.boundBareNames] — so nothing under this
				// mapping is rooted, and the author is told where. Refusing costs a
				// migration somebody finishes by hand; guessing costs a file that
				// still validates and computes something else.
				f.refuse(unresolvable,
					"this binding is written through an alias that cannot be resolved here, so the names it binds are unknown; write the name directly, or root the step references under this key by hand")
				steps = nil
			}

			for _, v := range node.Values {
				walk(v, task, sees(steps, v, vars, iterator), false)
			}
		case *ast.MappingValueNode:
			name, named := keyNameOf(node.Key)
			if named && name == waitUntilKey {
				// `now` is bound only here. Bound for this value alone rather than
				// for the step, because outside a wait it is an ordinary name and a
				// step may legitimately be called it.
				steps = without(steps, map[string]bool{nowBinding: true})
			}
			if named && task.deferred[name] {
				// One deferred scope this rewriter *can* see. The http task binds
				// exactly four names into its `expect:` and `outputs:` expressions,
				// and under the rooted grammar none of them can be anything else
				// there — a step is `steps.<id>` now, so a bare `body` is the
				// response's or it is unbound.
				if task.name == httpTaskName {
					f.rootResponse(node.Value)
				}

				// Evaluated by the task, in a scope this rewriter cannot see and
				// must not guess at — but not a scope where a step is unreachable.
				// The http task evaluates these under an activation whose *parent*
				// resolves step outputs, so a bare name here may be the response's
				// or may be a step's, and only the author knows which.
				//
				// So: not rewritten, and not silent either. Declining without
				// saying so would leave a reference that works today only because
				// the runtime still answers the bare spelling, and stops working
				// the day that arm is dropped.
				//
				// Except where the author already said which. A name the step binds
				// as a variable is that variable, not a step that happens to share
				// its spelling, and there is nothing conditional left to raise.
				candidates := steps
				if task.name == httpTaskName {
					// Rewritten above, so there is nothing conditional left to raise
					// — and a note suggesting the *step* spelling for a name that is
					// now the response's would send an author to undo a migration
					// this command just performed.
					candidates = without(candidates, responseNames)
				}
				f.noteDeferred(node.Value, name, candidates)
				f.recordDeferredValue(node.Value)

				return
			}
			// A task's key opens its inputs, so the names its own scope binds are
			// known from here down and nowhere else.
			if named {
				if def, known := v1.LookupTask(name); known {
					walk(node.Value, taskScope{
						name:     name,
						deferred: deferredInputs(def),
					}, steps, false)
					return
				}
			}
			walk(node.Value, task, steps, false)
		case *ast.SequenceNode:
			for _, v := range node.Values {
				walk(v, task, steps, false)
			}
		case *ast.StringNode:
			f.rootScalar(node, steps)
		}
	}
	walk(n, taskScope{}, steps, true)
}

// resolved follows anchors and aliases to the node that was actually written,
// mirroring [compiler.resolveQuiet].
//
// The rewriter needs it for the same reason the compiler does: the two have to agree
// about what a value *is*, and the compiler accepts `as: &n host` and `as: *hostname`
// as readily as `as: host`. A rewriter that reads only the third spelling does not
// merely miss a binding — it substitutes a different one, because a loop with an
// unreadable `as:` looks like a loop with none and falls back to [v1.DefaultIterator].
//
// The bool reports whether the value could be resolved at all. Bounded by
// [maxAliasDepth], because an alias cycle is a shape the parser accepts and this walk
// would otherwise follow forever — the compiler refuses one for the same reason.
func (f *fixer) resolved(n ast.Node) (ast.Node, bool) {
	for depth := 0; depth <= maxAliasDepth; depth++ {
		switch node := n.(type) {
		case nil:
			return nil, false
		case *ast.AnchorNode:
			n = node.Value
		case *ast.AliasNode:
			name, ok := scalarText(node.Value)
			if !ok {
				return nil, false
			}
			target, known := f.anchors[name]
			if !known {
				return nil, false
			}
			n = target
		default:
			return n, true
		}
	}

	return nil, false
}

// boundBareNames reads the bindings a mapping introduces: the keys of a step's
// own `vars:`, and the item name of a loop.
//
// Returned separately because they are in scope in different places, which is the
// whole difficulty — see [sees].
//
// The iterator is `item` when a loop writes no `as:`, matching [v1.IteratorName].
// A loop is recognised by carrying both `items:` and `steps:`, since that is what
// makes the default reachable at all: a mapping with an explicit `as:` announces
// itself, and one without announces nothing.
//
// workflow says the mapping is the document's own, whose `vars:` are the *workflow's*
// and are reached as `vars.<name>` rather than bare — so they bind nothing here. Read
// as bindings they made a top-level var sharing a step's id suppress that step's
// rooting, leaving a legacy reference bare in a file stamped with the new edition:
// `flow fix` exits zero and `flow validate` then rejects it. Only a *step's* vars are
// bare, and only inside that step.
//
// unresolvable is the value of a binding this walk could not read — an alias naming an
// anchor that is not in the document, or a cycle. It is returned rather than guessed
// around, because both guesses corrupt: assuming no binding roots the body's uses of
// it, and assuming the default subtracts a name the file never bound. The caller
// refuses instead.
func (f *fixer) boundBareNames(node *ast.MappingNode, workflow bool) (vars map[string]bool, iterator string, unresolvable ast.Node) {
	var (
		hasItems     bool
		hasSteps     bool
		unreadableAs ast.Node
	)

	for _, value := range node.Values {
		key, named := keyNameOf(value.Key)
		if !named {
			continue
		}

		switch key {
		case forEachItemsKey:
			hasItems = true

		case forEachStepsKey:
			hasSteps = true

		case forEachAsKey:
			written, ok := f.resolved(value.Value)
			if !ok {
				unreadableAs = value.Value

				continue
			}
			if name, isString := scalarText(written); isString && name != "" {
				iterator = name
			}

		case varsKey:
			if workflow {
				continue
			}
			written, ok := f.resolved(value.Value)
			if !ok {
				unresolvable = value.Value

				continue
			}
			if declared, isMapping := written.(*ast.MappingNode); isMapping {
				for _, entry := range declared.Values {
					if name, ok := keyNameOf(entry.Key); ok {
						if vars == nil {
							vars = map[string]bool{}
						}
						vars[name] = true
					}
				}
			}
		}
	}

	if !hasItems || !hasSteps {
		// Not a loop's inner mapping. An `as:` elsewhere is somebody's input named
		// `as`, and claiming it would suppress a rooting for no reason — and an
		// unreadable one is nothing to refuse over, for that same reason.
		iterator = ""
	} else {
		if unreadableAs != nil {
			unresolvable = unreadableAs
		}
		if iterator == "" {
			iterator = v1.DefaultIterator
		}
	}

	return vars, iterator, unresolvable
}

// sees returns the step ids one value of a mapping may still be rooted against.
//
// A binding is in scope for some of its siblings and not others, and getting that
// wrong costs a file in one direction or the other. Too narrow and a legacy
// reference is rewritten into the binding — the corruption this is here to stop.
// Too wide and a legacy reference is *left* bare while the edition is stamped, so
// `flow fix` exits zero on a file the validator then rejects, which is the other
// thing this command promises not to do.
//
// The three scopes, each taken from where the engine evaluates the thing:
//
//   - A loop's item is bound for the body and nothing else. `items:` is evaluated
//     to produce the list before anything is bound, so a bare reference there is
//     a step.
//   - A step's `vars:` are bound for the rest of the step but not for its `if:`,
//     because the condition decides whether the step runs at all and is evaluated
//     first — `runNodes` says so where it does it. Nor for the `vars:` block
//     itself, where they are being defined rather than read.
//   - `now` is handled at the wait's own key, since it is in scope for one value.
func sees(steps map[string]bool, value *ast.MappingValueNode, vars map[string]bool, iterator string) map[string]bool {
	key, named := keyNameOf(value.Key)
	if !named {
		return steps
	}

	switch key {
	case conditionKey, varsKey:
		// Neither sees the step's own vars.
		return steps

	case forEachStepsKey:
		if iterator != "" {
			return without(steps, map[string]bool{iterator: true})
		}

		return without(steps, vars)

	default:
		return without(steps, vars)
	}
}

// recordDeferredValue marks the lines a deferred input's value occupies.
//
// See [fixer.deferredValueLines] for what they are for. A span is used rather than
// the node's text because a block scalar spans several lines, and every one of them
// is expression source.
func (f *fixer) recordDeferredValue(value ast.Node) {
	span := spanOfNode(value)
	if !span.IsValid() {
		return
	}

	if f.deferredValueLines == nil {
		f.deferredValueLines = map[int]bool{}
	}
	for line := span.Start.Line; line <= span.End.Line; line++ {
		f.deferredValueLines[line] = true
	}
}

// taskScope is what the walk knows once it is inside a task's inputs.
type taskScope struct {
	// name is the task whose inputs these are, or empty outside one.
	//
	// Carried because one task's private scope has a *known* shape: the http task
	// binds four response names, so a bare one of those inside `expect:` or
	// `outputs:` is unambiguous and can be rewritten rather than only reported. No
	// other deferred scope is knowable, which is why this is a name and not a flag.
	name string

	// deferred names the inputs the task evaluates itself.
	deferred map[string]bool
}

// without returns the names in base that are not in remove.
func without(base, remove map[string]bool) map[string]bool {
	if len(remove) == 0 {
		return base
	}
	out := make(map[string]bool, len(base))
	for name := range base {
		if !remove[name] {
			out[name] = true
		}
	}
	return out
}

// deferredInputs returns the inputs a task evaluates itself, as a set.
//
// It used to subtract the one input the compiler emptied into the inputs around it,
// because the registry deferred that input and the compiler had already dissolved it
// by the time anything else looked. Both the hoist and the task that needed it retired
// at edition v2026.2, so the registry's list is the whole answer again.
func deferredInputs(def v1.TaskDef) map[string]bool {
	out := make(map[string]bool, len(def.DeferredInputs))
	for _, name := range def.DeferredInputs {
		out[name] = true
	}
	if len(out) == 0 {
		return nil
	}

	return out
}

// httpTaskName is the one task whose private scope this rewriter knows the shape of.
const httpTaskName = "http"

// rootResponse rewrites the bare response names in an `expect:` or `outputs:` value.
//
// Deliberately not going through [fixer.rootScalar]: that one requires a fence, and an
// `outputs:` mapping is written as ordinary keys whose *values* are fenced, while an
// `expect:` is a fenced scalar. Both shapes reach here, and the recursion handles the
// mapping by descending into it.
func (f *fixer) rootResponse(n ast.Node) {
	switch node := unwrapAnchor(n).(type) {
	case *ast.MappingNode:
		for _, v := range node.Values {
			f.rootResponse(v.Value)
		}

		return
	case *ast.MappingValueNode:
		f.rootResponse(node.Value)

		return
	case *ast.SequenceNode:
		for _, v := range node.Values {
			f.rootResponse(v)
		}

		return
	case *ast.StringNode:
		f.rootResponseScalar(node)

		return
	}
}

// rootResponseScalar rewrites one scalar holding a response expression.
//
// The fence is optional here and that is not an oversight: `expect:` is *always* an
// expression, fenced or not, so a file written either way has to migrate. The rewritten
// text goes back in the spelling it was found in.
func (f *fixer) rootResponseScalar(node *ast.StringNode) {
	source, fenced := SplitFence(node.Value)
	if !fenced {
		source = node.Value
	}

	rooted, changed, err := rootedResponseExpr(source)
	if err != nil {
		f.refuse(node, "%s", err.Error())

		return
	}
	if !changed {
		return
	}

	span := spanOfNode(node)
	if !span.IsValid() || span.Start.Line != span.End.Line {
		f.refuse(node,
			"this expression spans more than one line, so it cannot be rewritten by splicing; root its response references by hand")

		return
	}

	want, replacement := source, rooted
	if fenced {
		want, replacement = fenceOpen+source+fenceClose, fenceOpen+rooted+fenceClose
	}

	line := f.line(span.Start.Line)
	from, located := byteOffsetOfColumn(line, span.Start.Column)
	if !located {
		return
	}
	if from+len(want) > len(line) || line[from:from+len(want)] != want {
		// Quoted, most likely: `outputs: "${ … }"` puts the value one column right of
		// where the scalar's own span starts. The search begins at the value's own
		// column rather than at the start of the line, for the same reason
		// [fixer.rootScalar] does: a line can hold more than one expression — a
		// comment, or another value in flow style — and the whole point of having a
		// position is not to rewrite one of those instead.
		at := strings.Index(line[from:], want)
		if at < 0 {
			f.refuse(node,
				"this value is not shaped so its response references can be rewritten safely; root them by hand")

			return
		}
		from += at
	}

	f.lines[span.Start.Line-1] = line[:from] + replacement + line[from+len(want):]
	f.substituted = true
	f.changes = append(f.changes, FixChange{
		Line:    span.Start.Line,
		Message: "response references rooted under `" + v1.ResponseRoot + ".`",
	})
}

// rootScalar rewrites one fenced scalar in place.
func (f *fixer) rootScalar(node *ast.StringNode, steps map[string]bool) {
	inner, fenced := SplitFence(node.Value)
	if !fenced {
		return
	}

	rooted, changed, err := rootedExpr(inner, steps)
	if err != nil {
		f.refuse(node, "%s", err.Error())
		return
	}
	if !changed {
		return
	}

	span := spanOfNode(node)
	if !span.IsValid() || span.Start.Line != span.End.Line {
		f.refuse(node,
			"this expression spans more than one line, so it cannot be rewritten by splicing; root its step references by hand")
		return
	}

	line := f.line(span.Start.Line)
	want, replacement := fenceOpen+inner+fenceClose, fenceOpen+rooted+fenceClose

	// Located from the value's own column rather than by searching the line, so a
	// fence written earlier on the same line — in a comment, or in another value in
	// flow style — cannot be the one rewritten.
	from, located := byteOffsetOfColumn(line, span.Start.Column)
	if !located {
		return
	}
	at := strings.Index(line[from:], want)
	if at < 0 {
		f.refuse(node,
			"this expression is not written on its line the way it was read, which happens with a block or folded scalar; root its step references by hand")
		return
	}
	at += from

	f.lines[span.Start.Line-1] = line[:at] + replacement + line[at+len(want):]
	f.substituted = true
	f.changes = append(f.changes, FixChange{
		Line:    span.Start.Line,
		Message: fmt.Sprintf("step references rooted under `%s`", v1.StepsRoot),
	})
}

// byteOffsetOfColumn converts a 1-based code-point column into a byte index into
// line, and reports whether the column is on the line at all.
//
// The parser counts a column in code points — `{p: "🎵é", q: "${a.b}"}` puts `q`'s
// value at column 18 of a line 24 bytes long — while a Go string is indexed in
// bytes. Subtracting one from the column and indexing with it is the same number
// only for an ASCII line, and it is wrong in the direction that matters: the offset
// lands *left* of the value, so a splice located "from the value's own column"
// starts its search before the value, and an identical `${...}` written earlier on
// the line is the one rewritten. That is precisely the case the column was being
// used to rule out. See the same unit mismatch in [rootedUnder], and [markerSpan],
// which has always counted runes.
func byteOffsetOfColumn(line string, column int) (int, bool) {
	if column < 1 {
		return 0, false
	}
	remaining := column - 1
	for offset := range line {
		if remaining == 0 {
			return offset, true
		}
		remaining--
	}
	if remaining == 0 {
		// One past the last rune: the end of the line, which is where a span may
		// legitimately start when nothing was written.
		return len(line), true
	}
	return 0, false
}

// noteCommentsMentioningExpressions points at prose that talks about code the
// rewriter has just moved.
//
// A comment is not rewritten. Its `${a.result}` may be an example, a caveat, or a
// sentence about a step that no longer exists, and a tool that edits prose to
// match code it did not write will eventually write nonsense confidently. But a
// comment describing the old spelling is stale the moment the file is migrated,
// so the one useful thing is to say where it is.
func (f *fixer) noteCommentsMentioningExpressions() {
	for n := 1; n <= len(f.lines); n++ {
		line := f.line(n)
		hash := commentStart(line)
		if hash < 0 || !strings.Contains(line[hash:], fenceOpen) {
			continue
		}
		f.note(n, hash+1,
			"this comment mentions an expression; comments are prose and are not rewritten, so check whether it still describes the file")
	}
}

// commentStart returns the byte offset of a line's comment, or -1.
//
// Conservative on purpose: a `#` on a line carrying a quote may be inside a
// string, and this exists to point somewhere useful rather than to lex YAML.
// Missing a note costs nothing; a wrong one wastes a reader's time.
func commentStart(line string) int {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "#") {
		return strings.Index(line, "#")
	}
	if strings.ContainsAny(line, `"'`) {
		return -1
	}
	if i := strings.Index(line, " #"); i >= 0 {
		return i + 1
	}
	return -1
}

// noteDeferred points at a deferred input that mentions something spelled like a
// step.
//
// It cannot be rewritten — see the caller — but it is the one place a bare step
// reference can survive this migration, so leaving it unremarked is how it gets
// found later, by a run that stops working for no visible reason.
//
// # A deferred input is read as an expression whether or not it is fenced
//
// Deferring an input is the registry declaring that the task evaluates it, so its
// value is an expression by construction — and it may be written with a fence or
// without one. `http`'s `expect:` carries a fence because it could have been a
// literal; an input that *is* expression source, where a fence would be a fence
// around a fence, is written bare.
//
// Only the fenced half used to be read, which left the bare half silent. Nothing else
// catches it: the fence rewriter never sees an unfenced value, and the validator does
// not reference-check a deferred input at all, so a bare `expr: a.result` migrated
// clean while still meaning the pre-root spelling.
//
// No built-in is written bare any more — the one that was, `cel`'s `expr:`, retired at
// edition v2026.2 — so the branch is exercised by a registered task in the tests
// instead. It stays because a plugin may declare one, and removing a capability from
// plugins to tidy the built-ins is the wrong trade.
//
// It reads one scalar rather than descending, which is what a deferred input is: one
// expression the task evaluates. A deferred input holding a *mapping* used to exist —
// `cel`'s `vars:`, which the compiler flattened into ordinary inputs before the engine
// saw it — and descending into it here would have reported entries the validator
// already reports, in weaker words than the diagnostic they already get.
func (f *fixer) noteDeferred(n ast.Node, input string, steps map[string]bool) {
	text, ok := scalarText(n)
	if !ok {
		return
	}

	// SplitFence answers the empty string when there is no fence, so the bare form
	// has to fall back to the whole value rather than to what it returned.
	source, fenced := SplitFence(text)
	if !fenced {
		source = text
	}

	// Text that does not parse as CEL is not a stranded reference, it is a string
	// that happens to contain a word. Declining on the parse error is what keeps
	// this from inventing a migration for ordinary prose.
	rooted, changed, err := rootedExpr(source, steps)
	if err != nil || !changed {
		return
	}

	// Suggested back in the spelling it was written in, so it can be pasted.
	suggestion := rooted
	if fenced {
		suggestion = fenceOpen + rooted + fenceClose
	}

	span := spanOfNode(n)
	f.note(span.Start.Line, span.Start.Column,
		"`%s` is evaluated by the task against its own scope, so this was left alone — "+
			"but it names something spelled like a step. If it means the step, write it `%s`",
		input, suggestion)
}
