package flowfile

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A formatter renders the workflow, and the workflow is not the whole file.
//
// [Marshal] writes a document from the schema, which is the right contract for
// it: the schema describes what a run is, and a comment is not part of that. But
// a file carries what its author wrote, and `flow fmt` rewrites the *file*, so
// rendering from the schema alone deletes every comment in it, which is the
// rewriter mistake CLAUDE.md records twice over (#381). A comment is where the
// *why* lives, and no formatter people trust throws one away.
//
// So formatting is two steps rather than one. [Marshal] renders the workflow, and
// then every comment the source carried is carried across onto the document that
// renders, anchored to the same thing it was anchored to before: the key it sat
// above, the value it trailed, the list entry it introduced. Blank-line grouping
// and key order stay the formatter's own opinions, because they are not content
// an author wrote down in words.
//
// The anchor is a *path* rather than a position, which is what makes this survive
// the normalizing a formatter does. `vars:` entries come back sorted and a folded
// scalar comes back on one line, and a comment above `retries:` is still above
// `retries:` afterwards wherever the key moved to.
//
// Where a path does not exist in the rendered document, nothing is written and
// the whole format is refused. That is fail-closed applied to an author's prose:
// a formatter choosing between wrong output and no output chooses no output, and
// a comment silently dropped is exactly the loss this file exists to stop. It
// happens for a comment anchored to something the document no longer spells the
// same way: inside a mapping reached through an alias the compiler expanded, or
// on a key written as its own default and therefore not written back.
//
// # A `digest:` pin gets the same carve-out, for a sharper reason
//
// [v1.Call.SourceDigest] cannot serve the way the rest of the schema does,
// because it is set whether or not an author wrote a pin — it is always the
// callee's digest, only ever *checked* against a pin when one was written. So
// Marshal's compiled workflow carries no signal of which calls were pinned, and
// a `digest:` an author wrote would be silently dropped exactly the way a
// comment would be, except a comment is prose and a pin is a security check
// (#339). A rewriter that drops a check with no diagnostic is the shape CLAUDE.md
// calls out worst: doing less than asked, silently.
//
// So a pin is read from source and carried across the same way a comment is —
// anchored to the mapping it sits in rather than to a position, verbatim rather
// than recomputed, and refused rather than dropped where the anchor no longer
// exists. It is placed *before* comments are, so that a comment written above or
// beside the pin itself has something to attach to once the pin exists again in
// the rendered tree.
//
// # A known gap: a pin reached only through `<<:`
//
// Both `call:` and `digest:` may legally arrive at a step via a YAML merge key
// (fields.go resolves one for any step property), and [collectPins] does not
// follow one to find them — it looks at the mapping's own written keys, the
// same as [collectComments] does. A step whose `digest:` sits inside the
// anchor a `<<: *shared` merges in, rather than written on the step itself, is
// therefore invisible to this carve-out and formats as though it were never
// pinned, with no diagnostic — silently, in that one specific and (nothing in
// examples/ or DSL.md demonstrates it) unusual shape. No example, test, or
// document in this repository writes a pin that way; closing this is tracked
// rather than attempted here, because doing it partially — resolving one
// level of alias and calling it done — is the shape of rewriter mistake
// CLAUDE.md already has two entries for.

// Format renders wf as the document [Marshal] writes, carrying across every
// comment in source.
//
// source is the text wf was compiled from. It is read only for its comments; the
// document written is Marshal's, so a file with no comments in it formats to
// exactly the bytes Marshal produces and nothing about the canonical shape moves.
//
// It reports [Diagnostics], positioned at the comment, for a comment that cannot
// be carried across, and Marshal's own error for a workflow that cannot be
// written at all. Both mean the same thing to a caller: there is nothing safe to
// write, so write nothing.
func Format(source []byte, wf *v1.Workflow) ([]byte, error) {
	// The same byte bound Parse and Fix hold, because this parses bytes an
	// outside party wrote and an exported entry point does not get to assume
	// its caller compiled them first. With the bytes bounded, the walk's node
	// count is bounded too: goyaml's parser keeps aliases as alias nodes
	// rather than expanding them, so breadth cannot exceed what the bytes
	// spell out, and depth carries its own bound below.
	if len(source) > maxBytes {
		return nil, fmt.Errorf("the source is %d bytes, more than the %d a Flowfile may hold", len(source), maxBytes)
	}

	formatted, err := Marshal(wf)
	if err != nil {
		return nil, err
	}

	comments, err := sourceComments(source)
	if err != nil {
		return nil, err
	}
	pins, err := sourcePins(source)
	if err != nil {
		return nil, err
	}
	if len(comments) == 0 && len(pins) == 0 {
		// The overwhelmingly common file, and the one whose output must not move:
		// with nothing to carry, the answer is Marshal's bytes themselves rather
		// than a re-rendering of them that might differ in some corner.
		return formatted, nil
	}

	rendered, err := parser.ParseBytes(formatted, parser.ParseComments)
	if err != nil {
		// Marshal wrote it, so this cannot happen from a Flowfile; if it ever
		// does, the honest answer is that the document is not one this can
		// annotate rather than a file written without its comments.
		return nil, fmt.Errorf("the formatted document could not be read back to place comments in: %w", err)
	}

	// Pins before comments: a pin is inserted as a real key into a mapping that
	// otherwise has none, and a comment anchored to that key — above it, beside
	// it — needs the key to already exist to attach to.
	placedPins := make(map[string]bool, len(pins))
	for _, doc := range rendered.Docs {
		if err := placePins(doc.Body, "", 0, pins, placedPins); err != nil {
			return nil, err
		}
	}
	if diagnostics := unplacedPins(pins, placedPins); len(diagnostics) > 0 {
		return nil, diagnostics
	}

	placed := make(map[commentAnchor]bool, len(comments))
	for _, doc := range rendered.Docs {
		if err := placeComments(doc.Body, "", 0, comments, placed); err != nil {
			return nil, err
		}
	}

	if diagnostics := unplaced(comments, placed); len(diagnostics) > 0 {
		return nil, diagnostics
	}

	out := []byte(rendered.String())
	// A document rendered from an AST carries no trailing newline of its own,
	// and every other file this package writes ends in one.
	if !bytes.HasSuffix(out, []byte("\n")) {
		out = append(out, '\n')
	}
	return out, nil
}

// A commentAnchor names the one place in a document a comment group belongs.
//
// The path is built from the keys and indices on the way down rather than from
// [ast.Node.GetPath], because a YAML path is written with dots and brackets and a
// Flowfile key may contain either, so `steps[0].a.b` addresses two different
// things depending on whether a key is called `a.b`. Each step here is written with its
// own length in front of it, which no key can forge.
type commentAnchor struct {
	path string
	kind commentKind
}

// A commentKind distinguishes the places a comment can sit on one node, which a
// path alone cannot: a mapping entry has a comment above it, a comment after its
// key, and a comment under it, and its value has one of its own.
type commentKind int

const (
	// commentHead is the block above a mapping entry or a sequence entry.
	commentHead commentKind = iota

	// commentKeyLine is the comment after `key:` on a line whose value is a
	// block below it.
	commentKeyLine

	// commentLine is the comment after a scalar value on its own line.
	commentLine

	// commentFoot is the block below a mapping entry, which is where a comment
	// at the end of a file lands: YAML reads it as belonging under the last
	// entry it follows.
	commentFoot

	// commentContainerFoot is the same for a mapping or a sequence itself. It is
	// a separate kind because a mapping entry and the value under it share a
	// path, so one kind would have them overwrite each other.
	commentContainerFoot

	// commentContainerHead is a comment the parser hangs on a container node
	// itself, which is where prose above an inline `{}` or `[]` lands. Its own
	// kind for the same overwrite reason as the foot.
	commentContainerHead
)

// sourceComments collects every comment in a document, keyed by where it sits.
func sourceComments(source []byte) (map[commentAnchor]*ast.CommentGroupNode, error) {
	file, err := parser.ParseBytes(source, parser.ParseComments)
	if err != nil {
		// The caller compiled this source, so it parses. Refusing rather than
		// carrying on is the fail-closed reading: unable to see the comments is
		// not the same as knowing there are none.
		return nil, fmt.Errorf("the source could not be read to collect its comments: %w", err)
	}

	out := map[commentAnchor]*ast.CommentGroupNode{}
	for _, doc := range file.Docs {
		if err := collectComments(doc.Body, "", 0, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// collectComments walks a source document, recording each comment under the path
// of the thing it is written against.
func collectComments(n ast.Node, path string, depth int, out map[commentAnchor]*ast.CommentGroupNode) error {
	if n == nil {
		return nil
	}
	if depth > maxDepth {
		// The same bound the compiler walks under, for the same reason: this
		// walks a tree an outside party wrote, and a walk with no bound is one a
		// document can drive off the stack.
		return fmt.Errorf("nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
	}

	record := func(kind commentKind, group *ast.CommentGroupNode) {
		if group != nil {
			out[commentAnchor{path: path, kind: kind}] = group
		}
	}

	switch x := n.(type) {
	case *ast.MappingNode:
		// A comment can sit on the mapping itself as well as under it: the
		// parser hangs prose above an inline `{}` off the container, and a
		// collector that only read the foot would never see it, which is a
		// silent deletion rather than a refusal.
		record(commentContainerHead, x.GetComment())
		record(commentContainerFoot, x.FootComment)
		for _, value := range x.Values {
			if err := collectComments(value, path, depth+1, out); err != nil {
				return err
			}
		}

	case *ast.MappingValueNode:
		child := childPath(path, keyStep(x))
		recordAt := func(kind commentKind, group *ast.CommentGroupNode) {
			if group != nil {
				out[commentAnchor{path: child, kind: kind}] = group
			}
		}
		recordAt(commentHead, x.GetComment())
		recordAt(commentKeyLine, x.Key.GetComment())
		recordAt(commentFoot, x.FootComment)
		if err := collectComments(x.Value, child, depth+1, out); err != nil {
			return err
		}

	case *ast.SequenceNode:
		record(commentContainerFoot, x.FootComment)
		for i, value := range x.Values {
			element := childPath(path, indexStep(i))
			if group := sequenceHead(x, i); group != nil {
				out[commentAnchor{path: element, kind: commentHead}] = group
			}
			if err := collectComments(value, element, depth+1, out); err != nil {
				return err
			}
		}

	default:
		record(commentLine, n.GetComment())
	}

	return nil
}

// placeComments walks the rendered document, writing each comment onto the node
// at the path it was collected from.
//
// It mirrors [collectComments] exactly, because a comment is placed where the
// same walk would have found it: the two functions are one rule read in two
// directions, and a shape one of them knows about and the other does not is a
// comment that goes missing.
func placeComments(n ast.Node, path string, depth int, in map[commentAnchor]*ast.CommentGroupNode, placed map[commentAnchor]bool) error {
	if n == nil {
		return nil
	}
	if depth > maxDepth {
		return fmt.Errorf("nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
	}

	take := func(kind commentKind) *ast.CommentGroupNode {
		anchor := commentAnchor{path: path, kind: kind}
		group, ok := in[anchor]
		if !ok {
			return nil
		}
		placed[anchor] = true
		return group
	}

	switch x := n.(type) {
	case *ast.MappingNode:
		if group := take(commentContainerHead); group != nil {
			// Attempted rather than assumed: a rendered block mapping may not
			// carry an attached comment the way an inline one does. The byte
			// tests hold the outcome to preserved-or-refused; what this branch
			// guarantees is that the comment entered the anchor map at all, so
			// a container the renderer cannot annotate refuses instead of
			// deleting.
			_ = x.SetComment(group)
		}
		if group := take(commentContainerFoot); group != nil {
			x.FootComment = group
		}
		for _, value := range x.Values {
			if err := placeComments(value, path, depth+1, in, placed); err != nil {
				return err
			}
		}

	case *ast.MappingValueNode:
		child := childPath(path, keyStep(x))
		takeAt := func(kind commentKind) *ast.CommentGroupNode {
			anchor := commentAnchor{path: child, kind: kind}
			group, ok := in[anchor]
			if !ok {
				return nil
			}
			placed[anchor] = true
			return group
		}
		if group := takeAt(commentHead); group != nil {
			x.Comment = group
		}
		if group := takeAt(commentKeyLine); group != nil {
			_ = x.Key.SetComment(group)
		}
		if group := takeAt(commentFoot); group != nil {
			x.FootComment = group
		}
		if err := placeComments(x.Value, child, depth+1, in, placed); err != nil {
			return err
		}

	case *ast.SequenceNode:
		if group := take(commentContainerFoot); group != nil {
			x.FootComment = group
		}
		// Written to the length the renderer requires: it only reads the head
		// comments at all when there is exactly one per value.
		if len(x.ValueHeadComments) != len(x.Values) {
			x.ValueHeadComments = make([]*ast.CommentGroupNode, len(x.Values))
		}
		for i, value := range x.Values {
			element := childPath(path, indexStep(i))
			anchor := commentAnchor{path: element, kind: commentHead}
			if group, ok := in[anchor]; ok {
				setSequenceHead(x, i, group)
				placed[anchor] = true
			}
			if err := placeComments(value, element, depth+1, in, placed); err != nil {
				return err
			}
		}

	default:
		if group := take(commentLine); group != nil {
			_ = n.SetComment(group)
		}
	}

	return nil
}

// sequenceHead returns the comment block above the i'th entry of a sequence.
//
// The first entry's is the sequence's own comment rather than an entry of
// ValueHeadComments, which is how the parser records it: a block above the first
// `- ` is read as introducing the sequence. [setSequenceHead] writes it back the
// same way, so a round trip puts it where it was.
func sequenceHead(n *ast.SequenceNode, i int) *ast.CommentGroupNode {
	if i < len(n.ValueHeadComments) && n.ValueHeadComments[i] != nil {
		return n.ValueHeadComments[i]
	}
	if i == 0 {
		return n.GetComment()
	}
	return nil
}

// setSequenceHead writes the comment block above the i'th entry of a sequence,
// the way [sequenceHead] reads one.
func setSequenceHead(n *ast.SequenceNode, i int, group *ast.CommentGroupNode) {
	if i == 0 {
		_ = n.SetComment(group)
		return
	}
	n.ValueHeadComments[i] = group
}

// unplaced turns every comment that found no home into a diagnostic positioned at
// the comment itself, so an author reading the refusal is sent to the line that
// stopped it rather than to the file in general.
func unplaced(comments map[commentAnchor]*ast.CommentGroupNode, placed map[commentAnchor]bool) Diagnostics {
	var out Diagnostics
	for anchor, group := range comments {
		if placed[anchor] {
			continue
		}
		diagnostic := Diagnostic{
			Message: "comment cannot be kept: what it is written against is not written back in the same shape " +
				"(a mapping reached through an alias, or a block written back as one expression), so there is " +
				"nowhere left to put it and nothing was written; move it above a key that survives, or leave " +
				"this file unformatted",
		}
		if token := group.GetToken(); token != nil && token.Position != nil {
			diagnostic.Line = token.Position.Line
			diagnostic.Column = token.Position.Column
		}
		out = append(out, diagnostic)
	}

	// In source order, because the map they came out of has none and a refusal
	// that lists the same lines in a different order each run is a refusal
	// nothing can be diffed against.
	slices.SortStableFunc(out, func(a, b Diagnostic) int {
		if a.Line != b.Line {
			return a.Line - b.Line
		}
		return a.Column - b.Column
	})
	return out
}

// keyStep returns a mapping entry's key as it addresses the entry.
//
// Taken from the key's token rather than from its rendered string, because a key
// carrying a line comment renders as `log # why`, and a path built from that
// addresses nothing.
func keyStep(n *ast.MappingValueNode) string {
	if token := n.Key.GetToken(); token != nil {
		return token.Value
	}
	return n.Key.String()
}

// childPath extends a path by one step, length-prefixed so that no key can be
// read as more than one step.
func childPath(path, step string) string {
	return path + "/" + strconv.Itoa(len(step)) + ":" + step
}

// indexStep names the i'th element of a sequence. It cannot collide with a key,
// because a key's step carries the key's own length in front of it.
func indexStep(i int) string {
	return "[" + strconv.Itoa(i) + "]"
}

// A sourcePin is a `digest:` value found beside a `call:` in source, along
// with where it was written — for a diagnostic if it cannot be carried across.
type sourcePin struct {
	text         string
	line, column int
}

// sourcePins collects every `digest:` pin in a document, keyed by the path of
// the mapping it sits in — the same path a comment on that mapping's own
// container gets, since a pin and a container comment are both properties of
// the mapping rather than of one entry in it.
//
// Only a mapping that also has a `call:` key is recorded. The grammar refuses
// `digest:` anywhere else (parse.go), so this is not a filter this package
// invents; it means a hand-built AST fed to this function (which, unlike a
// parsed Flowfile, [Format]'s exported contract does not get to assume is
// well-formed) cannot make it write a pin that names nothing.
func sourcePins(source []byte) (map[string]sourcePin, error) {
	file, err := parser.ParseBytes(source, parser.ParseComments)
	if err != nil {
		// The caller compiled this source, so it parses. Refusing rather than
		// carrying on is the fail-closed reading, the same one sourceComments
		// takes: unable to see the pins is not the same as knowing there are
		// none.
		return nil, fmt.Errorf("the source could not be read to collect its call pins: %w", err)
	}

	out := map[string]sourcePin{}
	for _, doc := range file.Docs {
		if err := collectPins(doc.Body, "", 0, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// collectPins walks a source document exactly the way [collectComments] does —
// same node kinds, same path arithmetic — because a pin is placed by the same
// walk finding the same path later, and a shape one of them knows about and the
// other does not is a pin that goes missing.
func collectPins(n ast.Node, path string, depth int, out map[string]sourcePin) error {
	if n == nil {
		return nil
	}
	if depth > maxDepth {
		return fmt.Errorf("nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
	}

	switch x := n.(type) {
	case *ast.MappingNode:
		var hasCall bool
		var digest *ast.MappingValueNode
		for _, entry := range x.Values {
			switch keyStep(entry) {
			case "call":
				hasCall = true
			case "digest":
				digest = entry
			}
		}
		if hasCall && digest != nil {
			// A digest reaching here compiled, through [compiler.text], which
			// accepts exactly the two node kinds scalarText does — so `ok` is
			// always true for a Flowfile Format was actually given one of.
			// Skipped rather than recorded on the rare hand-built AST it is
			// not: an entry this cannot read the value of is not a pin this
			// can carry, and pretending it is one with an empty string would
			// write `digest: ` into the mapping it lands in.
			if text, ok := scalarText(digest.Value); ok {
				pin := sourcePin{text: text}
				if token := digest.Key.GetToken(); token != nil && token.Position != nil {
					pin.line, pin.column = token.Position.Line, token.Position.Column
				}
				out[path] = pin
			}
		}
		for _, value := range x.Values {
			if err := collectPins(value, path, depth+1, out); err != nil {
				return err
			}
		}

	case *ast.MappingValueNode:
		child := childPath(path, keyStep(x))
		if err := collectPins(x.Value, child, depth+1, out); err != nil {
			return err
		}

	case *ast.SequenceNode:
		for i, value := range x.Values {
			element := childPath(path, indexStep(i))
			if err := collectPins(value, element, depth+1, out); err != nil {
				return err
			}
		}
	}

	return nil
}

// placePins walks the rendered document the same way [placeComments] does,
// and where the path matches a mapping [sourcePins] recorded, writes the pin
// into that mapping as a real `digest:` entry rather than a comment — right
// after `call:`, the position [DSL.md] and every example write one in.
func placePins(n ast.Node, path string, depth int, in map[string]sourcePin, placed map[string]bool) error {
	if n == nil {
		return nil
	}
	if depth > maxDepth {
		return fmt.Errorf("nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
	}

	switch x := n.(type) {
	case *ast.MappingNode:
		if pin, ok := in[path]; ok {
			ok, err := insertPin(x, pin.text)
			if err != nil {
				return err
			}
			if ok {
				placed[path] = true
			}
		}
		for _, value := range x.Values {
			if err := placePins(value, path, depth+1, in, placed); err != nil {
				return err
			}
		}

	case *ast.MappingValueNode:
		child := childPath(path, keyStep(x))
		if err := placePins(x.Value, child, depth+1, in, placed); err != nil {
			return err
		}

	case *ast.SequenceNode:
		for i, value := range x.Values {
			element := childPath(path, indexStep(i))
			if err := placePins(value, element, depth+1, in, placed); err != nil {
				return err
			}
		}
	}

	return nil
}

// insertPin writes digest as a new `digest:` entry in mapping, right after its
// `call:` entry, and reports whether there was a `call:` entry to write it
// after.
//
// A mapping with no `call:` entry is the anchor going stale: the document no
// longer spells this call the way it did when the pin was collected (an alias
// the compiler expanded, most likely, the same shape that already refuses a
// comment). Reporting false rather than inserting anywhere leaves that for
// [unplacedPins] to turn into a refusal, instead of writing a pin onto a
// mapping that was never the call it pinned.
//
// The entry is built by parsing a throwaway snippet rather than by
// constructing tokens by hand, because the encoder places a mapping key by the
// *column* its own token recorded rather than by its position in the slice —
// verified empirically before this shipped, since nothing in the library's own
// docs says so. Nesting the snippet one level deep, indented to the column
// `call:` itself sits at, is what makes the parser hand back a key token at
// that same column, which is what makes the entry land at the right indent
// once it is spliced into the mapping's own entries.
func insertPin(mapping *ast.MappingNode, digest string) (bool, error) {
	callIndex := -1
	for i, entry := range mapping.Values {
		if keyStep(entry) == "call" {
			callIndex = i
			break
		}
	}
	if callIndex == -1 {
		return false, nil
	}

	column := 1
	if token := mapping.Values[callIndex].Key.GetToken(); token != nil && token.Position != nil {
		column = token.Position.Column
	}

	snippet := fmt.Sprintf("_:\n%sdigest: %s\n", strings.Repeat(" ", column-1), digest)
	parsed, err := parser.ParseBytes([]byte(snippet), parser.ParseComments)
	if err != nil {
		return false, fmt.Errorf("digest %q could not be written back: %w", digest, err)
	}
	wrapper, ok := parsed.Docs[0].Body.(*ast.MappingNode)
	if !ok || len(wrapper.Values) != 1 {
		return false, fmt.Errorf("digest %q could not be written back: unexpected document shape", digest)
	}
	inner, ok := wrapper.Values[0].Value.(*ast.MappingNode)
	if !ok || len(inner.Values) != 1 {
		return false, fmt.Errorf("digest %q could not be written back: unexpected document shape", digest)
	}
	entry := inner.Values[0]

	values := make([]*ast.MappingValueNode, 0, len(mapping.Values)+1)
	values = append(values, mapping.Values[:callIndex+1]...)
	values = append(values, entry)
	values = append(values, mapping.Values[callIndex+1:]...)
	mapping.Values = values

	return true, nil
}

// unplacedPins turns every pin that found no home into a diagnostic naming
// exactly what would be lost and why, the same rule [unplaced] applies to a
// comment — except a comment is prose an author can rewrite by hand from the
// refusal alone, and a pin is a security check, which is why this diagnostic
// says that in as many words rather than leaving a reader to infer it.
func unplacedPins(pins map[string]sourcePin, placed map[string]bool) Diagnostics {
	var out Diagnostics
	for path, pin := range pins {
		if placed[path] {
			continue
		}
		out = append(out, Diagnostic{
			Message: fmt.Sprintf(
				"digest: %s cannot be kept: the call it pins is not written back in the same shape "+
					"(reached through an alias the compiler expanded, most likely), so there is nowhere "+
					"left to put it; a formatter that dropped it here would silently turn off a security "+
					"check, so nothing was written instead — move the call out of whatever expands it, or "+
					"leave this file unformatted", pin.text),
			Line:   pin.line,
			Column: pin.column,
		})
	}

	// In source order, for the reason [unplaced] sorts its own diagnostics: a
	// refusal that lists the same lines in a different order each run is a
	// refusal nothing can be diffed against.
	slices.SortStableFunc(out, func(a, b Diagnostic) int {
		if a.Line != b.Line {
			return a.Line - b.Line
		}
		return a.Column - b.Column
	})
	return out
}
