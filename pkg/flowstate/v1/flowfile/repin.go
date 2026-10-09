package flowfile

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Repin is the explicit half of a `use:` pin.
//
// A pin is the author saying "I read these bytes". When a module legitimately
// changes, the pin of every file that uses it is stale, and the only way to a
// compiling tree is for someone to say again that they read the new bytes. That
// is a decision, so no command makes it as a side effect of something else:
// [Fix] reports a pin its own rewrite invalidated and never re-stamps one, and a
// compile that finds a stale pin refuses. [RepinUses] is the one place a pin is
// re-stamped, reached only by asking for it by name (`flow fix --repin`).
//
// What it rewrites is only ever the text of a digest the file already wrote:
//
//   - An unpinned `use:` entry is left unpinned. Repinning adopts a pin that was
//     already there; it does not start trusting a module that was not pinned.
//   - A pin that matches the module's bytes is left exactly as written, upper case
//     included, so a repin over a current tree changes nothing.
//   - The replacement is spliced into the line the digest is written on, so every
//     comment, quote style and blank line around it survives and the result is
//     what `flow fmt` already produced.
//
// And it refuses, rather than guesses, in every case where it cannot be sure what
// it is replacing: a module that does not resolve to a file this file may read, a
// pin that is not a digest at all (nothing says which bytes it meant), a digest
// not written on a single line, or two entries sharing one written digest that
// would need different values. A refusal leaves the whole file as it was, the way
// every other `flow fix` refusal does.
//
// The module's bytes are read once and hashed as read, the way the compiler reads
// them, so the digest written is the digest the next compile finds.

// RepinUses rewrites each stale `digest:` on a `use:` entry of source to the
// digest of the module it names now. file is where source was read from, which the
// modules are resolved against by the rule a compile resolves them by.
//
// The result is a [FixResult] so a caller reports it the way it reports any other
// rewrite: Changes lists each repin, Refusals lists each pin that could not be, and
// Source is the original unless the rewrite was complete.
func RepinUses(file string, source []byte) (FixResult, error) {
	if len(source) > maxBytes {
		return FixResult{}, fmt.Errorf("the source is %d bytes, more than the %d a Flowfile may hold", len(source), maxBytes)
	}

	pins, err := CallPins(source)
	if err != nil {
		return FixResult{}, err
	}
	result := FixResult{Source: source}

	var uses []CallPin
	for _, pin := range pins {
		if pin.Alias != "" {
			uses = append(uses, pin)
		}
	}
	if len(uses) > v1.MaxUsesPerFile {
		// The bound a compile holds a file to, applied where this reads a file per
		// pin: a file that names more modules than that does not compile either.
		result.Refusals = append(result.Refusals, Diagnostic{
			Line: uses[0].Line, Column: uses[0].Column,
			Message: fmt.Sprintf("holds %d pinned modules; the most a file uses is %d, so none was repinned", len(uses), v1.MaxUsesPerFile),
		})

		return result, nil
	}

	type edit struct {
		pin CallPin
		now string
	}
	var edits []edit
	// One read per resolved module per run, so two entries naming one file are
	// stamped from the same bytes and cannot come to differ.
	digests := map[string]string{}
	for _, pin := range uses {
		located := ResolveCallTarget(file, pin.Call)
		if message := explainRefusal(located, pin.Call, usingFile); message != "" {
			result.Refusals = append(result.Refusals, Diagnostic{
				Line: pin.Line, Column: pin.Column, Field: "use." + pin.Alias + ".digest",
				Message: fmt.Sprintf("module `%s` %s; there is nothing to read the new digest from", pin.Alias, message),
			})

			continue
		}
		now, seen := digests[located.Path]
		var data []byte
		var err error
		if !seen {
			data, err = readBoundedSource(located.Path)
		}
		if err != nil {
			result.Refusals = append(result.Refusals, Diagnostic{
				Line: pin.Line, Column: pin.Column, Field: "use." + pin.Alias + ".digest",
				Message: fmt.Sprintf("module `%s` (%s) could not be read: %s; there is nothing to read the new digest from", pin.Alias, pin.Call, err),
			})

			continue
		}
		if !seen {
			now = v1.ContentDigest(data)
			digests[located.Path] = now
		}

		written, shapeErr := v1.CanonicalContentDigest(pin.Digest)
		if shapeErr != nil {
			result.Refusals = append(result.Refusals, Diagnostic{
				Line: pin.Line, Column: pin.Column, Field: "use." + pin.Alias + ".digest",
				Message: fmt.Sprintf("the digest on module `%s` is %s, which is not the shape of a pin, so there is no telling which bytes it meant; "+
					"write `digest: %s` by hand if that is the module you read", pin.Alias, describeWrittenPin(pin.Digest), now),
			})

			continue
		}
		if written == now {
			continue
		}
		edits = append(edits, edit{pin: pin, now: now})
	}

	// One digest, one line: the splice replaces the text where it is written, so a
	// digest shared by two entries (through an alias) is one edit, and one that
	// would have to be two different values is not an edit at all.
	type site struct{ line int }
	chosen := map[site]string{}
	for _, e := range edits {
		key := site{e.pin.Line}
		if was, taken := chosen[key]; taken && was != e.now {
			result.Refusals = append(result.Refusals, Diagnostic{
				Line: e.pin.Line, Column: e.pin.Column, Field: "use." + e.pin.Alias + ".digest",
				Message: fmt.Sprintf("the digest here is shared by entries whose modules now hash to %s and %s, so no one value can replace it; "+
					"write each entry its own `digest:`", was, e.now),
			})

			continue
		}
		chosen[key] = e.now
	}
	if len(result.Refusals) > 0 {
		return result, nil
	}
	if len(edits) == 0 {
		return result, nil
	}

	if bytes.Contains(source, []byte("\r")) {
		// Line numbers and the splice below are by "\n"; a file with carriage returns
		// is not one this can edit in place without guessing, so it is not edited.
		result.Refusals = append(result.Refusals, Diagnostic{
			Line: edits[0].pin.Line, Column: edits[0].pin.Column, Field: "use." + edits[0].pin.Alias + ".digest",
			Message: "the file has CRLF line endings, which `flow fix --repin` does not edit in place; convert it to LF, or write the new digests by hand",
		})

		return result, nil
	}

	lines := bytes.Split(source, []byte("\n"))
	done := map[site]bool{}
	for _, e := range edits {
		key := site{e.pin.Line}
		if done[key] {
			continue
		}
		done[key] = true

		if e.pin.Line < 1 || e.pin.Line > len(lines) || bytes.Count(lines[e.pin.Line-1], []byte(e.pin.Digest)) != 1 {
			result.Refusals = append(result.Refusals, Diagnostic{
				Line: e.pin.Line, Column: e.pin.Column, Field: "use." + e.pin.Alias + ".digest",
				Message: fmt.Sprintf("the digest on module `%s` is not written once on its own line, so it cannot be replaced in place; "+
					"write `digest: %s` by hand", e.pin.Alias, e.now),
			})

			continue
		}
		lines[e.pin.Line-1] = bytes.Replace(lines[e.pin.Line-1], []byte(e.pin.Digest), []byte(e.now), 1)
	}
	if len(result.Refusals) > 0 {
		return result, nil
	}

	for _, e := range edits {
		result.Changes = append(result.Changes, FixChange{
			Line: e.pin.Line,
			Message: fmt.Sprintf("repinned module `%s` (%s) from %s to %s",
				e.pin.Alias, e.pin.Call, shortDigest(e.pin.Digest), shortDigest(e.now)),
			Pending: fmt.Sprintf("would repin module `%s` (%s) from %s to %s",
				e.pin.Alias, e.pin.Call, shortDigest(e.pin.Digest), shortDigest(e.now)),
		})
	}
	slices.SortStableFunc(result.Changes, func(a, b FixChange) int { return a.Line - b.Line })
	result.Source = bytes.Join(lines, []byte("\n"))

	// The rewrite is read back with the reader that found the pins: a result that
	// no longer parses, or that still holds a stale digest, is a defect in this
	// function and not something to hand an author's file.
	rewritten, err := CallPins(result.Source)
	if err != nil {
		return FixResult{}, fmt.Errorf("the repinned source could not be read back: %w", err)
	}
	for _, edit := range edits {
		if !slices.ContainsFunc(rewritten, func(p CallPin) bool {
			return p.Alias == edit.pin.Alias && strings.EqualFold(p.Digest, edit.now)
		}) {
			return FixResult{}, fmt.Errorf("the digest on module `%s` was not replaced; this is a defect in `flow fix --repin`, and the file was left as it was", edit.pin.Alias)
		}
	}

	return result, nil
}

// shortDigest cuts a digest to the algorithm and the first twelve hex characters,
// enough to tell two apart in a sentence that also names the module.
func shortDigest(digest string) string {
	const shown = len(v1.ContentDigestPrefix) + 12
	if len(digest) <= shown {
		return digest
	}

	return digest[:shown] + "…"
}
