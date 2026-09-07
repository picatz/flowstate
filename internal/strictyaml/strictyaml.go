// Package strictyaml is the one place this module decodes YAML it did not
// write itself: an operator's trust, egress, task-shape or pins policy, a test
// document, a plugin's catalog.
//
// It exists for a defect in the pinned decoder rather than for anything about
// those formats. goccy/go-yaml v1.19.2 dereferences a nil iterator when a tag
// precedes a value of the wrong shape where a slice (or a map) is expected:
// `deny: ! ` is enough, and `audiences: !x` with a scalar beneath it is the
// same panic through [ast.TagNode.ArrayRange], which answers nil for a tagged
// value that is not a sequence and whose caller in `decodeSlice` asks the nil
// for its length. FuzzLoadSource found the shape first (#877); FuzzParseConfig
// and FuzzParsePolicy found it again at two more boundaries (#1721), which is
// what moved the containment from flowtest's own decoder into this package.
//
// A document at those boundaries is untrusted input, and a refusal is the only
// answer a parser may give to one it cannot read. A panic is the process: for
// `flow server` it is every tenant's run, for `flow mcp` every session the
// server holds. So the decode is contained here, at the one call, rather than
// left to whatever recovery happens to be above it, and a guard test refuses a
// bare decode anywhere else in the module so the next parser cannot forget.
//
// Delete the recovery when the dependency is fixed. The scope is deliberately
// the decoder's call alone, so a panic raised by a caller's own checks is
// never swallowed with it.
package strictyaml

import (
	"errors"
	"fmt"

	"github.com/goccy/go-yaml"
)

// ErrDecoderStopped is wrapped by the error [Unmarshal] returns for a decoder
// panic, so a caller that knows its own format can add the remedy in that
// format's words.
var ErrDecoderStopped = errors.New("the YAML decoder stopped on this document")

// Unmarshal decodes data into the value, with the decoder's own panics turned
// into an error naming the shape it is known to stop on. Options are the
// decoder's; [UnmarshalStrict] is the form every boundary parser wants.
func Unmarshal(data []byte, into any, opts ...yaml.DecodeOption) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = fmt.Errorf("%w (%v); that is a defect in the decoder rather than a rule of this "+
			"format, and the shape it is known to stop on is a tagged value of the wrong kind where "+
			"a list or a mapping belongs — write the value out, as `[]` or `{}`, or drop the tag",
			ErrDecoderStopped, r)
	}()

	return yaml.UnmarshalWithOptions(data, into, opts...)
}

// UnmarshalStrict is [Unmarshal] under [yaml.Strict]: an unknown or duplicate
// field is an error, so a misspelled key in an operator's file fails at
// startup rather than silently dropping a restriction.
func UnmarshalStrict(data []byte, into any) error {
	return Unmarshal(data, into, yaml.Strict())
}
