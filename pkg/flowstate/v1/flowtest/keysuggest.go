package flowtest

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"

	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// unknownKeyRemedy is the sentence a strict decode's `unknown field` refusal
// was missing: the did-you-mean every *name* in the format already gets —
// steps, signals, stubs — over the keys legal where the unknown one was
// written, or that list itself when nothing is near (#1669). Field names are
// the first thing a newcomer gets wrong, and `unknown field "outputs"` with no
// hint at `expect:` sent them to the reference for a key they had nearly
// spelled.
//
// Which keys are legal is not asserted here twice: the decoder refused the key
// against a struct's yaml tags, and this resolves the same struct by walking
// the key's path in the parsed tree against [File]'s tags. Where the path runs
// into a map or an untyped value — `stubs[0].returns`, `vars:` — no struct
// decides the keys, and no remedy is offered rather than a wrong one.
//
// Empty when the refusal is not an unknown-field one, when the token cannot be
// found in the tree, or when nothing there is a struct.
func unknownKeyRemedy(err error, parsed *ast.File) string {
	var yamlErr yaml.Error
	if !errors.As(err, &yamlErr) || parsed == nil {
		return ""
	}
	key, ok := strings.CutPrefix(yamlErr.GetMessage(), `unknown field "`)
	if !ok {
		return ""
	}
	key = strings.TrimSuffix(key, `"`)
	tok := yamlErr.GetToken()
	if tok == nil || tok.Position == nil {
		return ""
	}

	var path string
	for _, doc := range parsed.Docs {
		if doc == nil || doc.Body == nil {
			continue
		}
		budget := maxLookupSteps
		if p, found := keyPathAt(doc.Body, tok.Position.Line, tok.Position.Column, &budget); found {
			path = p
			break
		}
	}
	if path == "" {
		return ""
	}

	legal := legalKeysAt(reflect.TypeOf(File{}), parentSegments(path))
	if len(legal) == 0 {
		return ""
	}
	if suggestion, ok := nearest.Name(key, legal); ok {
		return fmt.Sprintf("; did you mean %q?", suggestion)
	}
	return "; the keys legal here are: " + strings.Join(legal, ", ")
}

// keyPathAt finds the mapping key written at a position and returns its path,
// `$.tests[0].expect.outputs`, as the parser recorded it. Bounded by the same
// budget every other walk over a document spends, since the document is the
// author's.
func keyPathAt(n ast.Node, line, column int, budget *int) (string, bool) {
	if n == nil || *budget <= 0 {
		return "", false
	}
	*budget--

	switch node := n.(type) {
	case *ast.MappingNode:
		for _, v := range node.Values {
			if path, ok := keyPathAt(v, line, column, budget); ok {
				return path, true
			}
		}
	case *ast.MappingValueNode:
		if key := node.Key; key != nil {
			if tok := key.GetToken(); tok != nil && tok.Position != nil &&
				tok.Position.Line == line && tok.Position.Column == column {
				return key.GetPath(), true
			}
		}
		return keyPathAt(node.Value, line, column, budget)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			if path, ok := keyPathAt(v, line, column, budget); ok {
				return path, true
			}
		}
	case *ast.AnchorNode:
		return keyPathAt(node.Value, line, column, budget)
	case *ast.TagNode:
		return keyPathAt(node.Value, line, column, budget)
	}

	return "", false
}

// parentSegments splits a parser path into the steps leading to the key it
// names, dropping the key itself: `$.tests[0].expect.outputs` is
// [tests, [0], expect]. A quoted segment — the parser's spelling of a key
// holding a path character — is kept whole.
func parentSegments(path string) []string {
	path = strings.TrimPrefix(path, "$")

	var segments []string
	var current strings.Builder
	quoted := false
	flush := func() {
		if current.Len() > 0 {
			segments = append(segments, current.String())
			current.Reset()
		}
	}
	for _, r := range path {
		switch {
		case r == '\'':
			quoted = !quoted
		case quoted:
			current.WriteRune(r)
		case r == '.':
			flush()
		case r == '[':
			flush()
			current.WriteRune(r)
		case r == ']':
			current.WriteRune(r)
			flush()
		default:
			current.WriteRune(r)
		}
	}
	flush()

	if len(segments) == 0 {
		return nil
	}
	return segments[:len(segments)-1]
}

// legalKeysAt walks the segments from the root type and returns the yaml keys
// of the struct they land on, in declaration order, or nil when they land on
// anything that is not a struct.
func legalKeysAt(root reflect.Type, segments []string) []string {
	t := root
	for _, segment := range segments {
		t = deref(t)
		switch {
		case strings.HasPrefix(segment, "["):
			if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
				return nil
			}
			t = t.Elem()
		case t.Kind() == reflect.Map:
			// The segment is a key of the author's own; what it holds is the
			// map's value type.
			t = t.Elem()
		case t.Kind() == reflect.Struct:
			field, ok := fieldByYAMLKey(t, segment)
			if !ok {
				return nil
			}
			t = field.Type
		default:
			return nil
		}
	}

	t = deref(t)
	if t.Kind() != reflect.Struct {
		return nil
	}
	var keys []string
	for i := range t.NumField() {
		if key, ok := yamlKey(t.Field(i)); ok {
			keys = append(keys, key)
		}
	}
	return keys
}

func deref(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func fieldByYAMLKey(t reflect.Type, key string) (reflect.StructField, bool) {
	for i := range t.NumField() {
		field := t.Field(i)
		if name, ok := yamlKey(field); ok && name == key {
			return field, true
		}
	}
	return reflect.StructField{}, false
}

// yamlKey is the key a field decodes from: its yaml tag's name, or nothing for
// an unexported, untagged, or `-` field — the decoder's own rule, so the keys
// listed are exactly the ones it would have accepted.
func yamlKey(field reflect.StructField) (string, bool) {
	if !field.IsExported() {
		return "", false
	}
	tag, ok := field.Tag.Lookup("yaml")
	if !ok {
		return "", false
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" || name == "-" {
		return "", false
	}
	return name, true
}
