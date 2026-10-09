package flowdebug

import (
	"strconv"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// declaredShapes is what a workflow declares about its inputs, kept so an
// inspection can name a value by the record type its author gave it. A value
// at runtime is a CEL map and says nothing more; the declaration is where
// `Order` lives, and a debugger that shows `map` for it makes the reader
// translate back.
//
// The zero value and nil name nothing.
type declaredShapes struct {
	// workflow is the workflow the declarations belong to. An inspection held
	// inside a callee reads that callee's `inputs`, not these.
	workflow string
	inputs   map[string]*v1.Type
	table    v1.TypeTable
}

// shapesOf indexes the inputs wf declares by name, or returns nil when it
// declares no record type to name.
func shapesOf(wf *v1.Workflow) *declaredShapes {
	table := v1.TypesOf(wf)
	if len(table) == 0 {
		return nil
	}

	shapes := &declaredShapes{
		workflow: wf.GetName(),
		inputs:   make(map[string]*v1.Type, len(wf.GetDeclaredInputs())),
		table:    table,
	}
	for _, declaration := range wf.GetDeclaredInputs() {
		if t := declaration.DeclaredType(); t != nil {
			shapes.inputs[declaration.GetName()] = t
		}
	}

	return shapes
}

// recordAt names the record type the value an expression reads is declared as,
// or "" when the expression is not a plain path from `inputs` into declared
// structure, or reaches no record. executing is the workflow the inspection is
// held in ("" where the run carries no position): a path is read against these
// declarations only when they are that workflow's.
//
// A path is read, never evaluated: the label is a fact about the declaration,
// so nothing here can run an expression or change what the value shows.
func (s *declaredShapes) recordAt(expression, executing string) string {
	if s == nil || (executing != "" && executing != s.workflow) {
		return ""
	}

	segments, ok := pathSegments(expression)
	if !ok || len(segments) < 2 || segments[0].key != "inputs" {
		return ""
	}

	current, declared := s.inputs[segments[1].key]
	if !declared || segments[1].index {
		return ""
	}
	for _, segment := range segments[2:] {
		current = s.within(current, segment)
		if current == nil {
			return ""
		}
	}

	if name, isRecord := current.GetKind().(*v1.Type_Message); isRecord {
		if _, known := s.table[name.Message]; known {
			return name.Message
		}
	}

	return ""
}

// within is the type of one step into t: a record's field, a map's value, or a
// list's element. Nil where t has no such member.
func (s *declaredShapes) within(t *v1.Type, segment pathSegment) *v1.Type {
	switch kind := t.GetKind().(type) {
	case *v1.Type_Message:
		if segment.index {
			return nil
		}
		for _, field := range s.table[kind.Message].GetFields() {
			if field.GetName() == segment.key {
				return field.DeclaredType()
			}
		}
	case *v1.Type_Map_:
		if segment.index {
			return nil
		}

		return kind.Map.GetValue()
	case *v1.Type_List:
		if segment.index {
			return kind.List
		}
	}

	return nil
}

// pathSegment is one step of a path: a name (`.sku`, `["sku"]`) or a list
// position (`[0]`).
type pathSegment struct {
	key   string
	index bool
}

// pathSegments reads expression as a plain path: a leading name followed by
// `.name`, `["name"]` and `[n]` steps. Anything else, a call, an operator, a
// `.?` presence read, is not a path and reports false.
func pathSegments(expression string) ([]pathSegment, bool) {
	var segments []pathSegment

	rest := expression
	name, rest, ok := leadingName(rest)
	if !ok {
		return nil, false
	}
	segments = append(segments, pathSegment{key: name})

	for rest != "" {
		switch rest[0] {
		case '.':
			name, after, ok := leadingName(rest[1:])
			if !ok {
				return nil, false
			}
			segments = append(segments, pathSegment{key: name})
			rest = after
		case '[':
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return nil, false
			}
			inside := rest[1:end]
			if n, err := strconv.Atoi(inside); err == nil && n >= 0 {
				segments = append(segments, pathSegment{key: inside, index: true})
			} else if key, err := strconv.Unquote(inside); err == nil && strings.HasPrefix(inside, `"`) {
				segments = append(segments, pathSegment{key: key})
			} else {
				return nil, false
			}
			rest = rest[end+1:]
		default:
			return nil, false
		}
	}

	return segments, true
}

// leadingName splits an identifier off the front of s.
func leadingName(s string) (name, rest string, ok bool) {
	end := 0
	for end < len(s) {
		r := s[end]
		if r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || (end > 0 && '0' <= r && r <= '9') {
			end++

			continue
		}

		break
	}
	if end == 0 {
		return "", s, false
	}

	return s[:end], s[end:], true
}
