package main

import (
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// maxRecordBreaks bounds how many ways one declaration is reported to have
// broken through its records. Each is a real finding, but a rewrite of a large
// record is one change to the author and a hundred lines to the reader.
const maxRecordBreaks = 8

// recordBreaks reports how the structure behind a declared type shrank between
// two workflows, for the records `v1.TypeAssignable` treats as equal because
// they share a name. A record's name says nothing about its fields, so a field
// made required, dropped, or narrowed inside `Order` is invisible to a
// comparison of the two declarations that both say `Order`.
//
// The direction is the declaration's. An input is what a caller sends, so a
// record breaks when it now refuses something it accepted: a field a caller
// must now supply, a field removed (a record is closed, so a caller still
// sending it is refused), a field whose type or constraints narrowed, a tighter
// `must:`. An output is what a caller receives, so a record breaks when it now
// promises less: a field removed, no longer required, weakened in type, or
// stripped of its `must:`. A field added to an output only promises more.
//
// Each record is compared once, where it is first reached, and the walk is
// bounded by [v1.MaxStructureDepth] and [maxRecordBreaks].
func recordBreaks(old, neu *v1.Workflow, oldType, newType *v1.Type, output bool) []string {
	d := &recordDiff{
		old:    v1.TypesOf(old),
		neu:    v1.TypesOf(neu),
		output: output,
		seen:   map[string]struct{}{},
	}
	d.types("", oldType, newType, 0)

	return d.reasons
}

type recordDiff struct {
	old, neu v1.TypeTable
	output   bool
	seen     map[string]struct{}
	reasons  []string
}

func (d *recordDiff) full() bool { return len(d.reasons) >= maxRecordBreaks }

func (d *recordDiff) add(path, format string, args ...any) {
	if d.full() {
		return
	}
	d.reasons = append(d.reasons, path+": "+fmt.Sprintf(format, args...))
}

// types follows a pair of types to the records inside them. Pairs that differ in
// shape are left to the comparison that already reports a changed type.
func (d *recordDiff) types(path string, o, n *v1.Type, depth int) {
	if depth > v1.MaxStructureDepth || d.full() {
		return
	}

	switch ok := o.GetKind().(type) {
	case *v1.Type_Message:
		if nk, same := n.GetKind().(*v1.Type_Message); same && nk.Message == ok.Message {
			d.record(ok.Message, path, depth)
		}
	case *v1.Type_List:
		if nk, same := n.GetKind().(*v1.Type_List); same {
			d.types(path+"[]", ok.List, nk.List, depth+1)
		}
	case *v1.Type_Map_:
		if nk, same := n.GetKind().(*v1.Type_Map_); same {
			d.types(path+"[]", ok.Map.GetValue(), nk.Map.GetValue(), depth+1)
		}
	}
}

func (d *recordDiff) record(name, path string, depth int) {
	if _, done := d.seen[name]; done {
		return
	}
	d.seen[name] = struct{}{}

	oldRecord, neuRecord := d.old[name], d.neu[name]
	if oldRecord == nil || neuRecord == nil {
		return
	}

	at := "record " + name
	if path != "" {
		at = path + " (record " + name + ")"
	}

	if d.output {
		if was := oldRecord.GetMust(); was != "" && was != neuRecord.GetMust() {
			d.add(at, "its `must:` was removed or changed")
		}
	} else if now := neuRecord.GetMust(); now != "" && now != oldRecord.GetMust() {
		d.add(at, "its `must:` tightened")
	}

	oldFields := fieldsByName(oldRecord)
	neuFields := fieldsByName(neuRecord)

	for _, field := range neuRecord.GetFields() {
		fieldPath := at + " field " + field.GetName()
		was, existed := oldFields[field.GetName()]
		if !existed {
			if !d.output && mustSupply(field) {
				d.add(at, "field %q is new and must be supplied", field.GetName())
			}

			continue
		}

		d.field(fieldPath, was, field, depth)
	}

	for _, field := range oldRecord.GetFields() {
		if _, kept := neuFields[field.GetName()]; !kept {
			if d.output {
				d.add(at, "field %q was removed", field.GetName())
			} else {
				d.add(at, "field %q was removed, so a caller still sending it is refused", field.GetName())
			}
		}
	}
}

func (d *recordDiff) field(at string, was, now *v1.InputDeclaration, depth int) {
	oldType, newType := was.DeclaredType(), now.DeclaredType()

	if d.output {
		switch {
		case was.GetRequired() && !now.GetRequired():
			d.add(at, "is no longer required")
		case !v1.TypeAssignable(newType, oldType):
			d.add(at, "weakened its type from %s to %s", v1.TypeString(oldType), v1.TypeString(newType))
		case was.GetMust() != "" && was.GetMust() != now.GetMust():
			d.add(at, "its `must:` was removed or changed")
		}
	} else {
		switch {
		case mustSupply(now) && !mustSupply(was):
			d.add(at, "now must be supplied")
		case !v1.TypeAssignable(oldType, newType):
			d.add(at, "narrowed its type from %s to %s", v1.TypeString(oldType), v1.TypeString(newType))
		default:
			if why := constraintNarrowed(was, now); why != "" {
				d.add(at, "narrowed its constraint (%s)", why)
			}
		}
	}

	d.types(at, oldType, newType, depth+1)
}

func fieldsByName(record *v1.TypeDeclaration) map[string]*v1.InputDeclaration {
	fields := make(map[string]*v1.InputDeclaration, len(record.GetFields()))
	for _, field := range record.GetFields() {
		fields[field.GetName()] = field
	}

	return fields
}
