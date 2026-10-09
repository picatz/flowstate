package main

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A module is a contract in the same shape a workflow's inputs and outputs are,
// one layer down: the files that `use:` it are written against its types, its
// function signatures and its errors, and an edit that removes or tightens one
// breaks them at their next compile, one importer at a time, after the change has
// merged. `flow breaking` closes that gap for modules the way it does for
// workflows, in the module author's own CI.
//
// # Identity and basis
//
// A module is matched to its previous self by repository-relative path, exactly as
// a workflow is, and compared over what it compiles to rather than over its text,
// so reformatting or rewording a description never speaks. What counts as the
// interface is what an importer can write against:
//
//   - a type: removed; changed between record and scalar; for a scalar, a changed
//     base, or a rule that is new or different (undecidable, so read as tightened,
//     the way an input's `must:` is); for a record, any field change that breaks
//     the record in either direction, since the module does not know whether an
//     importer sends the record or receives it
//   - a function: removed; a parameter added or removed; a parameter type
//     narrowed; a result type weakened. A body is behavior, which is not this
//     command's scope, and a renamed parameter changes nothing a call can say
//   - an error: removed
//
// Adding any declaration, loosening a rule, or changing prose passes. A module's
// own declarations only: the ones it carried from another module are that module's
// interface, and its own row answers for them.
//
// # Blast radius
//
// A finding is not only that a declaration broke but who it breaks. The importers
// are found by reading the `use:` blocks of the files the invocation was given,
// from source rather than from what they compile to, because an importer whose
// module just lost a type it uses no longer compiles, and a list of only the
// importers that survived would be the least affected ones. The scope is the paths
// named, as it is for every other row: an importer outside them is not read.

// moduleBreaks reports every way the new module's interface shrank against the old
// one, as the sentence for each and the key its position is under in the new file.
func moduleBreaks(old, neu *v1.Workflow) []moduleBreak {
	var breaks []moduleBreak

	if !v1.IsModule(neu) {
		breaks = append(breaks, moduleBreak{"name", "no longer a module (it has steps, or declares something other than types, functions and errors), so files that `use:` it break; keep it a module, or move the workflow to its own file"})
	}

	// Types.
	newTypes := make(map[string]*v1.TypeDeclaration, len(neu.GetDeclaredTypes()))
	for _, d := range neu.GetDeclaredTypes() {
		newTypes[d.GetName()] = d
	}
	for _, was := range old.GetDeclaredTypes() {
		name := was.GetName()
		if v1.IsCarried(name) {
			continue
		}
		now, kept := newTypes[name]
		if !kept {
			breaks = append(breaks, moduleBreak{"types", fmt.Sprintf(
				"type %q was removed, so files that name it break; keep it, or add the new type beside it", name)})

			continue
		}
		if why := typeBreak(old, neu, was, now); why != "" {
			breaks = append(breaks, moduleBreak{"types." + name, fmt.Sprintf(
				"type %q changed incompatibly (%s), so files that use it break; keep it, or add a new type", name, why)})
		}
	}

	// Functions.
	newFunctions := make(map[string]*v1.FunctionDeclaration, len(neu.GetDeclaredFunctions()))
	for _, d := range neu.GetDeclaredFunctions() {
		newFunctions[d.GetName()] = d
	}
	for _, was := range old.GetDeclaredFunctions() {
		name := was.GetName()
		if v1.IsCarried(name) {
			continue
		}
		now, kept := newFunctions[name]
		if !kept {
			breaks = append(breaks, moduleBreak{"functions", fmt.Sprintf(
				"function %q was removed, so files that call it break; keep it, or add the new function beside it", name)})

			continue
		}
		if why := functionBreak(was, now); why != "" {
			breaks = append(breaks, moduleBreak{"functions." + name, fmt.Sprintf(
				"function %q changed its signature (%s), so files that call it break; keep the signature, or add a new function", name, why)})
		}
	}

	// Errors.
	newErrors := make(map[string]bool, len(neu.GetDeclaredErrors()))
	for _, d := range neu.GetDeclaredErrors() {
		newErrors[d.GetName()] = true
	}
	for _, was := range old.GetDeclaredErrors() {
		if name := was.GetName(); !v1.IsCarried(name) && !newErrors[name] {
			breaks = append(breaks, moduleBreak{"errors", fmt.Sprintf(
				"error %q was removed, so files that raise or match it break; keep it, or add the new error beside it", name)})
		}
	}

	return breaks
}

// A moduleBreak is one finding about a module: the position key in the new file
// it is best reported at, and what to say.
type moduleBreak struct {
	key     string
	message string
}

// typeBreak is why a type changed incompatibly, or "" when it did not.
func typeBreak(old, neu *v1.Workflow, was, now *v1.TypeDeclaration) string {
	switch {
	case was.IsScalar() != now.IsScalar():
		if was.IsScalar() {
			return "a scalar became a record"
		}

		return "a record became a scalar"

	case was.IsScalar():
		var reasons []string
		if was.GetBase() != now.GetBase() {
			reasons = append(reasons, fmt.Sprintf("its base changed from %s to %s", typeName(was.GetBase()), typeName(now.GetBase())))
		}
		if now.GetMust() != "" && now.GetMust() != was.GetMust() {
			// A rule that is gone only loosens; one that is new or different is read
			// as tightened, the way an input's `must:` is.
			reasons = append(reasons, "its rule changed, which is read as tightened")
		}

		return strings.Join(reasons, "; ")
	}

	// A record. The module cannot tell whether an importer sends it or receives it,
	// so it is held to both directions; a reason that holds in both is said once.
	message := &v1.Type{Kind: &v1.Type_Message{Message: was.GetName()}}
	var reasons []string
	for _, output := range []bool{false, true} {
		for _, reason := range recordBreaks(old, neu, message, message, output) {
			if !slices.Contains(reasons, reason) {
				reasons = append(reasons, reason)
			}
		}
	}

	return strings.Join(reasons, "; ")
}

// functionBreak is why a function's signature changed incompatibly, or "" when it
// did not: a call passes arguments by position, so a parameter's name is free to
// change and its count and types are not.
func functionBreak(was, now *v1.FunctionDeclaration) string {
	oldParams, newParams := was.GetParameters(), now.GetParameters()
	if len(oldParams) != len(newParams) {
		return fmt.Sprintf("it took %d parameter%s and now takes %d", len(oldParams), plural(len(oldParams)), len(newParams))
	}

	var reasons []string
	for i := range oldParams {
		if from, to := oldParams[i].GetType(), newParams[i].GetType(); !v1.TypeAssignable(from, to) {
			reasons = append(reasons, fmt.Sprintf("parameter %d (%s) narrowed from %s to %s",
				i+1, oldParams[i].GetName(), v1.TypeString(from), v1.TypeString(to)))
		}
	}
	if from, to := now.GetResult(), was.GetResult(); !v1.TypeAssignable(from, to) {
		reasons = append(reasons, fmt.Sprintf("it returned %s and now returns %s", v1.TypeString(to), v1.TypeString(from)))
	}

	return strings.Join(reasons, "; ")
}

func plural(n int) string {
	if n == 1 {
		return ""
	}

	return "s"
}

// maxImportersListed bounds how many importers a finding names. The count is the
// whole number; the list is for a reader, and a module a whole tree uses would
// otherwise answer with a page.
const maxImportersListed = 8

// An importer is a file that `use:`s a module, and the alias it uses it as.
type importer struct {
	file  string
	alias string
}

// An importerIndex answers "who uses this module" for a whole invocation from one
// pass over the files: each file's `use:` block is read and resolved once, however
// many modules have findings.
type importerIndex struct {
	files []string
	built bool
	byKey map[string][]importer
}

// of returns the files that name the module at modulePath, in the order the files
// were given.
func (x *importerIndex) of(modulePath string) []importer {
	if !x.built {
		x.byKey = importersByModule(x.files)
		x.built = true
	}

	return x.byKey[canonicalFile(modulePath)]
}

// importersByModule reads the `use:` block of each file and groups the files by
// the module each entry names, by the rule a compile resolves a `use:` path by. A
// file that cannot be read is skipped: it is not known to use anything, and
// `validate` owns saying why it is unreadable.
func importersByModule(files []string) map[string][]importer {
	out := map[string][]importer{}
	for _, file := range files {
		data, truncated, err := readFileBounded(file)
		if err != nil || truncated {
			continue
		}
		uses, err := flowfile.ModuleUses(data)
		if err != nil {
			continue
		}
		for _, use := range uses {
			located := flowfile.ResolveCallTarget(file, use.Path)
			if located.Refusal != flowfile.CallTargetResolved {
				continue
			}
			key := canonicalFile(located.Path)
			out[key] = append(out[key], importer{file: file, alias: use.Alias})
		}
	}

	return out
}

// canonicalFile is [canonicalPath] for a file that may no longer exist: the
// directory is resolved when the file cannot be, so a removed module compares equal
// to the path an importer resolves to.
func canonicalFile(path string) string {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real
	}
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	if dir, err := filepath.EvalSymlinks(filepath.Dir(path)); err == nil {
		return filepath.Join(dir, filepath.Base(path))
	}

	return path
}

// blastRadius is the sentence that says who a module's findings reach.
func blastRadius(importers []importer) string {
	if len(importers) == 0 {
		return "no file among the paths given uses this module"
	}

	listed := make([]string, 0, maxImportersListed)
	for _, i := range importers[:min(len(importers), maxImportersListed)] {
		listed = append(listed, fmt.Sprintf("%s (as %s)", i.file, i.alias))
	}
	more := ""
	if extra := len(importers) - len(listed); extra > 0 {
		more = fmt.Sprintf(", and %d more", extra)
	}

	verb := "use"
	if len(importers) == 1 {
		verb = "uses"
	}

	return fmt.Sprintf("%d file%s %s this module: %s%s", len(importers), plural(len(importers)), verb, strings.Join(listed, ", "), more)
}
