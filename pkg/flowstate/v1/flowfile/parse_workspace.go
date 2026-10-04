package flowfile

import (
	"regexp"

	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// This file compiles the two step keys that give a task files to work in and
// a way to keep what it built: `workspace:` and `produce:`.
//
// Both are properties of a step doing a task's work, in the sense `undo:` is, so
// both are refused on every other kind of step; the pair is not a kind of work
// of its own.

// notInWorkspaceHelp is the refusal for a secret reference in `workspace:`,
// read the way [notInVarHelp] is: the entry is evaluated by the workflow, and
// what it names is a tree of files, which a credential is not.
const notInWorkspaceHelp = "a secret reference cannot be part of `workspace:`; an entry names an artifact, " +
	"which is a digest the workflow evaluates and records, and nothing that evaluates it resolves secrets. " +
	"Pass a secret to the task input that consumes it instead"

// producedName is the shape of an artifact name, from the schema's own pattern
// on Task.produce so the compiler and the validator agree about what one is.
var producedName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

// workspaceAndProduce compiles a step's `workspace:` and `produce:` onto its
// task. fields holds the keys the step wrote, and task is nil when the step
// does not run one.
func (c *compiler) workspaceAndProduce(fields *fieldSet, task *v1.Task, path string, r ref) {
	for _, key := range []string{"workspace", "produce"} {
		f, found := fields.get(key)
		if !found {
			continue
		}
		keyPath := fieldPath(path, key)
		c.pos.record(keyPath, spanOfNode(f.key))

		if task == nil {
			c.report(spanOfNode(f.key), r,
				"`%s:` gives a task files to work in, and is only meaningful on a step that runs a task, which this step does not",
				key)
			continue
		}

		switch key {
		case "workspace":
			task.Workspace = c.workspace(f.value, keyPath, r)
		case "produce":
			task.Produce = c.produce(f.value, keyPath, r)
		}
	}
}

// workspace compiles a `workspace:` mapping: a directory of the step's
// workspace on the left, the artifact that fills it on the right.
//
//	workspace:
//	  src: ${steps.checkout.artifacts.tree}
//
// A value is an expression evaluating to an artifact, which is the only thing a
// directory can be filled from; a bare string, a number or a list is refused
// here rather than at the worker, since every one of them is knowable now.
func (c *compiler) workspace(n ast.Node, path string, r ref) map[string]*v1.Value {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	mounts := make([]string, 0, len(entries))
	compiled := make(map[string]*v1.Value, len(entries))
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		valueRef := ref{step: r.step, path: valuePath, label: "workspace." + e.name}

		if err := v1.CheckWorkspacePath(e.name); err != nil {
			c.report(spanOfNode(e.key), valueRef, "%s", err)
			continue
		}
		mounts = append(mounts, e.name)

		if resolved := c.resolveQuiet(e.value); resolved != nil && c.holdsSecretMarker(resolved) {
			c.report(c.secretMarkerSpan(resolved), valueRef, "%s", notInWorkspaceHelp)
			continue
		}

		value := c.inputValue(e.value, valuePath, valueRef)
		if value == nil {
			continue
		}
		if _, isExpr := value.GetKind().(*v1.Value_Expr); !isExpr {
			c.report(spanOfNode(e.value), valueRef,
				"must be an artifact, written as an expression such as ${steps.checkout.artifacts.tree}; "+
					"an artifact is made by a step's `produce:`, and a literal cannot name one")
			continue
		}
		compiled[e.name] = value
	}

	if err := v1.CheckWorkspaceMounts(mounts); err != nil {
		c.report(spanOfNode(n), r, "%s", err)
		return nil
	}
	if len(compiled) == 0 {
		return nil
	}

	return compiled
}

// produce compiles a `produce:` mapping: the name an artifact is read by on the
// left, the directory of the workspace it is a snapshot of on the right.
//
//	produce:
//	  binary: out
//
// Both sides are read when the file compiles. A path is text and never an
// expression: what a step snapshots is a fact about the file, so it can be
// checked, and a computed path would make "what does this step leave behind"
// unanswerable without running it.
func (c *compiler) produce(n ast.Node, path string, r ref) map[string]string {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	compiled := make(map[string]string, len(entries))
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		valueRef := ref{step: r.step, path: valuePath, label: "produce." + e.name}

		if !producedName.MatchString(e.name) || len(e.name) > 64 {
			c.report(spanOfNode(e.key), valueRef,
				"%q is not a name an artifact can be read by; use a letter followed by letters, digits and underscores (at most 64), "+
					"so that ${steps.<id>.artifacts.%s} is an expression", e.name, "<name>")
			continue
		}

		dir, ok := c.text(e.value, valuePath, valueRef)
		if !ok {
			continue
		}
		if err := v1.CheckWorkspacePath(dir); err != nil {
			c.report(spanOfNode(e.value), valueRef, "%s", err)
			continue
		}
		compiled[e.name] = dir
	}

	if len(compiled) > v1.MaxProducedArtifacts {
		c.report(spanOfNode(n), r, "produce: %d entries, over the limit of %d", len(compiled), v1.MaxProducedArtifacts)
		return nil
	}
	if len(compiled) == 0 {
		return nil
	}

	return compiled
}
