package flowfile

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A call reads another Flowfile at compile time, on whichever client is
// compiling this one — an editor, `flow validate`, `flow run` — never on a
// worker. [v1.Call]'s doc says why: a run's spec is frozen at submit and
// carried whole across every Continue-As-New, so resolving a call by name at
// execution time would let a workload mean one thing in its first segment and
// another in its last, and it keeps filesystem access out of the worker
// entirely.
//
// Which is why this package, the one place that already reads an author's
// files, is where a call is resolved — with the same three concerns as
// anything else in this package that reads input an outside party chose: the
// path is attacker-shaped (refused rather than sanitised), a cycle across
// files has to be caught the way an anchor cycle already is, and the total
// compiled size has to be bounded by breadth rather than by depth, because a
// diamond of calls multiplies breadth exactly as a repeated YAML alias does.

// maxCallExpansionNodes bounds the total compiled node count across every
// callee resolved while compiling one file's whole call tree.
//
// Shared across the tree rather than counted per file, because a diamond is
// the shape that defeats a per-file limit: A calling B twice, each of which
// calls C twice, embeds four whole copies of C's compiled steps, and nothing
// here deduplicates a callee compiled more than once — see [v1.Call]'s doc on
// why a call carries a whole specification rather than a reference. So the
// bound has to be on the running total, checked as the tree is built, the same
// way [maxNodes] bounds one file's own alias expansion.
const maxCallExpansionNodes = 100_000

// call compiles a `call:` step: resolves the callee relative to this file's
// own directory, compiles it, and checks `with:` against what it declares.
//
// stepPath is the step's own path, for addressing `with:` and its entries;
// kindPath addresses the `call:` value itself. withField is the step's `with:`
// field, if it wrote one.
func (c *compiler) call(pathNode ast.Node, stepPath, kindPath string, r ref, withField field, hasWith bool) *v1.Call {
	callRef := ref{step: r.step, path: kindPath, label: "call"}

	target, ok := c.text(pathNode, kindPath, callRef)
	if !ok {
		return nil
	}

	if c.filePath == "" {
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, but this file was compiled with no location of its own to resolve a "+
				"relative path against; compile it as a file rather than from bytes alone — "+
				"`flow validate`, `flow run` and the language server all do", target)
		return nil
	}

	if filepath.IsAbs(target) {
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, an absolute path; a call is resolved relative to the file that calls "+
				"it and may not name an absolute one", target)
		return nil
	}

	clean := filepath.ToSlash(filepath.Clean(target))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, which climbs above the directory of the file that calls it; a call may "+
				"reach anything at or below its own file's directory and nothing above it", target)
		return nil
	}

	callerDir := filepath.Dir(c.filePath)
	resolved := filepath.Clean(filepath.Join(callerDir, target))

	// The lexical check above refuses `../` climbing out of callerDir in the
	// path as *written*, but a path that stays lexically inside it can still
	// land outside on disk: an in-directory symlink pointing elsewhere follows
	// right through that check, the same class of hole as the git plugin's
	// symlink-through-entry. So containment is checked again here, against
	// where the path actually resolves to once every symlink in it — in
	// callerDir and in the target — is followed, and refused on the real
	// location rather than the written one.
	//
	// EvalSymlinks requires the path to exist, so a target that is merely
	// missing (no symlink involved at all) falls through to the read below,
	// which reports that in the ordinary way; what is refused here is
	// specifically a real path outside callerDir, not a path that cannot be
	// resolved at all.
	if realCallerDir, err := filepath.EvalSymlinks(callerDir); err == nil {
		if realResolved, err := filepath.EvalSymlinks(resolved); err == nil {
			rel, relErr := filepath.Rel(realCallerDir, realResolved)
			escapes := relErr != nil || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(filepath.ToSlash(rel), "../")
			if escapes {
				c.report(spanOfNode(pathNode), callRef,
					"calls %q, which resolves — through a symlink — to %q, outside %q; a call may "+
						"reach anything at or below its own file's directory and nothing above it, "+
						"and a symlink does not change what \"at or below\" means", target, realResolved, realCallerDir)
				return nil
			}

			// Read via the fully-resolved real path from here on, not the
			// symlink path just verified: re-deriving it from `resolved` after
			// the check above would leave a window between the check and the
			// read for the symlink to be repointed in.
			resolved = realResolved
		}
	}

	// The chain of files compiling this one, including this file itself — so
	// that a direct self-call (A calls A) is caught by the same walk as a
	// longer cycle (A calls B calls A), and so the callee inherits the whole
	// chain rather than starting a new one.
	//
	// Canonicalized the same way `resolved` is, best-effort: two different
	// symlinks aliasing the same real file must compare equal here, or a cycle
	// walked through one alias and back through another would read as two
	// distinct files rather than the self-reference it is.
	self := c.filePath
	if real, err := filepath.EvalSymlinks(c.filePath); err == nil {
		self = real
	}
	ancestors := make([]string, 0, len(c.callStack)+1)
	ancestors = append(ancestors, c.callStack...)
	ancestors = append(ancestors, self)

	if err := v1.CheckCallDepth(len(ancestors)); err != nil {
		c.report(spanOfNode(pathNode), callRef, "%s", err.Error())
		return nil
	}

	for _, prior := range ancestors {
		if prior == resolved {
			chain := append(append([]string{}, ancestors...), resolved)
			c.report(spanOfNode(pathNode), callRef,
				"calls itself through a chain of files rather than directly, which the parser "+
					"catches the same way it catches an anchor referring to its own value: %s",
				strings.Join(chain, " -> "))
			return nil
		}
	}

	data, err := os.ReadFile(resolved)
	if err != nil {
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, which could not be read: %s", target, err.Error())
		return nil
	}

	callee, _, err := parse(data, resolved, ancestors, c.callBudget)
	if err != nil {
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, which failed to compile:\n%s", target, indentLines(err.Error()))
		return nil
	}

	// Taken from the same bytes the compile above read, at the same moment —
	// never recomputed later and never checked against anything, exactly like
	// Source itself. It is a record of which bytes actually produced this
	// embedded specification, for whoever audits a run afterward, not an
	// instruction anything here or later acts on.
	digest := sha256.Sum256(data)
	sourceDigest := "sha256:" + hex.EncodeToString(digest[:])

	// Enforced immediately after this callee is known, rather than only once at
	// the end of the whole tree — an early call in a wide tree must not let a
	// later one build unboundedly on the strength of budget the first has not
	// yet spent.
	*c.callBudget += countCompiledNodes(callee.GetSteps())
	if *c.callBudget > maxCallExpansionNodes {
		c.report(spanOfNode(pathNode), callRef,
			"calling %q brings the total compiled across every call in this file to more than "+
				"%d steps, which is more than a Flowfile's calls are meant to expand to; a diamond "+
				"of calls multiplies breadth the way a repeated YAML alias does, and this is that "+
				"same bound applied to it", target, maxCallExpansionNodes)
		return nil
	}

	call := &v1.Call{Workflow: callee, Source: target, SourceDigest: sourceDigest}

	args := map[string]*v1.Value{}
	if hasWith {
		withPath := fieldPath(stepPath, "with")
		c.pos.record(withPath, spanOfNode(c.resolveQuiet(withField.value)))
		args = c.callArguments(withField.value, withPath, ref{step: r.step, path: withPath, label: "with"})
	}
	call.Arguments = args

	declared := make(map[string]*v1.InputDeclaration, len(callee.GetDeclaredInputs()))
	for _, d := range callee.GetDeclaredInputs() {
		declared[d.GetName()] = d
	}

	// Bound to what the callee declares: an argument for a name it does not
	// take is almost always a rename in one file and not the other, and reading
	// it as "extra data nobody minds" would let that go unnoticed until whoever
	// reads the callee wonders why its own `${inputs.foo}` is never bound.
	for name := range args {
		if _, ok := declared[name]; !ok {
			withEntryPath := fieldPath(fieldPath(stepPath, "with"), name)
			c.report(spanOfNode(withField.value), ref{step: r.step, path: withEntryPath, label: "with"},
				"binds %q, which workflow %q declares no input named; the inputs it takes are %s",
				name, callee.GetName(), declaredInputNameList(callee))
		}
	}

	// The other direction: a required input with no default that `with:` never
	// bound. Checked here, at compile time, rather than left to the run-time
	// check `v1.BindRunInputs` also makes, because an editor is where an author
	// can act on it.
	for _, d := range callee.GetDeclaredInputs() {
		if !d.GetRequired() || d.GetDefault() != nil {
			continue
		}
		if _, ok := args[d.GetName()]; !ok {
			c.report(spanOfNode(pathNode), callRef,
				"workflow %q requires input %q, which `with:` does not bind", callee.GetName(), d.GetName())
		}
	}

	return call
}

// callArguments compiles a call's `with:` mapping: one value per bound
// argument, each checked through [compiler.callArgumentValue] rather than
// [compiler.vars]'s — the same shape, and a stricter rule about what may fill
// it. See [notAcrossCallHelp].
func (c *compiler) callArguments(n ast.Node, path string, r ref) map[string]*v1.Value {
	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	compiled := make(map[string]*v1.Value, len(entries))
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		if value := c.callArgumentValue(e.value, valuePath, ref{step: r.step, path: valuePath, label: "with." + e.name}); value != nil {
			compiled[e.name] = value
		}
	}

	if len(compiled) == 0 {
		return nil
	}
	return compiled
}

// declaredInputNameList renders a workflow's declared input names for a
// diagnostic, in the order they were declared.
func declaredInputNameList(wf *v1.Workflow) string {
	if len(wf.GetDeclaredInputs()) == 0 {
		return "none"
	}
	names := make([]string, 0, len(wf.GetDeclaredInputs()))
	for _, d := range wf.GetDeclaredInputs() {
		names = append(names, d.GetName())
	}
	return strings.Join(names, ", ")
}

// countCompiledNodes counts a compiled node list's own contribution to the
// call-expansion budget: itself and every node nested in a for_each body or a
// parallel branch.
//
// A call is deliberately not descended into. The callee it embeds already
// added its own count to the shared budget when it was compiled — by this same
// function, one level down — so descending here would count it twice. What
// this totals, across every file in a call tree, is therefore the size of the
// whole expansion exactly once.
func countCompiledNodes(nodes []*v1.Node) int {
	n := len(nodes)
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			n += countCompiledNodes(kind.ForEach.GetBody())
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				n += countCompiledNodes(branch.GetSteps())
			}
		}
	}
	return n
}

// indentLines prefixes every line of s with two spaces, so a callee's own
// diagnostics read as a block nested under the call that reached them rather
// than running into the sentence that introduces them.
func indentLines(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = "  " + line
	}
	return strings.Join(lines, "\n")
}
