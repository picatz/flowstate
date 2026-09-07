package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow breaking` is `buf breaking` one level up. The repo runs `buf breaking`
// against origin/main because the proto schema is a public contract: plugins
// compile against the descriptors, so a break is every plugin in the wild, not a
// compile error somebody sees. A workflow's declared inputs and outputs are a
// contract in exactly the same shape, one layer higher. Composition is vendoring
// (#172): a callee is resolved at compile time and embedded whole, so an
// interface break never strikes a running spec, it strikes the callee's author's
// next compile, one caller at a time, after the change already merged. This
// closes that gap where `buf breaking` closes it for the proto, in the callee
// author's own CI, at the moment of the change.
//
// The comparison basis is the compiled protos (the DeclaredInputs and
// DeclaredOutputs off v1.Workflow), never the YAML text, so the check is immune
// to formatting and comment churn by construction (principle 6: the proto is the
// contract, the file is a projection of it).
//
// A break is a property of the file at HEAD measured against a ref, which the
// author's own CI can see, so it is a legitimate diagnostic in the sense
// CLAUDE.md draws the line: it reports what is a property of the file, not what a
// deployment decides.
//
// # Scope
//
// This checks interface compatibility, not behavior. A callee that keeps its
// shape and changes its meaning passes, as in every schema-compatibility system.
// Behavior is what `*.test.yaml` and the `digest:` pin govern.
//
// # Identity
//
// A workflow is matched to its previous self by its repository-relative path,
// never by `name:`. `call:` addresses a callee by path and nothing in the
// language requires names to be unique beyond one file, so in a tree with more
// than one team `notify`, `deploy` and `cleanup` are each declared several times
// and are each several workflows (#1712). Path is the one identity that exists
// until #106 and #1447 decide a namespace, and it is unambiguous: two files at
// one path across revisions cannot occur. A file that moved is the one case a
// path cannot follow, and `--moved old=new` says where it went.

// newBreakingCommand builds the `flow breaking` command.
func newBreakingCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "breaking [path...]",
		Short: "Report workflows whose declared inputs or outputs broke a contract",
		Long: "Compile every Flowfile at the working tree and at a git ref, match each workflow to " +
			"its previous self by path, and report interface breaks: a declared input that a caller must now supply, " +
			"an input whose type narrowed, an input removed, a declared output removed or renamed, " +
			"a declared output whose type or guarantee weakened, or a constraint tightened. " +
			"Loosening a contract passes, mirroring `buf breaking`: a contract may grow, not " +
			"shrink.\n\n" +
			"The comparison is over the compiled protos, not the YAML text, so it is immune to " +
			"formatting and comment churn. Each finding names the position in the working-tree file, " +
			"what broke, and what to do instead. Exit is 1 on any finding, 0 on none, the same as " +
			"`validate`.\n\n" +
			"A named file is taken as given; a directory is walked for Flowfiles, the same walk " +
			"`validate` and `test` use. The `--against` ref must be present in the local git " +
			"history: fetch the base branch first, exactly as the `buf breaking` check does.\n\n" +
			"A workflow is its path: two files declaring one `name:` in different directories are " +
			"two workflows, each compared against the file at its own path at the ref. A file that " +
			"moved since the ref is matched with `--moved old=new`; without it the old path reads " +
			"as removed and the new one as brand new.",
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBreaking(cmd, args)
		},
		Example: `# Check every example against the base branch.
# Fetch it first, the same way the buf breaking check does: git fetch origin main
flow breaking --against origin/main examples/

# Check one workflow against the last commit:
flow breaking --against HEAD~1 examples/hello-world/workflow.yaml

# A file that moved is compared against its old path, not reported as removed:
flow breaking --against origin/main --moved shared/notify.yaml=workflows/notify.yaml .`,
	}

	cmd.Flags().String("against", "",
		"git ref holding the old contract to compare the working tree against, such as origin/main")
	_ = cmd.MarkFlagRequired("against")
	cmd.Flags().StringArray("moved", nil,
		"a Flowfile that moved since the ref, as old=new paths, so it is compared against its old self (repeatable)")

	return cmd
}

// errBreakingFound reports that at least one interface break was found. It
// carries no message of its own because the findings have already been printed.
var errBreakingFound = errors.New("breaking changes found")

// runBreaking compiles every Flowfile under the paths given at the working tree
// and at the ref, matches by name, and reports every interface break.
func runBreaking(cmd *cobra.Command, paths []string) error {
	ref, err := cmd.Flags().GetString("against")
	if err != nil {
		return err
	}
	movedArgs, err := cmd.Flags().GetStringArray("moved")
	if err != nil {
		return err
	}

	root, err := gitToplevel()
	if err != nil {
		return err
	}
	if err := gitHasRef(root, ref); err != nil {
		return err
	}
	moves, err := parseMoves(root, movedArgs)
	if err != nil {
		return err
	}

	// The working-tree (HEAD) side: the same walk validate and test use.
	files, err := collectFlowfiles(paths)
	if err != nil {
		return err
	}

	newByPath := compileHead(root, files)
	oldByPath := compileRef(root, ref, paths, files)
	if err := applyMoves(ref, moves, oldByPath, newByPath); err != nil {
		return err
	}

	surface := newSurface(cmd)
	out, theme := surface.Out, surface.Theme

	var failed bool
	for _, path := range sortedPaths(newByPath, oldByPath) {
		old, oldOK := oldByPath[path]
		neu, newOK := newByPath[path]

		switch {
		case oldOK && newOK:
			for _, d := range breakingDiagnostics(old.wf, neu.wf, neu.pos) {
				failed = true
				printBreak(out, theme, neu.path, d)
			}
		case oldOK && !newOK:
			// Present at the ref, gone at the working tree. A removed workflow
			// breaks every caller, which can no longer resolve the `call:`. That
			// is also caught by `validate` at the caller, but it is reported here
			// too, at the callee, which is the author this command exists to reach.
			// There is no working-tree file to position it in, so it names the ref
			// path instead. A file that moved looks exactly like this from the old
			// path, so the sentence says how to say so.
			failed = true
			printBreak(out, theme, old.path, flowfile.Diagnostic{
				Field: "name",
				Message: fmt.Sprintf(
					"workflow %q was removed; callers that `call:` it break at their next compile. Keep it, or rename callers off it in the same change; if it moved, compare it against its old self with `--moved %s=<new path>`",
					old.wf.GetName(), path),
			})
		}
		// newOK && !oldOK is a brand-new workflow: no contract to break.
	}

	if failed {
		return errBreakingFound
	}
	return nil
}

// move is one `--moved old=new`, both sides repository-relative.
type move struct {
	old, neu string
}

// parseMoves reads each `--moved old=new` into repository-relative paths. Either
// side may be given relative to the working directory, the way every other path
// argument is, or already repository-relative; both resolve through the same
// rule the path arguments use.
func parseMoves(root string, args []string) ([]move, error) {
	moves := make([]move, 0, len(args))
	for _, arg := range args {
		oldPath, newPath, ok := strings.Cut(arg, "=")
		if !ok || oldPath == "" || newPath == "" {
			return nil, fmt.Errorf("--moved %q: want old=new, the path at the ref and the path in the working tree", arg)
		}
		oldRel, ok := repoRel(root, oldPath)
		if !ok {
			return nil, fmt.Errorf("--moved %q: %s is outside the repository", arg, oldPath)
		}
		newRel, ok := repoRel(root, newPath)
		if !ok {
			return nil, fmt.Errorf("--moved %q: %s is outside the repository", arg, newPath)
		}
		moves = append(moves, move{old: oldRel, neu: newRel})
	}
	return moves, nil
}

// applyMoves rekeys each moved workflow's ref-side entry under its new path, so
// it is compared against the working-tree file there. A move whose old side is
// not at the ref, whose new side is not in the working tree, or whose new path
// already exists at the ref (so the move would silently replace a workflow that
// is still there) is refused rather than guessed.
func applyMoves(ref string, moves []move, oldByPath, newByPath map[string]compiled) error {
	for _, m := range moves {
		old, ok := oldByPath[m.old]
		if !ok {
			return fmt.Errorf("--moved %s=%s: %s is not a Flowfile at %s under the paths given", m.old, m.neu, m.old, ref)
		}
		if _, ok := newByPath[m.neu]; !ok {
			return fmt.Errorf("--moved %s=%s: %s is not a Flowfile in the working tree under the paths given", m.old, m.neu, m.neu)
		}
		if _, ok := oldByPath[m.neu]; ok {
			return fmt.Errorf("--moved %s=%s: %s is also a Flowfile at %s, so the move would replace the workflow still there", m.old, m.neu, m.neu, ref)
		}
		delete(oldByPath, m.old)
		oldByPath[m.neu] = old
	}
	return nil
}

// compiled is one workflow compiled from one file, with the positions its
// diagnostics point into.
type compiled struct {
	wf   *v1.Workflow
	pos  *flowfile.Positions
	path string
}

// compileHead compiles every working-tree file, keyed by repository-relative
// path.
//
// A file that does not compile is skipped rather than reported: `validate` owns
// that diagnostic, and a file that will not compile has no contract to compare.
// A file outside the repository has no path at the ref to compare against and is
// skipped for the same reason.
func compileHead(root string, files []string) map[string]compiled {
	byPath := make(map[string]compiled, len(files))
	for _, path := range files {
		rel, ok := repoRel(root, path)
		if !ok {
			continue
		}
		wf, pos, err := flowfile.ParseFile(path)
		if err != nil {
			continue
		}
		byPath[rel] = compiled{wf: wf, pos: pos, path: path}
	}
	return byPath
}

// compileRef compiles the ref-side version of every Flowfile under the paths,
// keyed by repository-relative path.
//
// It reads the ref's bytes through `git show` and compiles them as if they sat
// at the same path, so a `call:` resolves against the working tree's callees.
// That is the right basis for slice 1: this command asks whether a workflow's
// own declared inputs and outputs shrank, and a callee's contract is a separate
// question the callee's own row answers.
//
// The file set is the union of the working-tree files and every Flowfile tracked
// at the ref under the same paths, so a workflow deleted between the ref and the
// working tree is still seen on the ref side.
func compileRef(root, ref string, paths, headFiles []string) map[string]compiled {
	relSet := make(map[string]struct{})
	for _, path := range headFiles {
		if rel, ok := repoRel(root, path); ok {
			relSet[rel] = struct{}{}
		}
	}
	for _, rel := range gitListYAML(root, ref, repoRelPaths(root, paths)) {
		relSet[rel] = struct{}{}
	}

	byPath := make(map[string]compiled, len(relSet))
	for rel := range relSet {
		data, err := gitShow(root, ref, rel)
		if err != nil {
			continue // Absent at the ref: a new file, nothing to compare.
		}
		abs := filepath.Join(root, rel)
		wf, pos, err := flowfile.ParseAt(data, abs)
		if err != nil {
			continue
		}
		byPath[rel] = compiled{wf: wf, pos: pos, path: rel}
	}
	return byPath
}

// printBreak renders one finding in the same shape `validate` renders a
// diagnostic: the file, then the positioned message.
func printBreak(out io.Writer, theme ui.Theme, path string, d flowfile.Diagnostic) {
	fmt.Fprintln(out, diagnosticLine(theme.Muted.Render(path), d))
}

// sortedPaths is the union of the two maps' keys, in a stable order so a run's
// output does not depend on map iteration.
func sortedPaths(a, b map[string]compiled) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for path := range a {
		seen[path] = struct{}{}
	}
	for path := range b {
		seen[path] = struct{}{}
	}
	return slices.Sorted(maps.Keys(seen))
}

// --- comparison: the old (ref) contract against the new (working-tree) one ---

// breakingDiagnostics reports every way the new workflow's declared interface
// shrank against the old one. The rule mirrors `buf breaking`: a contract may
// grow, not shrink, so loosening (an added optional input, a widened bound, a
// dropped constraint, an added output) is silent and only tightening speaks.
//
// Positions come from the new file's Positions, because a break is a property of
// the file at HEAD and the author reading it is looking at HEAD. A removal has no
// position in the new file (the declaration is gone), so it is reported against
// the `inputs:` or `outputs:` field with no line.
func breakingDiagnostics(old, neu *v1.Workflow, pos *flowfile.Positions) flowfile.Diagnostics {
	var ds flowfile.Diagnostics

	oldInputs := inputsByName(old)
	newInputs := inputsByName(neu)

	// Inputs.
	for name, ni := range newInputs {
		oi := oldInputs[name] // nil when the input is new.

		// An input a caller must now supply, that they did not have to before
		// (or that did not exist before). "Must supply" is required with no
		// default: a required input with a default is filled for a caller who
		// passes nothing, so it does not break them. This one class covers an
		// added required input, an optional input turned required, and a default
		// removed while required stays true.
		if mustSupply(ni) && (oi == nil || !mustSupply(oi)) {
			ds = append(ds, diagAt(pos, "inputs."+name, flowfile.Diagnostic{
				Field: "inputs." + name, Value: name,
				Message: fmt.Sprintf(
					"input %q now must be supplied, so callers that do not pass it break; make it optional, or give it a `default:`", name),
			}))
			continue
		}

		if oi == nil {
			continue // A new optional input loosens the contract.
		}

		// Type changed: a caller passing the old type is now refused.
		if ni.GetType() != oi.GetType() {
			ds = append(ds, diagAt(pos, "inputs."+name+".type", flowfile.Diagnostic{
				Field: "inputs." + name, Value: name,
				Message: fmt.Sprintf(
					"input %q changed type from %s to %s, so callers passing the old type break; keep the type, or add a new input",
					name, typeName(oi.GetType()), typeName(ni.GetType())),
			}))
			continue
		}

		// Constraint narrowed: a value the old contract accepted is now refused.
		if why := constraintNarrowed(oi, ni); why != "" {
			ds = append(ds, diagAt(pos, "inputs."+name, flowfile.Diagnostic{
				Field: "inputs." + name, Value: name,
				Message: fmt.Sprintf(
					"input %q narrowed its constraint (%s), so values the old contract accepted now break; widen it back, or add a new input",
					name, why),
			}))
		}
	}

	// An input removed: a caller passing it breaks, because an unknown `with:`
	// key is refused (flowfile/validate_call.go: "declares no input named").
	for name := range oldInputs {
		if _, ok := newInputs[name]; !ok {
			ds = append(ds, flowfile.Diagnostic{
				Field: "inputs",
				Message: fmt.Sprintf(
					"input %q was removed, so callers passing it via `with:` break; keep it, or accept and ignore it", name),
			})
		}
	}

	// An output removed or renamed: a caller reading it breaks. A rename is a
	// removal of the old name (plus a silent addition of the new one), so it is
	// caught here by the old name's absence. An output kept but weakened breaks
	// callers who relied on the guarantee it dropped.
	newOutputs := outputsByName(neu)
	for _, oo := range old.GetDeclaredOutputs() {
		name := oo.GetName()
		no, ok := newOutputs[name]
		if !ok {
			ds = append(ds, flowfile.Diagnostic{
				Field: "outputs",
				Message: fmt.Sprintf(
					"output %q was removed or renamed, so callers reading it break; keep the name, or add the new one alongside it", name),
			})
			continue
		}

		// A declared output type is the other half of that same guarantee, and
		// it is read in the *opposite* direction from an input's: an input's
		// type constrains what a caller may send, so tightening it breaks them,
		// while an output's type promises what a caller will receive, so it is
		// weakening that breaks them. Dropping the type (nothing is promised any
		// more) and changing it (something else is promised) are both that;
		// adding one where there was none only promises more, so it is silent,
		// exactly as an added `must:` above is. There is no subtyping in this
		// vocabulary — the six data types are disjoint and `enum` is a string
		// with a set — so "changed" is the whole of the decidable question and
		// no direction of change is safe.
		if oldType := oo.GetType(); oldType != v1.InputDeclaration_TYPE_UNSPECIFIED && oldType != no.GetType() {
			ds = append(ds, diagAt(pos, "outputs."+name+".type", flowfile.Diagnostic{
				Field: "outputs." + name, Value: name,
				Message: fmt.Sprintf(
					"output %q changed type from %s to %s, so callers reading the old type break; keep the type, or add a new output",
					name, typeName(oldType), typeName(no.GetType())),
			}))
			continue
		}

		// An enum output's `values:` is the set a caller may switch on, so
		// *adding* a member breaks them — the run may now answer with something
		// their code has never seen — which is the exact inverse of the input
		// rule one block up, where adding a member only admits more. Removing a
		// member narrows what the run can answer with, which no consumer of the
		// old set can be surprised by, so it stays silent.
		// [removedValues] with its two sides swapped, which is what "the inverse
		// rule" means concretely: what is removed reading new-to-old is what was
		// added reading old-to-new, in the new declaration's own order.
		//
		// Gated on the old declaration already being an enum: an untyped output
		// has no values of its own, so without this gate every member of a
		// newly adopted enum reads as "added" against that empty set, contradicting
		// the type rule just above — adopting a type where there was none is
		// silent. A typed old declaration reaching this point already shares the
		// new one's type, since the block above `continue`s otherwise.
		if added := removedValues(no.GetValues(), oo.GetValues()); oo.GetType() == v1.InputDeclaration_TYPE_ENUM && len(added) > 0 {
			ds = append(ds, diagAt(pos, "outputs."+name+".values", flowfile.Diagnostic{
				Field: "outputs." + name, Value: name,
				Message: fmt.Sprintf(
					"output %q widened its declared values (added: %s), so callers switching on the old set break; keep the set, or add a new output",
					name, strings.Join(added, ", ")),
			}))
		}

		// A declared output's `must:` is a postcondition the callee guarantees,
		// so a consumer may rely on it. Dropping or changing it weakens that
		// guarantee and can invalidate a caller's assumptions, the mirror of an
		// input precondition tightening. Adding a `must:` where there was none
		// only strengthens the guarantee, which is safe, so it stays silent. A
		// changed predicate is undecidable in general, so it is treated as a
		// weakening, fail-closed, the same direction an input `must:` change
		// leans.
		if oldMust := oo.GetMust(); oldMust != "" && oldMust != no.GetMust() {
			ds = append(ds, diagAt(pos, "outputs."+name, flowfile.Diagnostic{
				Field: "outputs." + name, Value: name,
				Message: fmt.Sprintf(
					"output %q weakened its guarantee (its `must:` was removed or changed), so callers relying on it break; keep the guarantee, or add a new output", name),
			}))
		}
	}

	return ds
}

// diagAt fills a diagnostic's line and column from the new file's positions when
// the key is known there, and leaves them zero otherwise. A finding with a
// position sends the author to the declaration; one without still names the
// field.
func diagAt(pos *flowfile.Positions, key string, d flowfile.Diagnostic) flowfile.Diagnostic {
	if span, ok := pos.At(key); ok {
		d.Line = span.Start.Line
		d.Column = span.Start.Column
	}
	return d
}

// mustSupply reports whether a caller must pass this input for a run to start:
// required, with no default to stand in when they pass nothing. This is the
// exact predicate flowfile/validate_call.go uses to refuse an unbound required
// input, so a break reported here is a break a caller would actually hit.
func mustSupply(d *v1.InputDeclaration) bool {
	return d.GetRequired() && d.GetDefault() == nil
}

// inputsByName keys a workflow's declared inputs by name.
func inputsByName(wf *v1.Workflow) map[string]*v1.InputDeclaration {
	m := make(map[string]*v1.InputDeclaration, len(wf.GetDeclaredInputs()))
	for _, d := range wf.GetDeclaredInputs() {
		m[d.GetName()] = d
	}
	return m
}

// outputsByName keys a workflow's declared outputs by name.
func outputsByName(wf *v1.Workflow) map[string]*v1.OutputDeclaration {
	m := make(map[string]*v1.OutputDeclaration, len(wf.GetDeclaredOutputs()))
	for _, o := range wf.GetDeclaredOutputs() {
		m[o.GetName()] = o
	}
	return m
}

// constraintNarrowed reports how the new declaration's constraints tightened
// against the old, or empty when they only loosened or held.
//
// The numeric bounds are decidable: a raised floor or a lowered ceiling refuses
// values the old contract accepted, and the reverse (or a dropped bound) only
// admits more. Adding a bound where none existed is a narrowing; removing one is
// a widening.
//
// `must:` is a free-form CEL predicate, so proving one predicate is weaker than
// another is undecidable in general. The house rule here is fail-closed, the same
// direction the rest of this file leans: an added `must:` or a changed `must:` is
// treated as a narrowing (it is reported), and a removed `must:` is treated as a
// widening (it passes). That can flag a genuine widening of a `must:` as a break;
// the author widens the text back, or moves to a new input, which is the safe
// direction for a compatibility gate that must never miss a real narrowing.
func constraintNarrowed(old, neu *v1.InputDeclaration) string {
	var reasons []string

	if raised(old.MinLen, neu.MinLen) {
		reasons = append(reasons, "min_len raised")
	}
	if lowered(old.MaxLen, neu.MaxLen) {
		reasons = append(reasons, "max_len lowered")
	}
	if raised(old.MinItems, neu.MinItems) {
		reasons = append(reasons, "min_items raised")
	}
	if lowered(old.MaxItems, neu.MaxItems) {
		reasons = append(reasons, "max_items lowered")
	}

	oldMust, newMust := old.GetMust(), neu.GetMust()
	if newMust != "" && newMust != oldMust {
		reasons = append(reasons, "must tightened")
	}

	// An enum's `values:` is a closed set, so removing a member refuses an
	// argument the old contract accepted — the identical narrowing a raised
	// `min_len` is, just over a set instead of a range. Adding a member only
	// admits more, the same direction a dropped bound does, so it is silent.
	// Reached only when the type is unchanged (the caller checks that first),
	// so both declarations are enums here whenever either carries `values:`.
	if removed := removedValues(old.GetValues(), neu.GetValues()); len(removed) > 0 {
		reasons = append(reasons, fmt.Sprintf("values removed: %s", strings.Join(removed, ", ")))
	}

	return strings.Join(reasons, ", ")
}

// removedValues returns the members of old that are absent from neu, in old's
// order, for a narrowing message that reads like the enum the author wrote
// rather than in whatever order a set iterates.
func removedValues(old, neu []string) []string {
	if len(old) == 0 {
		return nil
	}
	present := make(map[string]bool, len(neu))
	for _, v := range neu {
		present[v] = true
	}

	var removed []string
	for _, v := range old {
		if !present[v] {
			removed = append(removed, v)
		}
	}
	return removed
}

// raised reports whether a lower bound (min_len, min_items) tightened: it appeared
// where there was none, or its value went up. A higher floor refuses shorter
// values the old contract accepted.
func raised(old, neu *uint64) bool {
	if neu == nil {
		return false // Dropped: a widening.
	}
	if old == nil {
		return true // Added a floor: a narrowing.
	}
	return *neu > *old
}

// lowered reports whether an upper bound (max_len, max_items) tightened: it
// appeared where there was none, or its value went down. A lower ceiling refuses
// longer values the old contract accepted.
func lowered(old, neu *uint64) bool {
	if neu == nil {
		return false // Dropped: a widening.
	}
	if old == nil {
		return true // Added a ceiling: a narrowing.
	}
	return *neu < *old
}

// typeName renders an input type for a diagnostic in the spelling an author
// writes in the file.
//
// Delegates to [v1.DeclaredTypeName] rather than repeating its own switch:
// that function derives its spelling from the schema's descriptor, so a type
// added to [v1.InputDeclaration_Type] is named correctly here the day it is
// added, with nothing in this file to edit. A hand-kept switch here is
// exactly how TYPE_ENUM went unnamed ("unspecified") in a breaking-change
// diagnostic despite being a real, well-formed type.
func typeName(t v1.InputDeclaration_Type) string {
	return v1.DeclaredTypeName(t)
}

// --- git plumbing: read the ref's bytes without touching the working tree ---

// gitToplevel returns the absolute path of the repository root, so ref-side
// paths can be made repo-relative for `git show`.
func gitToplevel() (string, error) {
	out, err := runGit("", "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("not inside a git repository, which `flow breaking` needs to read `--against`: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// gitHasRef verifies the ref resolves to a commit locally, and otherwise returns
// the same guidance the `buf breaking` check carries: fetch the base branch.
func gitHasRef(root, ref string) error {
	if _, err := runGit(root, "rev-parse", "--verify", "--quiet", ref+"^{commit}"); err != nil {
		return fmt.Errorf(
			"git ref %q is not in the local history; fetch the base branch first, for example `git fetch origin main`", ref)
	}
	return nil
}

// gitShow returns the bytes of a repo-relative path at the ref, or an error when
// the path did not exist there.
func gitShow(root, ref, rel string) ([]byte, error) {
	return runGit(root, "show", ref+":"+rel)
}

// repoRelPaths converts each path argument to a repository-relative pathspec.
// `git ls-tree` runs from the repository root, while the paths were given
// relative to the caller's working directory, so passing them through unchanged
// would resolve them against the wrong base: run from a subdirectory, `.` would
// name the whole tree at the ref and every unrelated workflow would look
// removed. A path that escapes the repository is dropped.
func repoRelPaths(root string, paths []string) []string {
	var rels []string
	for _, path := range paths {
		if rel, ok := repoRel(root, path); ok {
			rels = append(rels, rel)
		}
	}
	return rels
}

// gitListYAML lists the repo-relative .yaml and .yml paths tracked at the ref
// under the given path arguments, so a workflow deleted since the ref is still
// seen on the ref side.
func gitListYAML(root, ref string, paths []string) []string {
	if len(paths) == 0 {
		// A bare `ls-tree` with no pathspec lists the whole tree, which is not
		// what a caller who named specific paths asked for; answer with nothing
		// rather than the entire repository.
		return nil
	}
	args := []string{"ls-tree", "-r", "--name-only", ref, "--"}
	args = append(args, paths...)
	out, err := runGit(root, args...)
	if err != nil {
		return nil
	}
	var rels []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch filepath.Ext(line) {
		case ".yaml", ".yml":
			rels = append(rels, line)
		}
	}
	return rels
}

// repoRel makes a path repo-relative, matching what `git ls-tree` and `git show`
// speak. It returns false when the path escapes the repository, which nothing
// under the walked paths should.
func repoRel(root, path string) (string, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

// runGit runs a git command, from root when it is set, and returns its stdout.
func runGit(root string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	if root != "" {
		cmd.Dir = root
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return nil, err
	}
	return stdout.Bytes(), nil
}
