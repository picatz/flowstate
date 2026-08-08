package flowfile

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
//
// A caller may also pin what it is calling. `digest:` beside a `call:` is the
// content hash of the callee as the author last read it, and it is verified
// against the bytes this compiles, at the moment it compiles them. It is a
// property of the *file* rather than of the run: the schema already carries
// SourceDigest, which records what was embedded, so nothing downstream of the
// compiler reads a pin and there is nothing there for it to read. Either the
// pin matched, and the run is the run it would have been anyway, or it did not,
// and there is no run.
//
// A pin is not written back out. [Marshal] renders a [v1.Workflow] and a pin is
// not part of one, so `flow fmt` drops it the way it drops a comment. Which is
// the same fact from the other side: a pin is over *bytes*, so anything that
// rewrites the callee, a formatter as readily as an author, changes its digest
// and needs the pin updated. That is the mechanism working rather than failing.

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

// A CallRefusal says why a `call:` target names nothing the calling file is
// allowed to read, or [CallTargetResolved] when it names something it is.
type CallRefusal int

const (
	// CallTargetResolved means the target is a path the caller may read — that
	// it exists is a separate question, and deliberately not asked here.
	CallTargetResolved CallRefusal = iota

	// CallRefusedNoCallerLocation means the calling file has no location of its
	// own, so there is no directory a relative path could be resolved against.
	CallRefusedNoCallerLocation

	// CallRefusedAbsolute means the target is an absolute path, which a call
	// may not name.
	CallRefusedAbsolute

	// CallRefusedClimbs means the target, as written, climbs above the calling
	// file's own directory.
	CallRefusedClimbs

	// CallRefusedEscapesThroughSymlink means the target stays inside the
	// calling file's directory as written but lands outside it once symlinks
	// are followed.
	CallRefusedEscapesThroughSymlink
)

// A CallTarget is where a `call:` target lands on disk, and whether the calling
// file is allowed to read it.
//
// Path is the fully resolved location — every symlink in the caller's directory
// and in the target followed — which is meaningful for two of the outcomes: it
// is the file to read when Refusal is [CallTargetResolved], and it is the place
// the target escaped *to* when Refusal is [CallRefusedEscapesThroughSymlink],
// which is what the refusal has to name to be actionable. For the other
// refusals there was never a path to speak of and it is empty.
type CallTarget struct {
	Path string

	// CallerDir is the calling file's own directory, symlinks resolved. Set
	// alongside Path when there was one to compare against.
	CallerDir string

	Refusal CallRefusal
}

// ResolveCallTarget resolves a `call:` target written in the file at callerPath,
// applying the whole of the rule a call is subject to: relative to the calling
// *file's* directory, never absolute, never above that directory as written, and
// never above it once symlinks are followed.
//
// Exported because two readers ask this question and must get one answer. The
// compiler asks it to decide which file to compile; the language server asks it
// to decide which file go-to-definition opens. A second derivation of the rule
// in the editor is how the editor and the engine come to disagree about which
// file a call names — the author navigates to one file and the run compiles
// another, with nothing anywhere saying they differ.
//
// It performs no I/O beyond following symlinks, and never reports whether the
// target exists: a missing file is the caller's to handle, because the compiler
// treats it as a diagnostic and the language server treats it as nothing to
// navigate to.
func ResolveCallTarget(callerPath, target string) CallTarget {
	if callerPath == "" {
		return CallTarget{Refusal: CallRefusedNoCallerLocation}
	}

	if filepath.IsAbs(target) {
		return CallTarget{Refusal: CallRefusedAbsolute}
	}

	clean := filepath.ToSlash(filepath.Clean(target))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return CallTarget{Refusal: CallRefusedClimbs}
	}

	callerDir := filepath.Dir(callerPath)
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
	// missing (no symlink involved at all) falls through as resolved, and
	// whoever reads it reports that in the ordinary way; what is refused here
	// is specifically a real path outside callerDir, not a path that cannot be
	// resolved at all.
	if realCallerDir, err := filepath.EvalSymlinks(callerDir); err == nil {
		if realResolved, err := filepath.EvalSymlinks(resolved); err == nil {
			rel, relErr := filepath.Rel(realCallerDir, realResolved)
			escapes := relErr != nil || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(filepath.ToSlash(rel), "../")
			if escapes {
				return CallTarget{Path: realResolved, CallerDir: realCallerDir, Refusal: CallRefusedEscapesThroughSymlink}
			}

			// Report the fully-resolved real path from here on, not the symlink
			// path just verified: re-deriving it from `resolved` after the check
			// above would leave a window between the check and the read for the
			// symlink to be repointed in.
			resolved = realResolved
		}
		return CallTarget{Path: resolved, CallerDir: realCallerDir}
	}

	return CallTarget{Path: resolved, CallerDir: callerDir}
}

// call compiles a `call:` step: resolves the callee relative to this file's
// own directory, compiles it, and checks `with:` against what it declares.
//
// stepPath is the step's own path, for addressing `with:` and its entries;
// kindPath addresses the `call:` value itself. withField is the step's `with:`
// field, if it wrote one, and digestField its `digest:` pin.
func (c *compiler) call(pathNode ast.Node, stepPath, kindPath string, r ref, withField field, hasWith bool, digestField field, hasDigest bool) *v1.Call {
	callRef := ref{step: r.step, path: kindPath, label: "call"}

	target, ok := c.text(pathNode, kindPath, callRef)
	if !ok {
		return nil
	}

	located := ResolveCallTarget(c.filePath, target)
	switch located.Refusal {
	case CallRefusedNoCallerLocation:
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, but this file was compiled with no location of its own to resolve a "+
				"relative path against; compile it as a file rather than from bytes alone — "+
				"`flow validate`, `flow run` and the language server all do", target)
		return nil
	case CallRefusedAbsolute:
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, an absolute path; a call is resolved relative to the file that calls "+
				"it and may not name an absolute one", target)
		return nil
	case CallRefusedClimbs:
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, which climbs above the directory of the file that calls it; a call may "+
				"reach anything at or below its own file's directory and nothing above it", target)
		return nil
	case CallRefusedEscapesThroughSymlink:
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, which resolves — through a symlink — to %q, outside %q; a call may "+
				"reach anything at or below its own file's directory and nothing above it, "+
				"and a symlink does not change what \"at or below\" means", target, located.Path, located.CallerDir)
		return nil
	}
	resolved := located.Path

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

	// Hashed from the bytes just read, before anything else is done with them, so
	// that the digest an author's pin is checked against and the digest recorded
	// on the compiled call are two readings of one array already in memory.
	//
	// Never re-read from disk to check a pin. A second read is a second file, as
	// far as anything can tell: it leaves a window between the bytes that were
	// verified and the bytes that get embedded, which is precisely the gap a pin
	// exists to close.
	sourceDigest := formatSourceDigest(data)

	// Checked before the callee is compiled, and refused rather than reported
	// alongside whatever compiling it would say.
	//
	// Fail closed: a pin that does not verify says the caller has not authorized
	// these bytes, so there is nothing to gain by type-checking `with:` against a
	// file the author never sanctioned, and a page of diagnostics drawn from it
	// would bury the one that explains them.
	if hasDigest && !c.verifySourcePin(digestField.value, stepPath, r, target, sourceDigest) {
		return nil
	}

	callee, _, err := parse(data, resolved, ancestors, c.callBudget)
	if err != nil {
		c.report(spanOfNode(pathNode), callRef,
			"calls %q, which failed to compile:\n%s", target, indentLines(err.Error()))
		return nil
	}

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

	// SourceDigest records which bytes produced the embedded specification, for
	// whoever audits a run afterward. When the step wrote a `digest:` it is also
	// the value that pin was just held against, which is the whole of what the
	// pin buys: the same string, from the same read, checked once here rather
	// than written down twice and never compared.
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

// A `digest:` pin is written as the algorithm, a colon, and the hash in hex:
// `sha256:` followed by 64 characters. That is the form [v1.Call]'s SourceDigest
// is already recorded in and the form a container image reference uses, so an
// author reading a compiled run and an author writing a pin see one spelling.
//
// Both the spelling and the hashing now live in [v1.ContentDigest], because a
// second surface needed to name bytes by their hash and a second helper beside
// it would have been the same value written down twice. These names stay as the
// compiler's local vocabulary for what that function produces.
const (
	sourceDigestPrefix = v1.ContentDigestPrefix
	sourceDigestHexLen = v1.ContentDigestHexLen
)

// formatSourceDigest renders a callee's bytes as the digest this tree writes
// everywhere: lower-case hex, algorithm first.
//
// One function because there is one spelling. A pin is compared against what
// this returns and every diagnostic prints what this returns, so the digest an
// author is told to adopt is the digest that will then match.
func formatSourceDigest(data []byte) string {
	return v1.ContentDigest(data)
}

// verifySourcePin checks an author's `digest:` against the callee's bytes, and
// reports whether the call may go on being compiled.
//
// actual is the digest of the bytes already read, never a fresh hash of a fresh
// read: see [compiler.call].
//
// # Case
//
// A pin is compared lower-cased. Hex has no case and neither does the algorithm
// label, so `SHA256:AB12…` and `sha256:ab12…` name the same bytes, and refusing
// one of them would be refusing a pin copied out of a tool that renders
// upper-case for a difference that means nothing. The tree writes exactly one
// form, [formatSourceDigest]'s, from [hex.EncodeToString], which is lower-case,
// so that is the form every diagnostic prints and the form a pin normalizes to
// before being compared.
func (c *compiler) verifySourcePin(pinNode ast.Node, stepPath string, r ref, target, actual string) bool {
	pinPath := fieldPath(stepPath, "digest")
	pinRef := ref{step: r.step, path: pinPath, label: "digest"}

	written, ok := c.text(pinNode, pinPath, pinRef)
	if !ok {
		return false
	}
	pin := strings.ToLower(written)

	if !wellFormedSourcePin(pin) {
		c.report(spanOfNode(pinNode), pinRef,
			"is %s, which is not the shape of a pin; write `sha256:` and the 64 hex characters "+
				"of the callee's SHA-256, which for %q is `digest: %s` right now",
			describeWrittenPin(written), target, actual)
		return false
	}

	if pin != actual {
		c.report(spanOfNode(pinNode), pinRef,
			"pins %q at %s, but that file hashes to %s right now; a mismatch means the callee "+
				"changed since the pin was written, so read what it does now and then write "+
				"`digest: %s` to adopt it",
			target, pin, actual, actual)
		return false
	}

	return true
}

// wellFormedSourcePin reports whether a lower-cased pin is written the way a
// digest is written here.
//
// Length before content, deliberately. The pin is text an outside party chose,
// bounded only by the megabyte a whole Flowfile may be, and a fixed-width hash
// is the one input where the cheap check is also the complete one.
func wellFormedSourcePin(pin string) bool {
	digits, ok := strings.CutPrefix(pin, sourceDigestPrefix)
	if !ok || len(digits) != sourceDigestHexLen {
		return false
	}
	_, err := hex.DecodeString(digits)
	return err == nil
}

// describeWrittenPin renders what the author wrote, for the diagnostic above,
// without echoing an arbitrarily long value into a message.
//
// A pin is a fixed-length string, so something several times longer than one is
// not a near miss worth quoting back at whoever wrote it, and what is quoted
// back would otherwise be up to a megabyte of somebody else's choosing, printed
// into an editor's problem list.
func describeWrittenPin(written string) string {
	if len(written) > 2*(len(sourceDigestPrefix)+sourceDigestHexLen) {
		return fmt.Sprintf("%d characters long", len(written))
	}
	return strconv.Quote(written)
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
		case *v1.Node_Loop:
			// A loop body's nodes are compiled nodes like any others, so they count
			// against the expansion budget too — without this a `call:` tree could
			// hide arbitrarily many nodes inside loop bodies under the bound, the
			// billion-laughs shape through a construct the counter did not descend.
			n += countCompiledNodes(kind.Loop.GetBody())
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
