package flowfile

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/goccy/go-yaml/token"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
	"google.golang.org/protobuf/proto"
)

// Parsing goes straight from the YAML document tree to the schema. There is no
// intermediate set of Go structs shaped like the DSL, because that would be a
// second definition of the workflow model to keep in step with the schema by hand,
// and because decoding into structs discards the one thing a compiler most needs:
// where in the source each value was written.

// implicitNull is the token type the parser gives a key written with no value at
// all, as distinct from one written as null. The difference is worth keeping: the
// first is nearly always an unfinished line, the second is a deliberate value.
const implicitNull = token.ImplicitNullType

// A Flowfile is written by a person, so how deeply it nests and how many values it
// holds have limits well above anything hand-written and well below what would
// exhaust the machine.
//
// Both are needed, and neither is theoretical. Every level of nesting is another
// path recorded, so depth alone costs quadratic memory; and an alias may be
// referenced many times, so a short document can expand into an enormous one — the
// YAML equivalent of a billion laughs. A compiler that a Flowfile can hang is a
// compiler an editor and a server cannot safely run on input from elsewhere.
// The third bound is on the input itself, because the two above are enforced while
// walking a document the YAML parser has already built. Building that tree is work
// too, and it happens before this package sees a single node.
//
// One mebibyte is the same limit the language server analyzes up to, so a file an
// editor will check is a file `flow validate` will compile.
const (
	maxDepth = 64
	maxNodes = 100_000
	maxBytes = 1 << 20
)

// stepsKey is the one key a step list is ever written under.
//
// Every place the grammar takes a list of steps spells it the same way — the
// workflow's own `steps:`, a `for_each:`'s, a `loop:`'s, a `parallel:` branch's,
// and a `switch:` case's or default's — which is what makes "an element of a
// `steps:` sequence" a complete definition of *a step* rather than a guess at
// one. Named rather than spelled out at each site so that anything reasoning
// about where steps live reads it from here: [pinCollector] does exactly that,
// because a `digest:` is only a pin when the mapping holding it is a call step
// (#833), and a collector that decided by key names alone would read
// `vars: {call: …, digest: …}` — two ordinary variables — as a security pin.
// TestEveryStepListIsSpelledStepsKey holds the claim this constant makes.
const stepsKey = "steps"

// The keys each part of a Flowfile accepts. Anything else is reported: a
// misspelled `timout:` that is silently ignored does nothing at run time and gives
// the author no reason to doubt it, which is the worst of both outcomes.
var (
	workflowKeys = []string{"edition", "name", "labels", "description", "inputs", "outputs", "vars", "steps", "triggers", "signals", "plugins"}

	// The keys of one input declaration and of one output declaration. Both are
	// mappings keyed by the name being declared, so these are the keys *under* a
	// name rather than the names themselves — which are the author's and are checked
	// as names, not as keys.
	inputKeys = []string{
		"type", "values", "required", "default", "description", "example", "sensitive",
		"min_len", "max_len", "min_items", "max_items", "must",
	}
	outputKeys = []string{"value", "description", "must", "sensitive"}

	// stepPropertyKeys say which step this is, how it runs, and what it is for —
	// everything except what work it does.
	//
	// `with` is here rather than in nodeKindKeys because it is not itself a kind
	// of work — `call:` is — in the same sense `steps:` is not what makes a
	// `for_each` a loop. It binds the callee's declared inputs, and only means
	// anything beside `call:`; [validate.go] is where a `with:` on any other kind
	// of step is refused, since that is a property of the *file* and belongs
	// there rather than here.
	//
	// `digest` is the second of those: the content hash the caller pins its
	// callee to, checked against the bytes the call reads when this file is
	// compiled. See [compiler.verifySourcePin].
	// `async` is here rather than in nodeKindKeys for the reason `undo` is: it
	// is not a kind of work, it is a property of a step doing some other kind —
	// the one marker that lets execution depart from written order (#418).
	stepPropertyKeys = []string{"id", "description", "if", "vars", "timeout", "retry", "continue_on_error", "undo", "async", "with", "digest"}

	// nodeKindKeys are the kinds of work that are not a task, and so name a node
	// kind in the schema rather than anything in the registry.
	nodeKindKeys = []string{"for_each", "loop", "parallel", "sleep", "wait_until", "wait_for_signal", "wait_for_signals", "call", "value", "switch"}

	retryKeys   = []string{"attempts", "interval", "backoff", "max_interval"}
	forEachKeys = []string{"items", "as", "max_parallel", "steps"}
	loopKeys    = []string{"steps", "until", "max_iterations", "as", "init", "update"}
	branchKeys  = []string{"steps"}

	// The keys of a `switch:` mapping, of one entry in its `cases:` list, and of
	// its `default:`. The discriminant key is `value:` — not `on:`, which is a
	// YAML 1.1 boolean spelling, and a key some tool in a future chain would
	// read as `true` is the Norway problem placed load-bearing.
	switchKeys        = []string{"value", "cases", "default"}
	switchCaseKeys    = []string{"case", "steps"}
	switchDefaultKeys = []string{"steps"}
)

// A step names the work it does directly — `http:` with the request under it —
// so the keys a step accepts are not a constant. They are the properties, plus
// the non-task kinds, plus every task the registry has.
//
// Deriving it means a task added to the registry becomes writable with no change
// here, and an unknown-key diagnostic offers task names alongside grammar
// keywords, because from an author's position those are the same kind of thing.
// [v1.ReservedStepKeys] keeps the two halves disjoint, so a key is never
// ambiguous.
func stepKeys() []string {
	keys := make([]string, 0, len(stepPropertyKeys)+len(nodeKindKeys)+8)
	keys = append(keys, stepPropertyKeys...)
	keys = append(keys, nodeKindKeys...)
	return append(keys, v1.TaskNames()...)
}

// couldBeATaskName reports whether a key is spelled the way a task name must be.
//
// The same pattern the schema puts on Task.name, so this package and the
// validator agree about what a task could be called.
//
// The leading character is checked separately because a name may not start with
// a digit — `TaskManifest.name` is `^[a-z][a-z0-9_]*$` — and without that, `123:`
// reads as a plausible task. YAML gives that key to the parser as a number, which
// the compiler refuses outright with "keys must be strings"; a promotion rule
// looser than the schema it claims to mirror makes the language server model a
// task where the compiler sees an error.
func couldBeATaskName(key string) bool {
	// One dot, splitting a plugin's name from its task — `slack.post` — and no
	// more than one, matching the schema's pattern. The halves are checked with
	// the same rule as a bare name, so `.post`, `slack.`, and `a.b.c` all stay
	// unknown keys rather than plausible tasks.
	if plugin, task, dotted := strings.Cut(key, "."); dotted {
		// Cut splits at the first dot, so a second one survives inside `task` —
		// where the recursion would happily split it again. Counting keeps
		// `a.b.c` an unknown key instead of a plausible plugin task.
		return strings.Count(key, ".") == 1 && couldBeATaskName(plugin) && couldBeATaskName(task)
	}

	if key == "" {
		return false
	}
	if first := key[0]; !(first >= 'a' && first <= 'z') && !(first >= 'A' && first <= 'Z') {
		return false
	}
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

// stepKindKeys are the keys saying what a step *does*. Exactly one is required.
func stepKindKeys() []string {
	return append(slices.Clone(nodeKindKeys), v1.TaskNames()...)
}

// retiredStepKeys are spellings a step used to have, and what to write instead.
//
// A key the language no longer has is otherwise reported as an unknown task,
// which is true and useless: it tells an author what their file is not and leaves
// them to guess what it should be. `task:` is the one that matters, because it is
// the shape every Flowfile written before the flattening has, and the shape a
// model trained on them will keep producing.
//
// This is where a future retirement goes. A key listed here is never taken for a
// task, so the specific message is the only one reported — see [StepTaskKeys].
var retiredStepKeys = map[string]string{
	// The example names `log:` rather than `echo:` deliberately. Advice is followed
	// literally, and `echo` is retired — a diagnostic whose worked example produces a
	// file the next diagnostic rejects has sent the author one step sideways.
	"task": "a step names its task directly now: replace `task:` with the task's own name and put its inputs beneath, so `task:` / `name: log` / `inputs:` / `message: hi` becomes `log:` / `message: hi`",

	// The three the registry no longer holds. Each says what replaced it *and* which
	// of the two things the author was doing with it — because that is the question
	// the replacement depends on, and the one `flow fix` cannot answer for them.
	//
	// Without an entry here each of these would be reported as an unknown task, with
	// a nearest-match suggestion among the tasks that remain. That reads as "you
	// misspelled something" to an author who spelled a real capability correctly and
	// is now looking for where it went.
	"echo": "`echo:` is retired, and which replaces it depends on what the step was for: " +
		"a value later steps read is `vars:` at the top of the file, read as `${vars.<name>}`; " +
		"a line for a person to see is `log:`. `echo` was doing both under a name that means neither: " +
		"it returned a value, which is not what a message is for. Run `flow fix`: it rewrites the first " +
		"and reports the second, since only you know which was meant",

	"printf": "`printf:` is retired: the profile ships CEL's strings extension, so a format is an " +
		"expression like any other: `${'hello %s, %d left'.format([vars.name, 0])}`. `format` is " +
		"specified at the CEL level, which is the determinism story a task wrapping Go's `fmt` could " +
		"never have. Run `flow fix` to rewrite it",

	"cel": "`cel:` is retired: an expression is a *value* now rather than a step that produces one, " +
		"so write it where the value is wanted: inline as `${...}`, or once at the top under `vars:` " +
		"if more than one step needs it. The name answered \"which evaluator?\" when the question was " +
		"\"what does this value do?\". Run `flow fix`: it rewrites a step whose result is read into a " +
		"`vars:` binding and reports one whose result is not",
}

// retiredPatternMessage builds the diagnostic for an input declaration's
// retired `pattern:` key — [declaredInput]'s counterpart to [retiredStepKeys]
// one level up, held back the same way and for the same reason, but not
// joined to that table: a `retiredStepKeys` entry replaces a word with
// another fixed word, and `pattern:`'s replacement is not fixed — it depends
// on the regular expression the author wrote, so the remedy is built from the
// value in hand rather than looked up.
//
// n is that value, already resolved through aliases. When it reads as a plain
// scalar this echoes it verbatim inside a `must: this.matches(...)` call, so
// the message is copy-pasteable rather than a shape to figure out; anything
// else — an expression, no value at all — falls back to naming the shape
// generically, since RE2 syntax has not changed and the advice is accurate
// either way.
func retiredPatternMessage(n ast.Node) string {
	const remedy = "`pattern:` is removed: `must:` already says the identical thing, through the " +
		"one constraint language this schema has, instead of a second one that duplicated a corner of it"

	regex, ok := scalarText(n)
	if !ok {
		return fmt.Sprintf("%s. Write `must: this.matches('<the same regular expression>')` instead", remedy)
	}
	return fmt.Sprintf("%s. Write `must: this.matches(%s)` instead", remedy, celMatchesArgument(regex))
}

// celMatchesArgument renders regex as a CEL string literal suitable for
// `this.matches(...)`, preferring a raw string literal — `r'...'` — because a
// regex's own backslash escapes (`\.`, `\d`, `\s`, ...) pass through
// unchanged in one, where CEL's ordinary escape table (`\n`, `\t`, `\\`, a
// handful of others) does not cover most of what RE2 uses and would need
// every backslash doubled by hand to read back as the original pattern.
//
// A raw string still ends at its own unescaped quote character, so this picks
// whichever of `'` or `"` regex does not itself contain — the case a
// character class like `[^']` puts one in, however uncommon. If it contains
// both (rarer still), the fallback is an ordinary literal with every
// backslash and double quote escaped, which always parses: CEL's `\\` escape
// reproduces a literal backslash exactly, so a regex's own `\.` survives as
// `\\.` — two input characters producing the two the regex needs — the same
// way any other character neither `\` nor `"` passes through unchanged.
func celMatchesArgument(regex string) string {
	switch {
	case !strings.Contains(regex, "'"):
		return "r'" + regex + "'"
	case !strings.Contains(regex, `"`):
		return `r"` + regex + `"`
	default:
		escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(regex)
		return `"` + escaped + `"`
	}
}

// retiredMinMaxMessage builds the diagnostic for an input declaration's
// retired `min:`/`max:` key — [declaredInput]'s counterpart to
// [retiredPatternMessage], held back from [inputKeys] for the identical
// reason: the remedy depends on the number the author wrote, so it is built
// from the value in hand rather than looked up.
//
// key is "min" or "max", deciding which comparison operator the remedy uses.
// n is the value node, already resolved through aliases. When it reads as a
// plain numeric scalar this echoes its own source text verbatim into the
// `must:` expression — not a value parsed and reformatted through float64,
// which is the exact conversion that made `min:`/`max:` lossy on `type: int`
// in the first place (see the schema's own doc on the reserved fields). A
// diagnostic that reproduced the bug in its own remedy would be worse than
// none.
func retiredMinMaxMessage(key string, n ast.Node) string {
	op := ">="
	if key == "max" {
		op = "<="
	}

	remedy := fmt.Sprintf(
		"`%s:` is removed: on `type: int` its bound is stored as a float64 and can silently accept "+
			"a value the bound was written to refuse once the number is large enough to lose precision "+
			"in that conversion. `must: this %s <N>` is exact at every magnitude either declared type allows",
		key, op)

	text, ok := numericScalarText(n)
	if !ok {
		return fmt.Sprintf("%s. Write `must: this %s <the same number>` instead", remedy, op)
	}
	return fmt.Sprintf("%s. Write `must: this %s %s` instead", remedy, op, text)
}

// numericScalarText returns the literal source text of an integer or float
// scalar, unparsed — the number as the author wrote it, not as YAML decoded
// it. [ast.IntegerNode.String] and [ast.FloatNode.String] both return the
// token's own text, so a numeral of any magnitude survives without passing
// through a Go numeric type that could round it.
func numericScalarText(n ast.Node) (string, bool) {
	switch n.(type) {
	case *ast.IntegerNode, *ast.FloatNode:
		return n.String(), true
	default:
		return "", false
	}
}

// retiredUniqueMessage builds the diagnostic for an input declaration's
// retired `unique:` key. Unlike `pattern:`/`min:`/`max:`, the remedy needs
// nothing from the value in hand — `unique: true` has exactly one meaning —
// so this is a constant message rather than one built per occurrence.
const retiredUniqueMessage = "`unique:` is removed: `must: this == this.distinct()` says the identical " +
	"thing, through the one constraint language this schema has, instead of a second one that " +
	"duplicated a corner of it. Write `must: this == this.distinct()` instead"

// StepTaskKeys reports which of a step's keys name the task it runs, in the order
// they were written.
//
// A step names its task directly, so deciding which key is the task is a question
// with no fixed answer: any key that is not grammar might be one. Two callers ask
// it — the compiler, deciding what to build, and the language server, deciding
// what to underline — and they must reach the same answer, because a diagnostic
// the editor puts on a different token than `flow validate` does is a diagnostic
// the author has to reconcile by hand. Exported for that reason rather than for
// general use.
//
// The rule:
//
//   - A registered task name is a task. Nothing else needs deciding.
//   - A step property (`id`, `timeout`, ...) or a non-task kind (`for_each`, ...)
//     is not.
//   - Neither is a near-miss of one: `timout:` is a misspelled property and wants
//     "did you mean timeout?", which is a better message than "unknown task".
//   - Nor is anything spelled the way a task name cannot be — `${chosen.task}:` is
//     somebody reaching for a task chosen at run time, which the grammar
//     deliberately cannot express.
//   - What is left is an unregistered name, and it counts only when the step has
//     no other kind. A step that already loops and also has a stray key has a key
//     problem; calling that key a task would report "this does two kinds of work",
//     which is true of the reading and not of the file.
//
// The last clause is why this takes the whole step rather than one key: the answer
// for `shell` depends on what else the step says.
func StepTaskKeys(keys []string) []string {
	// The words the step grammar speaks for, which is [v1.ReservedStepKeys] and
	// not the subset this build happens to implement.
	//
	// Those two are deliberately different: `call`, `vars`, `undo` and `needs` are
	// reserved for grammar not written yet, precisely so that adding them later is
	// a change to one package rather than a break for anyone who registered a task
	// under the name. Promoting them here would give that away — a `needs:` written
	// today would compile as a task nobody registered, and the day `needs:` becomes
	// grammar, a file that compiles would silently mean something else.
	grammar := v1.ReservedStepKeys()
	kinds := stepKindKeys()

	var out []string
	for _, key := range keys {
		if _, known := v1.LookupTask(key); known {
			out = append(out, key)
		}
	}

	// A registered name, or a `for_each:`, settles it: the step says what it does,
	// so no other key needs promoting to say it.
	if len(out) > 0 || slices.ContainsFunc(keys, func(k string) bool { return slices.Contains(kinds, k) }) {
		return out
	}

	// At most one, and the first. A second unrecognised key is a *key* problem —
	// a stray line, a misspelling — and promoting it too would report "has both
	// run and environment; split it into two steps", which is true of the reading
	// and not of the file, and whose advice yields two broken steps.
	//
	// Which one is arbitrary only in appearance: the first is the one the author
	// most likely meant as the work, and everything after it then gets the
	// "unknown key; the keys here are ..." message it would have got beside a
	// registered task name. One authoring mistake should not draw two different
	// diagnostics depending on whether the neighbouring key is in the registry.
	for _, key := range keys {
		if slices.Contains(grammar, key) {
			continue
		}
		if _, retired := retiredStepKeys[key]; retired {
			// Spelled like a task and reads like one, which is exactly why it must
			// not be taken for one: `unknown task "task"` would bury the message
			// that says what to write.
			continue
		}
		if _, near := nearest.Name(key, grammar); near {
			continue
		}
		if !couldBeATaskName(key) {
			continue
		}
		return append(out, key)
	}
	return out
}

// Parse compiles a Flowfile into a workflow and the source positions of
// everything in it.
//
// The positions are what lets a diagnostic name a line and column, and what lets
// an editor underline the token at fault rather than the whole step. Callers that
// do not need them can use [Unmarshal].
//
// A failure to compile is returned as [Diagnostics], one per problem found, so a
// caller can report all of them at once. A failure to parse the YAML itself is
// returned the same way — one [Diagnostic], translated from the parser's own
// error so an author meets one error language regardless of which layer of the
// file rejected it (#654). goccy's errors carry the token they failed on, which
// is where the position comes from; an error that carries none reports with the
// position left at zero, [Diagnostic]'s own honest answer for a problem with the
// document as a whole.
//
// Compiled from bytes with no file identity, so a `call:` step in this file
// cannot be resolved — there is no directory to resolve it relative to — and is
// refused with a diagnostic saying so. Use [ParseFile] to compile a file that
// may contain one.
func Parse(data []byte) (*v1.Workflow, *Positions, error) {
	return parse(data, "", nil, new(int))
}

// ParseFile compiles a Flowfile read from disk, exactly as [Parse] does, but
// additionally resolves any `call:` step relative to path's own directory.
//
// A call is the only part of the grammar that reads another file, and doing so
// here — at whichever client is compiling this one — is the point: see the
// package doc on `v1.Call` for why filesystem access belongs at the edge that
// already has an author's files, and never at a worker.
func ParseFile(path string) (*v1.Workflow, *Positions, error) {
	data, err := readBoundedSource(path)
	if err != nil {
		return nil, nil, err
	}
	return parse(data, path, nil, new(int))
}

// ParseAt is [Parse] for bytes that did not come from path but should be
// treated as if they were about to be written there — an editor's in-memory
// buffer for a file that may hold unsaved changes, most notably. A `call:` step
// resolves relative to path's directory exactly as it would under [ParseFile];
// what differs is that data, not whatever path currently holds on disk, is
// compiled as this file's own content. A callee a call reaches is still read
// from disk, because a callee is a *different* file's content and this
// function has no in-memory version of it to prefer.
func ParseAt(data []byte, path string) (*v1.Workflow, *Positions, error) {
	return parse(data, path, nil, new(int))
}

// parse is the whole of what both [Parse] and [ParseFile] do, plus what a
// nested call needs and an ordinary file never supplies: the chain of files
// already being compiled, for cycle detection across files, and the running
// total of nodes a call has compiled so far, shared across the whole tree —
// see call.go.
func parse(data []byte, path string, callStack []string, callBudget *int) (*v1.Workflow, *Positions, error) {
	if len(data) > maxBytes {
		return nil, nil, Diagnostics{{
			Line:   1,
			Column: 1,
			Message: fmt.Sprintf(
				"file is %d bytes, larger than the %d byte limit a Flowfile is compiled up to; nothing was read",
				len(data), maxBytes),
		}}
	}

	file, err := parser.ParseBytes(data, 0)
	if err != nil {
		return nil, nil, yamlSyntaxDiagnostics(err)
	}

	c := &compiler{
		pos:        newPositions(),
		anchors:    make(map[string]ast.Node),
		filePath:   path,
		callStack:  callStack,
		callBudget: callBudget,
	}
	workflow := c.compile(file)
	if len(c.diags) > 0 {
		return nil, nil, c.sorted()
	}
	return workflow, c.pos, nil
}

// yamlCoordinate matches the `at [line:column]` goccy's parser appends to a
// duplicate-key message to name the earlier definition — see
// parser.go's `"mapping key %q already defined at [%d:%d]"`. It is goccy's own
// bracket spelling, not this package's `line:column` one, so left untranslated
// it would put a second, differently-punctuated position inside a message this
// package otherwise renders through exactly one convention (see position.go and
// #384's positionLine). Rewritten to prose rather than dropped, because the
// position it names is still the information an author needs to resolve a
// duplicate key.
//
// Anchored on the literal `at ` prefix and the end of the string — the exact
// shape the parser generates — rather than matching any `[N:M]`-shaped run
// wherever it occurs. A looser match would also rewrite a key whose own name is
// spelled that way: goccy quotes the key verbatim into the message ahead of
// this suffix, so a mapping key literally named `[1:2]` produces `mapping key
// "[1:2]" already defined at [3:4]`, and only the trailing, unquoted occurrence
// is the parser's own position rather than the author's text.
var yamlCoordinate = regexp.MustCompile(` at \[(\d+):(\d+)\]$`)

// yamlSyntaxDiagnostics translates a failure from the YAML parser into the
// [Diagnostic] grammar every other failure in this package speaks (#654).
//
// Before this, a YAML-level failure — a duplicate key, a tab used for
// indentation, an unterminated quote — bypassed the grammar entirely and
// returned goccy's own error, unpositioned as far as this package's callers
// could tell and rendered in goccy's own format (`[3:1] mapping key already
// defined`) rather than this tool's (`workflow.yaml:3:1: ...`). Every caller of
// [Parse] already widens a non-[Diagnostics] error into one unpositioned
// diagnostic naming the whole document — see cmd/flow's errDiagnosticsOf and
// the server's Validate and Compile handlers — so a YAML syntax error used to
// take that fallback and lose its position on the way, even though goccy had
// one to give.
//
// goccy's errors implement [yaml.Error], which carries the token the parser
// failed on, so the position is recovered from there rather than reimplemented.
// A future error that does not implement it — there is no such case in this
// version of goccy — still gets the standard shape, with the position left
// unset the way [Diagnostic] already reports "a problem with the document as a
// whole" everywhere else in this package.
func yamlSyntaxDiagnostics(err error) Diagnostics {
	d := Diagnostic{Message: err.Error()}

	var yamlErr yaml.Error
	if errors.As(err, &yamlErr) {
		if msg := yamlErr.GetMessage(); msg != "" {
			d.Message = msg
		}
		if tok := yamlErr.GetToken(); tok != nil && tok.Position != nil {
			d.Line = tok.Position.Line
			d.Column = tok.Position.Column
		}
	}

	d.Message = yamlCoordinate.ReplaceAllString(d.Message, " at line $1, column $2")

	return Diagnostics{d}
}

// A compiler walks one document tree, building the workflow and collecting every
// problem it finds rather than stopping at the first.
type compiler struct {
	diags   Diagnostics
	pos     *Positions
	anchors map[string]ast.Node

	// depth and nodes account for the work one document causes, bounded by
	// maxDepth and maxNodes. overflowed records that the limit was already
	// reported, since every enclosing value would otherwise report it again.
	depth      int
	nodes      int
	overflowed bool

	// filePath is this file's own location, for resolving a `call:` step's path
	// relative to its directory. Empty when this file was compiled from bytes
	// with no path — see [Parse] — in which case a `call:` here is refused.
	filePath string

	// callStack is the chain of files already being compiled to reach this one,
	// outermost first, not including this file itself. Checked in call.go
	// against the file a `call:` step would read, so a cycle across files is
	// caught the same way an anchor referring to its own value already is.
	callStack []string

	// callBudget is the total compiled node count across every callee resolved
	// so far in this file's whole call tree, shared by pointer with every
	// nested compile — see [maxCallExpansionNodes].
	callBudget *int
}

// enter accounts for descending into one more value, and reports whether the
// compiler may continue. A caller given true must call [compiler.exit].
func (c *compiler) enter(n ast.Node, r ref) bool {
	c.nodes++
	c.depth++
	if c.depth <= maxDepth && c.nodes <= maxNodes {
		return true
	}

	c.depth--
	if !c.overflowed {
		c.overflowed = true
		if c.depth >= maxDepth {
			c.report(spanOfToken(nodeToken(n)), r,
				"nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
		} else {
			c.report(spanOfToken(nodeToken(n)), r,
				"holds more than %d values once aliases are expanded, which is more than a Flowfile is meant to hold", maxNodes)
		}
	}
	return false
}

// exit undoes one [compiler.enter].
func (c *compiler) exit() { c.depth-- }

// nodeToken returns a node's own token without walking its subtree, which matters
// on the path that reports a document too large to walk.
func nodeToken(n ast.Node) *token.Token {
	if n == nil {
		return nil
	}
	return n.GetToken()
}

// A ref names what a diagnostic is about, so that every message reads the way the
// ones in validate.go do: the step it is in, and the input or property at fault.
type ref struct {
	// step is the enclosing step's id, when it has one.
	step string

	// input is the task input at fault, when the value is one.
	input string

	// task is the name of the task the input belongs to, when there is one.
	//
	// Carried because one rule genuinely depends on it: whether a secret reference
	// may be nested inside a list or a mapping is a property of the *input*, since
	// only a task that applies an input's entries itself can carry a reference to
	// the worker. Nothing else reads it, and a value that is not a task input —
	// a var, a declaration's default — leaves it empty and is refused, which is
	// the fail-closed direction.
	task string

	// path addresses the value in the source, and names it in a message when
	// there is no step id to name instead.
	path string

	// label is how a message should refer to the property — "timeout",
	// "task name" — for a field that is not a task input.
	label string
}

// report records one problem.
func (c *compiler) report(span Span, r ref, format string, args ...any) {
	c.reportWith(span, r, nil, format, args...)
}

// reportWith records one problem alongside the repairs a program may apply to
// the source, which is [compiler.report] for the few checks that can name the
// exact text they would write.
//
// Separate rather than a variadic on report, so that adding an edit to a site is
// a visible change at that site: a checker offering one is asserting it knows
// what the region it is replacing means, and that assertion should be readable
// in the call.
func (c *compiler) reportWith(span Span, r ref, edits []*v1.SuggestedEdit, format string, args ...any) {
	message := fmt.Sprintf(format, args...)

	d := Diagnostic{
		Line:    span.Start.Line,
		Column:  span.Start.Column,
		Message: message,
		Edits:   edits,
	}
	switch {
	case r.input != "":
		d.Step, d.Field = r.step, r.input
	case r.step != "":
		d.Step = r.step
		if r.label != "" {
			d.Message = r.label + " " + message
		}
	default:
		d.Field = r.path
	}

	c.diags = append(c.diags, d)
}

// sorted returns the diagnostics in source order, so the same file always reports
// the same way.
func (c *compiler) sorted() Diagnostics {
	slices.SortStableFunc(c.diags, func(a, b Diagnostic) int {
		if a.Line != b.Line {
			return a.Line - b.Line
		}
		if a.Column != b.Column {
			return a.Column - b.Column
		}
		return strings.Compare(a.Message, b.Message)
	})
	return c.diags
}

// compile builds the workflow from a parsed file.
func (c *compiler) compile(file *ast.File) *v1.Workflow {
	var bodies []ast.Node
	for _, doc := range file.Docs {
		if doc.Body != nil {
			bodies = append(bodies, doc.Body)
		}
	}

	start := Span{Start: Position{Line: 1, Column: 1}, End: Position{Line: 1, Column: 1}}
	switch {
	case len(bodies) == 0:
		c.report(start, ref{}, "the file is empty; a Flowfile needs a name and at least one step")
		return nil
	case len(bodies) > 1:
		c.report(spanOfNode(bodies[1]), ref{},
			"a Flowfile holds one workflow, but this file has %d documents separated by ---; put each workflow in its own file",
			len(bodies))
		return nil
	}

	// The strict subset is enforced before anything is resolved or expanded: a
	// document containing an anchor, alias, or merge key is refused on the
	// presence of the construct, so a billion-laughs shape is rejected without
	// ever following an alias. This must precede collectAnchors and every call to
	// entries, the two places expansion happens. See strict.go.
	if !c.refuseStrictYAML(bodies[0]) {
		return nil
	}

	for _, doc := range file.Docs {
		c.collectAnchors(doc.Body)
	}
	if !c.checkAnchorCycles() {
		// Every walk below follows aliases, so a cycle would not merely produce a
		// wrong answer: it would not terminate.
		return nil
	}

	root := bodies[0]
	entries, ok := c.entries(root, "", ref{path: "workflow"})
	if !ok {
		return nil
	}

	// The edition is settled before any other key is judged, which is why this
	// reads the entries itself rather than the checked field set.
	//
	// A file written in a grammar this build does not have will have other
	// problems, and every one of them describes the wrong language: `nonsense:` is
	// an unknown key *here*, and might be a perfectly good key in the edition the
	// file claims. Reporting those alongside would bury the one diagnostic that
	// explains all the rest.
	//
	// Required, because absent is not a default but a promise to reinterpret — see
	// [missingEdition]. `flow fix` writes it, so the ceremony is not an author's.
	if !c.checkDeclaredEdition(entries) {
		return nil
	}

	r := ref{path: "workflow"}
	fields := c.check(c.heldForLater(entries, r, workflowKeys), r, workflowKeys)

	// The vocabulary this file's expressions are being checked against, stamped
	// here so a run evaluates against the set the compiler used rather than
	// whatever the worker executing it happens to ship with.
	workflow := &v1.Workflow{Profile: v1.CurrentProfile}

	if f, found := fields.get("plugins"); found {
		workflow.PluginRequirements = c.pluginRequirements(f.value, "plugins", ref{path: "plugins", label: "plugins"})
	}

	if f, found := fields.get("name"); found {
		name, _ := c.text(f.value, "name", ref{path: "name", label: "name"})
		workflow.Name = name
	}

	// Description is set only when the key is present, so that "no description"
	// and "an empty description" stay distinguishable — which is what the schema's
	// optional means, and what makes Marshal an exact inverse.
	// What this workflow *is*, written where an author writes it: directly under
	// the name, above everything the run computes. Labels say who owns this
	// workload and what it belongs to, and every run records them so an operator
	// can select on them later — see [v1.Workflow.Labels] and `labels` in
	// [v1.RunFilter]'s vocabulary.
	//
	// Compiled through [compiler.stringMap], which is the one thing in this
	// grammar that turns a mapping of strings into a `map<string, string>`; the
	// other caller is a signal rule's `claims:`. A second one would drift.
	if f, found := fields.get("labels"); found {
		workflow.Labels = c.stringMap(f.value, "labels", ref{path: "labels", label: "labels"})
	}

	if f, found := fields.get("description"); found {
		if description, ok := c.text(f.value, "description", ref{path: "description", label: "description"}); ok {
			workflow.Description = proto.String(description)
		}
	}

	// What the run takes, read first because it is what a reader meets first: a
	// declaration is in scope for everything below it, and nothing below it can
	// change what it says.
	if f, found := fields.get("inputs"); found {
		workflow.DeclaredInputs = c.declaredInputs(f.value, "inputs", ref{path: "inputs", label: "inputs"})
	}

	// How the workflow starts on its own, read after what it takes and before what
	// it does — which is where an author writes it, because "this is the nightly
	// report" is a fact about the whole file rather than about any step in it.
	//
	// Nothing here starts anything: both drivers ignore this, and creating the
	// schedule is `flow schedule create`. See flowfile/triggers.go.
	if f, found := fields.get("triggers"); found {
		workflow.Triggers = c.triggers(f.key, f.value, "triggers", ref{path: "triggers", label: "triggers"})
	}

	// Who may deliver a named signal, read alongside `triggers:` for the same
	// reason: both are facts about the whole workflow's relationship with the
	// outside world rather than about any one step, even though what they
	// constrain — a `wait_for_signal:` — is written further down. See
	// flowfile/signals.go and [v1.Workflow.Signals].
	if f, found := fields.get("signals"); found {
		workflow.Signals = c.signals(f.value, "signals", ref{path: "signals", label: "signals"})
	}

	// Read before steps, because every step's expressions may reference these and a
	// reader follows the file in the order it is written.
	if f, found := fields.get("vars"); found {
		workflow.Vars = c.vars(f.value, "vars", ref{path: "vars", label: "vars"})
	}

	if f, found := fields.get(stepsKey); found {
		workflow.Steps = c.steps(f.value, "steps", ref{path: "steps", label: "steps"})
	}

	// What the run answers with, read last for the reason it is *evaluated* last:
	// every one of these expressions is written against steps that have finished.
	if f, found := fields.get("outputs"); found {
		workflow.DeclaredOutputs = c.declaredOutputs(f.value, "outputs", ref{path: "outputs", label: "outputs"})
	}

	return workflow
}

func (c *compiler) pluginRequirements(n ast.Node, path string, r ref) []*v1.PluginRequirement {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))
	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}
	if len(entries) > 64 {
		c.report(spanOfNode(n), r, "plugins has %d entries; at most 64 are allowed", len(entries))
	}
	out := make([]*v1.PluginRequirement, 0, len(entries))
	for _, e := range entries {
		p := fieldPath(path, e.name)
		version, ok := c.text(e.value, "plugin version", ref{path: p, label: e.name})
		if !ok {
			continue
		}
		if !v1.ValidPluginVersion(version) {
			c.report(spanOfNode(e.value), ref{path: p, label: e.name},
				"plugin %q requires a semantic version written as vMAJOR.MINOR.PATCH, but %q was written here", e.name, version)
			continue
		}
		out = append(out, &v1.PluginRequirement{Name: e.name, MinimumVersion: version})
	}
	return out
}

// declaredInputs compiles the top-level `inputs:` block: one entry per parameter a
// run may be started with.
//
// A repeated field rather than a map in the schema, so the order written here is
// the order everything downstream reports them in — a `--help` listing, an editor's
// completion, a diagnostic naming what a workflow declares. Which means the order
// this reads them in is part of the contract, and it is the order they were
// written.
//
// Everything a *set* of declarations can be wrong about — two sharing a name, a
// name CEL's lexer refuses, a default that is not a literal, a required input
// carrying one — belongs to [Validate], which sees the compiled workflow and can
// answer for all of them at once. What is decided here is only what one declaration
// says.
func (c *compiler) declaredInputs(n ast.Node, path string, r ref) []*v1.InputDeclaration {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	declarations := make([]*v1.InputDeclaration, 0, len(entries))
	for _, e := range entries {
		if declaration := c.declaredInput(e, path); declaration != nil {
			declarations = append(declarations, declaration)
		}
	}

	if len(declarations) == 0 {
		// Nil rather than an empty slice, so `inputs:` written with nothing under it
		// is indistinguishable from `inputs:` absent — which is what keeps Marshal an
		// exact inverse, the same rule `vars:` follows.
		return nil
	}

	return declarations
}

// declaredInput compiles one input declaration.
func (c *compiler) declaredInput(e entry, parent string) *v1.InputDeclaration {
	path := fieldPath(parent, e.name)
	r := ref{path: path, label: "input " + e.name}

	c.pos.record(path, spanOfNode(c.resolveQuiet(e.value)))

	entries, ok := c.entries(e.value, path, r)
	if !ok {
		// The message [compiler.entries] gives — "must be a mapping of keys to
		// values" — names the shape and not the reason, so the shape is spelled out
		// here where the reason is known.
		c.report(spanOfNode(e.value), r,
			"is declared as a mapping saying what a value for it must be: `type:` (one of %s), "+
				"and optionally `required:`, `default:` and `description:`",
			strings.Join(v1.DeclaredTypeNames(), ", "))

		return nil
	}

	// `pattern:` is retired, the same shape [retiredStepKeys] handles a level up
	// but held back here instead of joining that table: what an author writes
	// back is not a fixed replacement word, it depends on the regular
	// expression they wrote `pattern:` with, so the remedy is built from the
	// value in hand rather than looked up. Held back from the key check for the
	// identical reason a retired step key is — "unknown key `pattern`; did you
	// mean...?" sends an author looking for a typo that is not there, when what
	// they need is the `must:` spelling and their own regex, copy-pasteable.
	checkable := make([]entry, 0, len(entries))
	for _, en := range entries {
		switch en.name {
		case "pattern":
			c.report(spanOfNode(en.key), r, "%s", retiredPatternMessage(c.resolveQuiet(en.value)))
		case "min", "max":
			c.report(spanOfNode(en.key), r, "%s", retiredMinMaxMessage(en.name, c.resolveQuiet(en.value)))
		case "unique":
			c.report(spanOfNode(en.key), r, "%s", retiredUniqueMessage)
		default:
			checkable = append(checkable, en)
		}
	}

	fields := c.check(checkable, r, inputKeys)

	declaration := &v1.InputDeclaration{Name: e.name}

	if f, found := fields.get("type"); found {
		typePath := fieldPath(path, "type")
		typeRef := ref{path: typePath, label: "input " + e.name + " type"}
		if text, ok := c.text(f.value, typePath, typeRef); ok {
			declared, known := v1.ParseDeclaredType(text)
			if !known {
				c.report(spanOfNode(f.value), typeRef,
					"is %q, which is not a type an input can have; the types are %s",
					text, strings.Join(v1.DeclaredTypeNames(), ", "))
			}
			declaration.Type = declared
		}
	} else {
		// Required rather than inferred from the default, deliberately: a type
		// inferred from a default would leave an input with no default untyped, and
		// the whole point of declaring one is that a value is checked against it
		// before the run starts.
		c.report(spanOfNode(e.value), r,
			"has no `type:`; an input is checked against its type when a run is submitted, so say which of %s it is",
			strings.Join(v1.DeclaredTypeNames(), ", "))
	}

	if f, found := fields.get("values"); found {
		valuesPath := fieldPath(path, "values")
		declaration.Values = c.enumValues(f.value, valuesPath,
			ref{path: valuesPath, label: "input " + e.name + " values"})
	}

	if f, found := fields.get("required"); found {
		requiredPath := fieldPath(path, "required")
		if required, ok := c.boolean(f.value, requiredPath,
			ref{path: requiredPath, label: "input " + e.name + " required"}); ok {
			declaration.Required = required
		}
	}

	if f, found := fields.get("default"); found {
		defaultPath := fieldPath(path, "default")
		// An ordinary input value, so a default of the literal string
		// "steps.a.result" stays that string — the same fence rule as everywhere
		// else. That a default may not be an *expression* is a fact about the set of
		// things a declaration says, so [Validate] reports it, where the sentence can
		// explain why.
		declaration.Default = c.inputValue(f.value, defaultPath,
			ref{path: defaultPath, label: "input " + e.name + " default"})
	}

	if f, found := fields.get("description"); found {
		descriptionPath := fieldPath(path, "description")
		if description, ok := c.text(f.value, descriptionPath,
			ref{path: descriptionPath, label: "input " + e.name + " description"}); ok {
			declaration.Description = proto.String(description)
		}
	}

	if f, found := fields.get("example"); found {
		examplePath := fieldPath(path, "example")
		// An ordinary value, the same fence rule `default:` follows: an example is
		// never applied at runtime, but it is still checked against the
		// declaration's own type and constraints — see [Validate] — so it stays a
		// value rather than an expression for the identical reason a default does.
		declaration.Example = c.inputValue(f.value, examplePath,
			ref{path: examplePath, label: "input " + e.name + " example"})
	}

	if f, found := fields.get("sensitive"); found {
		sensitivePath := fieldPath(path, "sensitive")
		if sensitive, ok := c.boolean(f.value, sensitivePath,
			ref{path: sensitivePath, label: "input " + e.name + " sensitive"}); ok {
			declaration.Sensitive = sensitive
		}
	}

	if f, found := fields.get("min_len"); found {
		p := fieldPath(path, "min_len")
		if v, ok := c.unsignedWhole(f.value, p, ref{path: p, label: "input " + e.name + " min_len"}); ok {
			declaration.MinLen = proto.Uint64(v)
		}
	}
	if f, found := fields.get("max_len"); found {
		p := fieldPath(path, "max_len")
		if v, ok := c.unsignedWhole(f.value, p, ref{path: p, label: "input " + e.name + " max_len"}); ok {
			declaration.MaxLen = proto.Uint64(v)
		}
	}
	if f, found := fields.get("min_items"); found {
		p := fieldPath(path, "min_items")
		if v, ok := c.unsignedWhole(f.value, p, ref{path: p, label: "input " + e.name + " min_items"}); ok {
			declaration.MinItems = proto.Uint64(v)
		}
	}
	if f, found := fields.get("max_items"); found {
		p := fieldPath(path, "max_items")
		if v, ok := c.unsignedWhole(f.value, p, ref{path: p, label: "input " + e.name + " max_items"}); ok {
			declaration.MaxItems = proto.Uint64(v)
		}
	}
	if f, found := fields.get("must"); found {
		p := fieldPath(path, "must")
		if v, ok := c.text(f.value, p, ref{path: p, label: "input " + e.name + " must"}); ok {
			declaration.Must = proto.String(v)
		}
	}
	return declaration
}

// enumValues compiles an input declaration's `values:` list — the closed set a
// `type: enum` input may hold.
//
// This is the shape half only: whether `values:` belongs here at all (it does
// not, beside anything but `type: enum`) and whether an enum declares at
// least one are set-facts about the *declaration as a whole*, judged in
// [validateInputConstraintShape] once the whole declaration exists to judge —
// the same split the schema's own doc on [v1.InputDeclaration_values] draws
// between what a compiler decides and what [v1.CheckInputConstraintShape]
// decides for a specification that never was a Flowfile. What belongs here is
// only what this one key's own value can be wrong about: a scalar or a
// mapping written where a list belongs.
func (c *compiler) enumValues(n ast.Node, path string, r ref) []string {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	sequence, ok := n.(*ast.SequenceNode)
	if !ok {
		c.report(spanOfNode(n), r,
			"must be a list of the values this input may hold, like [staging, production], but %s was written here",
			describeNode(n))
		return nil
	}

	// An empty list is not a shape mistake this function can see — `values: []`
	// parses as a perfectly good, empty list — so it is left to
	// [validateInputConstraintShape], which reports "enum with no values" the
	// same way whether the key was omitted or written empty, against the
	// declaration as a whole rather than a list with nothing in it to point at.
	values := make([]string, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		elementPath := indexPath(path, i)
		// Resolving an alias can make this same sequence appear under many input
		// declarations. Charge every expanded element, rather than only the YAML
		// node that contains the alias, so a small document cannot make this loop
		// and its position table grow without reaching the document budget.
		if !c.enter(value, ref{path: elementPath, label: r.label}) {
			return values
		}
		if text, ok := c.text(value, elementPath, ref{path: elementPath, label: r.label}); ok {
			values = append(values, text)
		}
		c.exit()
	}
	return values
}

// declaredOutputs compiles the top-level `outputs:` block: one named expression per
// value a finished run reports.
func (c *compiler) declaredOutputs(n ast.Node, path string, r ref) []*v1.OutputDeclaration {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	declarations := make([]*v1.OutputDeclaration, 0, len(entries))
	for _, e := range entries {
		if declaration := c.declaredOutput(e, path); declaration != nil {
			declarations = append(declarations, declaration)
		}
	}

	if len(declarations) == 0 {
		return nil
	}

	return declarations
}

// declaredOutput compiles one output declaration.
//
// The value is written under `value:` rather than as the entry's own scalar, which
// costs a line and buys the room `description:` needs — and keeps the two blocks
// shaped alike, so an author who has written one can write the other.
func (c *compiler) declaredOutput(e entry, parent string) *v1.OutputDeclaration {
	path := fieldPath(parent, e.name)
	r := ref{path: path, label: "output " + e.name}

	c.pos.record(path, spanOfNode(c.resolveQuiet(e.value)))

	fields, ok := c.fields(e.value, path, r, outputKeys)
	if !ok {
		c.report(spanOfNode(e.value), r,
			"is declared as a mapping with `value:`, the expression that produces it, "+
				"and optionally `description:`")

		return nil
	}

	declaration := &v1.OutputDeclaration{Name: e.name}

	if f, found := fields.get("value"); found {
		valuePath := fieldPath(path, "value")
		// An expression field, so the fence is optional here the way it is on `if:`
		// and a loop's `items:`: the schema knows this is an expression, and a string
		// in it is expression source.
		declaration.Value = c.exprValue(f.value, valuePath,
			ref{path: valuePath, label: "output " + e.name + " value"})
	} else {
		c.report(spanOfNode(e.value), r,
			"has no `value:`; an output is the expression that produces it, evaluated once the steps have finished")
	}

	if f, found := fields.get("description"); found {
		descriptionPath := fieldPath(path, "description")
		if description, ok := c.text(f.value, descriptionPath,
			ref{path: descriptionPath, label: "output " + e.name + " description"}); ok {
			declaration.Description = proto.String(description)
		}
	}

	if f, found := fields.get("must"); found {
		p := fieldPath(path, "must")
		if v, ok := c.text(f.value, p, ref{path: p, label: "output " + e.name + " must"}); ok {
			declaration.Must = proto.String(v)
		}
	}
	if f, found := fields.get("sensitive"); found {
		p := fieldPath(path, "sensitive")
		if v, ok := c.boolean(f.value, p, ref{path: p, label: "output " + e.name + " sensitive"}); ok {
			declaration.Sensitive = v
		}
	}

	return declaration
}

// vars compiles a `vars:` mapping of names to values.
//
// Each value is an ordinary input value, which is what keeps the fence rule uniform:
// `${...}` is an expression and anything else is a literal, here as everywhere. The
// fence is *required* for an expression rather than inferred, and this is exactly the
// position that shows why — a var legitimately holds the literal string
// "steps.greet.result", so a bare scalar that happens to look like a reference has to
// stay a string.
//
// Returns nil rather than an empty map when nothing compiled, so that `vars:` written
// with nothing under it is indistinguishable from `vars:` absent — which is what makes
// Marshal an exact inverse.
func (c *compiler) vars(n ast.Node, path string, r ref) map[string]*v1.Value {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	compiled := make(map[string]*v1.Value, len(entries))
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		if value := c.varValue(e.value, valuePath, ref{path: valuePath, label: "vars." + e.name}); value != nil {
			compiled[e.name] = value
		}
	}

	if len(compiled) == 0 {
		return nil
	}

	return compiled
}

// checkDeclaredEdition reads an `edition:` key if one was written and reports
// whether the document may be compiled.
//
// Takes the raw entries rather than a checked field set so that it can run before
// unknown-key reporting — see the call site.
func (c *compiler) checkDeclaredEdition(entries []entry) bool {
	for _, e := range entries {
		if e.name != "edition" {
			continue
		}
		r := ref{path: "edition", label: "edition"}
		declared, ok := editionText(c.resolve(e.value, "edition", r))
		if !ok {
			c.report(spanOfNode(e.value), r,
				"edition must be written as %s, but %s was written here",
				CurrentEdition, describeNode(e.value))
			return false
		}
		c.pos.record("edition", spanOfNode(e.value))
		if err := checkEdition(declared); err != nil {
			c.report(spanOfNode(e.value), r, "%s", err.Error())
			return false
		}
		return true
	}

	// Reported against the document rather than against a position, because there is
	// nothing in the source to point at: the key was never written. The message says
	// where it goes and what to write there.
	c.report(Span{}, ref{path: "edition", label: "edition"}, "%s", missingEdition())

	// Reported, and then compiled anyway — unlike a *declared* edition this build does
	// not know.
	//
	// The abort exists because a file claiming another grammar makes every other
	// diagnostic describe the wrong language: `nonsense:` is an unknown key here and
	// might be a perfectly good key there. A file that declares nothing is not that. It
	// is almost always this grammar with a line missing, so the rest of what is wrong
	// with it is worth reading — and stopping would answer "you forgot a line" to
	// someone whose file has four other problems, one ceremonial diagnostic at a time.
	return true
}

// collectAnchors records every anchor in the document so that an alias can be
// resolved wherever it appears.
func (c *compiler) collectAnchors(n ast.Node) {
	switch node := n.(type) {
	case nil:
		return
	case *ast.AnchorNode:
		if name := node.Name; name != nil {
			c.anchors[name.String()] = node.Value
		}
		c.collectAnchors(node.Value)
	case *ast.MappingNode:
		for _, v := range node.Values {
			c.collectAnchors(v)
		}
	case *ast.MappingValueNode:
		c.collectAnchors(node.Key)
		c.collectAnchors(node.Value)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			c.collectAnchors(v)
		}
	case *ast.TagNode:
		c.collectAnchors(node.Value)
	}
}

// checkAnchorCycles reports an anchor that is part of its own value, and returns
// whether the document is free of them.
//
// A Flowfile is a tree, so an alias pointing back into the value it names is
// meaningless — and following one is unbounded. Detecting it once here is what lets
// every other walk resolve an alias without carrying a visited set.
func (c *compiler) checkAnchorCycles() bool {
	// The graph is anchor name to the anchors its value refers to.
	edges := make(map[string][]string, len(c.anchors))
	for name, value := range c.anchors {
		referenced := make(map[string]bool)
		collectAliases(value, referenced)
		edges[name] = slices.Sorted(maps.Keys(referenced))
	}

	state := make(map[string]int, len(edges)) // 0 unvisited, 1 in progress, 2 done
	var cyclic func(name string) bool
	cyclic = func(name string) bool {
		switch state[name] {
		case 1:
			return true
		case 2:
			return false
		}
		state[name] = 1
		for _, next := range edges[name] {
			if cyclic(next) {
				return true
			}
		}
		state[name] = 2
		return false
	}

	ok := true
	for _, name := range slices.Sorted(maps.Keys(c.anchors)) {
		if cyclic(name) {
			c.report(spanOfNode(c.anchors[name]), ref{path: "anchor &" + name},
				"anchor &%s is part of its own value; an alias cannot refer to the value it appears in", name)
			ok = false
		}
	}
	return ok
}

// collectAliases records the anchor names a node's subtree refers to.
func collectAliases(n ast.Node, into map[string]bool) {
	switch node := n.(type) {
	case nil:
		return
	case *ast.AliasNode:
		into[node.Value.String()] = true
	case *ast.AnchorNode:
		collectAliases(node.Value, into)
	case *ast.MappingNode:
		for _, v := range node.Values {
			collectAliases(v, into)
		}
	case *ast.MappingValueNode:
		collectAliases(node.Key, into)
		collectAliases(node.Value, into)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			collectAliases(v, into)
		}
	case *ast.TagNode:
		collectAliases(node.Value, into)
	}
}

// steps compiles a list of steps.
func (c *compiler) steps(n ast.Node, path string, r ref) []*v1.Node {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	sequence, ok := n.(*ast.SequenceNode)
	if !ok {
		c.report(spanOfNode(n), r, "must be a list of steps, each with an id and one of %s", stepKindList())
		return nil
	}
	if len(sequence.Values) == 0 {
		c.report(spanOfNode(n), r, "must have at least one step")
		return nil
	}

	nodes := make([]*v1.Node, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		nodes = append(nodes, c.step(value, indexPath(path, i)))
	}
	return nodes
}

// step compiles one step: its id, the one kind of work it does, and the policy
// controlling how it runs.
func (c *compiler) step(n ast.Node, path string) *v1.Node {
	n = c.resolve(n, path, ref{path: path})
	if n == nil || !c.enter(n, ref{path: path}) {
		return nil
	}
	defer c.exit()

	span := spanOfNode(n)
	c.pos.record(path, span)

	entries, ok := c.entries(n, path, ref{path: path})
	if !ok {
		return nil
	}

	// The id is read before anything else so that every diagnostic below can name
	// the step, which is how an author finds it in a file of thirty — including a
	// diagnostic about a key written above the id.
	step := &v1.Node{}
	for _, e := range entries {
		if e.name == "id" {
			id, _ := c.text(e.value, fieldPath(path, "id"), ref{path: fieldPath(path, "id"), label: "id"})
			step.Id = id
			c.pos.recordStep(id, path)
			break
		}
	}

	r := ref{step: step.GetId(), path: path}

	// Which key names the task is [StepTaskKeys]'s question, asked here and by the
	// language server so that both place a diagnostic on the same token.
	//
	// An unregistered name it returns — `shell:` — is accepted as a key here and
	// compiled as a task, so the *validator* reports it: "unknown task; available
	// tasks are ..." is a better message than "unknown key", and it belongs where
	// task names are known.
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.name
	}
	promoted := StepTaskKeys(names)

	// stepKeys already holds every *registered* name, so only an unregistered one
	// is news here. Appending the whole promoted set would list `echo` twice in
	// "the keys here are ...", which reads like a bug in the tool.
	known := stepKeys()
	for _, name := range promoted {
		if !slices.Contains(known, name) {
			known = append(known, name)
		}
	}
	kindKeys := append(slices.Clone(nodeKindKeys), promoted...)

	// A retired spelling is reported here and then held back from the key check,
	// so that the message naming its replacement is the only thing said about it.
	//
	// Held back rather than accepted, because the count of checked keys is what
	// tells the kind check below that a key was already rejected — and a step
	// written the old way has no kind, so without this it would also be told it
	// does nothing, which is the same mistake reported a second time in worse
	// words.
	checkable := make([]entry, 0, len(entries))
	for _, e := range entries {
		if instead, retired := retiredStepKeys[e.name]; retired {
			c.report(spanOfNode(e.key), r, "`%s:` is no longer a step key; %s", e.name, instead)
			continue
		}
		checkable = append(checkable, e)
	}
	checkable = c.heldForLater(checkable, r, stepPropertyKeys)

	fields := c.check(checkable, r, known)

	// Collected in the order they were *written*, not in the order of a canonical
	// list. It only matters when there are two, and then it is what makes the
	// diagnostic point at the second one — the key the author added to a step that
	// already did something.
	var kinds []field
	for _, f := range fields.list {
		if slices.Contains(kindKeys, f.name) {
			kinds = append(kinds, f)
		}
	}

	switch len(kinds) {
	case 0:
		// Silent when a key was already rejected, because that key is almost
		// certainly what should have been the kind — a misspelled `htpp:`, an
		// expression written where a key goes. Reporting "this has no kind of
		// work" as well says the same mistake twice and buries the one that names
		// the token at fault.
		if len(fields.list) == len(entries) {
			c.report(span, r, "must have one of %s; a step has to do something", stepKindList())
		}
	case 1:
		kind := kinds[0]
		kindPath := fieldPath(path, kind.name)

		// What `steps[N].<kind>` addresses depends on the kind.
		//
		// For a task it is the *key*: under the flattening the key is the task's
		// name, so a problem with the task is a problem with that word, and
		// `unknown task "shell"` wants to underline `shell` rather than the six
		// inputs an author wrote correctly beneath it. For every other kind the key
		// is a fixed grammar word nobody can get wrong and the interesting extent is
		// what was written under it.
		//
		// Recorded once, here, rather than by each arm: two writes to one path leave
		// the answer decided by call order, which is how this briefly addressed a
		// task's inputs while appearing to address its name.
		if slices.Contains(nodeKindKeys, kind.name) {
			c.pos.record(kindPath, spanOfNode(kind.value))
		} else {
			c.pos.record(kindPath, spanOfNode(kind.key))
		}

		switch kind.name {
		case "for_each":
			if loop := c.forEach(kind.value, kindPath, r); loop != nil {
				step.Kind = &v1.Node_ForEach{ForEach: loop}
			}
		case "loop":
			if loop := c.loop(kind.value, kindPath, r); loop != nil {
				step.Kind = &v1.Node_Loop{Loop: loop}
			}
		case "parallel":
			if parallel := c.parallel(kind.value, kindPath, r); parallel != nil {
				step.Kind = &v1.Node_Parallel{Parallel: parallel}
			}
		case "sleep":
			if wait := c.sleep(kind.value, kindPath, r); wait != nil {
				step.Kind = &v1.Node_Wait{Wait: wait}
			}
		case "wait_until":
			if wait := c.waitUntil(kind.value, kindPath, r); wait != nil {
				step.Kind = &v1.Node_Wait{Wait: wait}
			}
		case "wait_for_signal":
			if wait := c.waitForSignal(kind.value, kindPath, r); wait != nil {
				step.Kind = &v1.Node_Wait{Wait: wait}
			}
		case "wait_for_signals":
			if wait := c.waitForSignals(kind.value, kindPath, r); wait != nil {
				step.Kind = &v1.Node_Wait{Wait: wait}
			}
		case "switch":
			if sw := c.switchNode(kind.value, kindPath, r); sw != nil {
				step.Kind = &v1.Node_Switch{Switch: sw}
			}
		case "value":
			// The same fence-optional reading the workflow's own `outputs:` gives
			// its `value:`, because it is the same thing in a second position: the
			// schema knows this is an expression, so a bare string here is
			// expression source rather than text.
			step.Kind = &v1.Node_Value{Value: c.exprValue(kind.value, kindPath, ref{
				step:  step.GetId(),
				path:  kindPath,
				label: "value",
			})}

		case "call":
			withField, hasWith := fields.get("with")
			digestField, hasDigest := fields.get("digest")
			if call := c.call(kind.value, path, kindPath, r, withField, hasWith, digestField, hasDigest); call != nil {
				step.Kind = &v1.Node_Call{Call: call}
			}
		default:
			// A task: the key is its name, the value is its inputs. That is the
			// whole of the flattening — what used to be three levels of scaffolding
			// is the one fact a reader wanted.
			step.Kind = &v1.Node_Task{Task: c.task(kind.name, kind.value, kindPath, r)}
		}
	default:
		c.report(spanOfNode(kinds[1].key), r,
			"has both %s and %s; a step does exactly one kind of work, so split it into two steps",
			kinds[0].name, kinds[1].name)
	}

	// `with:` binds a callee's declared inputs, so it means nothing beside any
	// other kind of work — the same way `steps:` means nothing beside a task.
	// Reported here rather than silently accepted, for the reason every unknown
	// or misplaced key is: an author who moved a call's arguments onto some
	// other step gets no signal that they no longer bind anything.
	//
	// `digest:` is the same rule with a sharper edge. It is a *check*, and a
	// check nothing performs reads to whoever wrote it exactly like one that
	// passed, so a pin sitting on a step with no `call:` under it is the one
	// misplaced key that must never be quietly tolerated.
	if len(kinds) != 1 || kinds[0].name != "call" {
		if f, found := fields.get("with"); found {
			c.report(spanOfNode(f.key), r,
				"`with:` binds a called workflow's declared inputs and is only meaningful "+
					"beside `call:`, which this step does not have")
		}
		if f, found := fields.get("digest"); found {
			c.report(spanOfNode(f.key), r,
				"`digest:` pins the file a `call:` reads to the bytes it had when the pin was "+
					"written, and is only meaningful beside `call:`, which this step does not have; "+
					"nothing would verify it here, and a pin nobody checks reads like one that passed")
		}
	}

	if f, found := fields.get("if"); found {
		condition := ref{step: step.GetId(), path: fieldPath(path, "if"), label: "if"}
		step.Condition = c.exprValue(f.value, fieldPath(path, "if"), condition)
	}

	// Set only when written, so that "no description" and "an empty description"
	// stay distinguishable — the same rule the workflow's own description follows,
	// and what keeps Marshal an exact inverse.
	if f, found := fields.get("description"); found {
		descriptionPath := fieldPath(path, "description")
		if description, ok := c.text(f.value, descriptionPath, ref{step: step.GetId(), path: descriptionPath, label: "description"}); ok {
			step.Description = proto.String(description)
		}
	}

	// The step's own bare names. Read before the policy only because a reader meets
	// them in that order; nothing here depends on the ordering.
	if f, found := fields.get("vars"); found {
		varsPath := fieldPath(path, "vars")
		step.Vars = c.vars(f.value, varsPath, ref{step: step.GetId(), path: varsPath, label: "vars"})
	}

	// Read after the work, because that is the order it is written and the order it
	// is read: what the step does, then how to take it back.
	if f, found := fields.get("undo"); found {
		undoPath := fieldPath(path, "undo")
		c.pos.record(undoPath, spanOfNode(f.key))
		step.Undo = c.undo(f.value, undoPath, r)
	}

	// Read where it is written, above the work: `async: true` says how this step
	// relates to the ones around it, which is a thing to know before reading what
	// it does. Set only when true, so that a step spelling out the default is
	// equal to one that says nothing — the same rule `continue_on_error:` follows,
	// and what keeps Marshal an exact inverse.
	if f, found := fields.get("async"); found {
		async, ok := c.boolean(f.value, fieldPath(path, "async"),
			ref{step: step.GetId(), path: fieldPath(path, "async"), label: "async"})
		if ok {
			step.Async = async
		}
	}

	step.Policy = c.policy(fields, path, r)
	c.checkPolicyPlacement(step, fields, path, r)

	return step
}

// undo compiles a step's compensation: one task, named directly, with its inputs
// beneath — the same shape a step's own work has.
//
// The same shape deliberately, because it is the same kind of thing: `http:` under
// `undo:` reads as "the http request that undoes this" without anybody learning a
// second spelling for naming a task. What it is *not* is a step — no `id`, no
// `if:`, no `retry:`, and no second `undo:` — and that narrowing is the schema's
// (see [v1.Compensation]) rather than a set of keys refused here.
//
// So the whole of the grammar is: exactly one key, which must name a task.
func (c *compiler) undo(n ast.Node, path string, r ref) *v1.Compensation {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}

	if _, empty := n.(*ast.NullNode); empty {
		c.report(spanOfNode(n), r,
			"`undo:` must name the task that takes this step back, with its inputs beneath it, "+
				"the same shape as the step's own work")

		return nil
	}

	entries, ok := c.entries(n, path, ref{step: r.step, path: path, label: "undo"})
	if !ok {
		return nil
	}
	if len(entries) == 0 {
		c.report(spanOfNode(n), r,
			"`undo:` must name the task that takes this step back, with its inputs beneath it, "+
				"the same shape as the step's own work")

		return nil
	}
	if len(entries) > 1 {
		// Positioned on the second key, which is the one that was added to something
		// already complete — the same rule a step with two kinds of work follows.
		c.report(spanOfNode(entries[1].key), r,
			"`undo:` names both %s and %s; a compensation is a single task, so if taking this "+
				"step back needs two of them, make the second one a step of its own with its own `undo:`",
			entries[0].name, entries[1].name)
	}

	e := entries[0]
	if instead, retired := retiredStepKeys[e.name]; retired {
		c.report(spanOfNode(e.key), r, "`%s:` is no longer a step key; %s", e.name, instead)

		return nil
	}
	if slices.Contains(nodeKindKeys, e.name) {
		c.report(spanOfNode(e.key), r,
			"`%s:` is control flow rather than a task, and a compensation is a single task; "+
				"a block of work that has to be undone belongs in steps that each say how they are undone",
			e.name)

		return nil
	}
	if !couldBeATaskName(e.name) {
		c.report(spanOfNode(e.key), r,
			"`undo:` must name a task, and %q is not spelled the way a task name is; "+
				"a compensation cannot be chosen at run time", e.name)

		return nil
	}

	// The key is the task's name, so a diagnostic about the task underlines that
	// word — the same addressing a step's own task key gets.
	taskPath := fieldPath(path, e.name)
	c.pos.record(taskPath, spanOfNode(e.key))

	return &v1.Compensation{Task: c.task(e.name, e.value, taskPath, r)}
}

// task compiles a task step: the key is the task's name, the value is its inputs.
//
// A task written with no value at all — `echo:` on a line by itself — is a task
// with no inputs rather than a mistake. Whether that is *legal* is the task's
// question, not the grammar's: the registry declares which inputs are required
// and [Validate] answers from the schema, where a reader can see it written down.
func (c *compiler) task(name string, n ast.Node, path string, r ref) *v1.Task {
	task := &v1.Task{Name: name, Inputs: map[string]*v1.Value{}}
	c.inputs(n, path, r, name, task.Inputs)
	return task
}

// inputs compiles a task's inputs into the given map.
//
// Input names are whatever the task declares, so unlike everywhere else there is
// no set of known keys to check against; the registry's descriptors are what
// [Validate] and the language server check names against.
//
// The task's own path is recorded by its caller, which knows the key, and not
// here from the value: see the task case of [compiler.stepNode].
func (c *compiler) inputs(n ast.Node, path string, r ref, taskName string, into map[string]*v1.Value) {
	n = c.resolve(n, path, r)
	if n == nil {
		return
	}

	if _, empty := n.(*ast.NullNode); empty {
		return
	}

	entries, ok := c.entries(n, path, ref{step: r.step, path: path, label: "inputs"})
	if !ok {
		return
	}

	// A task's inputs are its inputs, with nothing flattened on the way through.
	//
	// One task used to have a `vars:` input emptied into the inputs around it here,
	// because it bound every key it did not recognise as a variable and needed each
	// resolved separately. It retired at edition v2026.2. `vars:` is a *step* key
	// now, at a level above this one, and a `vars:` written among a task's inputs is
	// an input by that name — reported as unknown, where it was written.
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		inputRef := ref{step: r.step, task: taskName, input: e.name, path: valuePath}

		// The one input compiled by a rule of its own, and the rule is the wait's:
		// a mapping written where a task shapes its outputs is compiled entry by
		// entry, so the names survive into the specification instead of being
		// erased into one expression that builds a map. See
		// [compiler.shapedOutputs].
		if e.name == v1.ShapingInput && v1.TaskShapesOutputs(taskName) {
			if value := c.shapedOutputs(e.value, e.key, valuePath, inputRef); value != nil {
				into[e.name] = value
			}
			continue
		}

		value := c.inputValue(e.value, valuePath, inputRef)
		if value != nil {
			into[e.name] = value
		}
	}
}

// shapedOutputs compiles a shaping task's `outputs:` input.
//
// Written as a mapping, it is compiled per entry — the same shape
// [compiler.waitOutputs] produces for a `wait_for_signal:`, and for the same
// reason: the names a step produces are the point of shaping, and a mapping run
// through [compiler.composite] would arrive at the engine as a single expression
// that happens to build a map, with its keys knowable only once it has run. Every
// later question — does `${steps.web.titel}` name something this step produces,
// what should the editor offer after `steps.web.` — is answerable exactly when
// the keys survive compilation.
//
// Written any other way, it is an ordinary task input. The string-fenced map
// (`'${ {"id": response.json.id} }'`) still compiles and still runs; what it
// cannot do is say what it produces, so nothing downstream says anything about
// it. That is the documented trade, and it is why the mapping form is the
// spelling the docs and `flow fix` lead with.
func (c *compiler) shapedOutputs(n, key ast.Node, path string, r ref) *v1.Value {
	resolved := c.resolveQuiet(n)
	switch resolved.(type) {
	case *ast.MappingNode, *ast.MappingValueNode:
	default:
		// Not a mapping: a fenced expression, a literal, an alias this cannot
		// follow. All of them are ordinary input values, decided by the rule every
		// other input is decided by.
		return c.inputValue(n, path, r)
	}

	// The mapping's own span, recorded before anything below can fail, so a
	// diagnostic about the block — an empty one, a computed key — has a line to
	// land on. [compiler.value] records this for every other input on the way
	// past; this branch does not go through it.
	//
	// The key's span is the fallback, and it is not hypothetical: `outputs: {}`
	// is a flow mapping holding no tokens at all, so the value has no position
	// and the only thing on the line that does is the key.
	at := spanOfNode(resolved)
	if !at.IsValid() {
		at = spanOfNode(key)
	}
	c.pos.record(path, at)

	// A structure holding a `${secret(...)}` is refused here exactly as it is
	// refused when this mapping is compiled as an ordinary input: shaping is
	// evaluated against a response and recorded as a step output, which is
	// durable history, and [compiler.structure] already says so with the position
	// and the list of inputs that do carry one.
	if c.holdsSecretMarker(resolved) {
		return c.structure(resolved, path, r)
	}

	entries, ok := c.entries(resolved, path, r)
	if !ok {
		return nil
	}

	if len(entries) == 0 {
		c.report(at, r,
			"is empty, and `outputs:` replaces what the step produces, so this step would have no outputs at all; write the names it should produce, or remove the key")
		return nil
	}

	compiled := make(map[string]*v1.Value, len(entries))
	for _, e := range entries {
		if fenceError(e.name) != nil || containsFence(e.name) {
			// A key is a name, and a name is written down. A computed one would be
			// a shaped set only the run knows, which is the one thing the mapping
			// form exists to rule out — and the escape hatch is exactly the older
			// spelling, so the refusal can name it.
			c.report(spanOfNode(e.key), r,
				"`%s` computes an output name, and the names a step produces have to be written down; "+
					"write the name plainly, or shape with a single expression "+
					"(`outputs: '${ {...} }'`), which is legal and deliberately unchecked",
				e.name)
			return nil
		}

		valuePath := fieldPath(path, e.name)
		value := c.inputValue(e.value, valuePath,
			ref{step: r.step, task: r.task, input: r.input, path: valuePath,
				label: v1.ShapingInput + "." + e.name})
		if value == nil {
			return nil
		}
		compiled[e.name] = value
	}

	return v1.NewStructureMap(compiled)
}

// forEach compiles a loop and its body.
func (c *compiler) forEach(n ast.Node, path string, r ref) *v1.ForEach {
	fields, ok := c.fields(n, path, r, forEachKeys)
	if !ok {
		return nil
	}

	loop := &v1.ForEach{}

	if f, found := fields.get("items"); found {
		itemsPath := fieldPath(path, "items")
		loop.Items = c.exprValue(f.value, itemsPath,
			ref{step: r.step, path: itemsPath, label: "for_each items"})
	} else {
		c.report(spanOfNode(n), r, "for_each requires items, an expression producing the list to iterate over")
	}

	if f, found := fields.get("as"); found {
		iterator, _ := c.text(f.value, fieldPath(path, "as"),
			ref{step: r.step, path: fieldPath(path, "as"), label: "for_each as"})
		loop.Iterator = iterator
	}

	if f, found := fields.get("max_parallel"); found {
		maxParallel, _ := c.integer(f.value, fieldPath(path, "max_parallel"),
			ref{step: r.step, path: fieldPath(path, "max_parallel"), label: "for_each max_parallel"}, 0, 1000)
		loop.MaxParallel = maxParallel
	}

	if f, found := fields.get(stepsKey); found {
		loop.Body = c.steps(f.value, fieldPath(path, "steps"),
			ref{step: r.step, path: fieldPath(path, "steps"), label: "for_each steps"})
	} else {
		c.report(spanOfNode(n), r, "for_each requires steps, the body to run for each item")
	}

	return loop
}

// loop compiles a `loop:` and its body.
//
// The shape is close to [compiler.forEach] on purpose — a body under `steps:`, a
// bare binding under `as:` — with the differences a loop needs: `until:` is the stop
// condition (an expression, like `if:`), `max_iterations:` the ceiling, and
// `init:`/`update:` the carried value's endpoints. The parser requires the two a
// loop cannot mean anything without — a body and a stop condition — and leaves the
// consistency of the state triple to [validateNamedLoop], where a positioned
// diagnostic can name which of the three is missing.
func (c *compiler) loop(n ast.Node, path string, r ref) *v1.Loop {
	fields, ok := c.fields(n, path, r, loopKeys)
	if !ok {
		return nil
	}

	loop := &v1.Loop{}

	if f, found := fields.get(stepsKey); found {
		loop.Body = c.steps(f.value, fieldPath(path, "steps"),
			ref{step: r.step, path: fieldPath(path, "steps"), label: "loop steps"})
	} else {
		c.report(spanOfNode(n), r, "loop requires steps, the body to run each iteration")
	}

	if f, found := fields.get("until"); found {
		untilPath := fieldPath(path, "until")
		loop.Until = c.exprValue(f.value, untilPath,
			ref{step: r.step, path: untilPath, label: "loop until"})
	} else {
		c.report(spanOfNode(n), r, "loop requires until, the condition that stops it: a loop with no bound on when it ends never ends")
	}

	if f, found := fields.get("max_iterations"); found {
		maxIterations, _ := c.integer(f.value, fieldPath(path, "max_iterations"),
			ref{step: r.step, path: fieldPath(path, "max_iterations"), label: "loop max_iterations"}, 0, 100000)
		loop.MaxIterations = maxIterations
	}

	if f, found := fields.get("as"); found {
		state, _ := c.text(f.value, fieldPath(path, "as"),
			ref{step: r.step, path: fieldPath(path, "as"), label: "loop as"})
		loop.State = state
	}

	if f, found := fields.get("init"); found {
		initPath := fieldPath(path, "init")
		loop.Initial = c.loopStateValue(f.value, initPath,
			ref{step: r.step, path: initPath, label: "loop init"})
	}

	if f, found := fields.get("update"); found {
		updatePath := fieldPath(path, "update")
		loop.Update = c.loopStateValue(f.value, updatePath,
			ref{step: r.step, path: updatePath, label: "loop update"})
	}

	return loop
}

// parallel compiles the branches of a parallel step.
func (c *compiler) parallel(n ast.Node, path string, r ref) *v1.Parallel {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}

	sequence, ok := n.(*ast.SequenceNode)
	if !ok {
		c.report(spanOfNode(n), r, "parallel must be a list of branches, each a mapping with its own steps")
		return nil
	}
	if len(sequence.Values) == 0 {
		c.report(spanOfNode(n), r, "parallel must have at least one branch")
		return nil
	}

	parallel := &v1.Parallel{Branches: make([]*v1.Parallel_Branch, 0, len(sequence.Values))}
	for i, value := range sequence.Values {
		branchPath := indexPath(path, i)
		c.pos.record(branchPath, spanOfNode(c.resolveQuiet(value)))

		branch := &v1.Parallel_Branch{}
		fields, ok := c.fields(value, branchPath, ref{step: r.step, path: branchPath}, branchKeys)
		if ok {
			if f, found := fields.get(stepsKey); found {
				branch.Steps = c.steps(f.value, fieldPath(branchPath, "steps"),
					ref{step: r.step, path: fieldPath(branchPath, "steps"), label: fmt.Sprintf("parallel branch %d steps", i+1)})
			} else {
				c.report(spanOfNode(value), ref{step: r.step, path: branchPath},
					"parallel branch %d requires steps", i+1)
			}
		}
		parallel.Branches = append(parallel.Branches, branch)
	}
	return parallel
}

// policy compiles a step's execution settings.
//
// It returns nil when the step declares none, so that the engine's defaults apply
// rather than a policy of zeroes that happens to mean the same thing today.
func (c *compiler) policy(fields *fieldSet, path string, r ref) *v1.StepPolicy {
	policy := &v1.StepPolicy{}
	declared := false

	if f, found := fields.get("timeout"); found {
		timeout, ok := c.duration(f.value, fieldPath(path, "timeout"),
			ref{step: r.step, path: fieldPath(path, "timeout"), label: "timeout"})
		if ok {
			policy.Timeout = timeout
			declared = true
		}
	}

	if f, found := fields.get("retry"); found {
		if retry := c.retry(f.value, fieldPath(path, "retry"), r); retry != nil {
			policy.Retry = retry
			declared = true
		}
	}

	if f, found := fields.get("continue_on_error"); found {
		continueOnError, ok := c.boolean(f.value, fieldPath(path, "continue_on_error"),
			ref{step: r.step, path: fieldPath(path, "continue_on_error"), label: "continue_on_error"})
		if ok {
			policy.ContinueOnError = continueOnError
			// A step that only says continue_on_error: false has said nothing: it
			// asked for the default. Recording a policy for it would make two
			// identical workflows unequal depending on whether one spelled the
			// default out.
			declared = declared || continueOnError
		}
	}

	if !declared {
		return nil
	}
	return policy
}

// retry compiles a retry policy.
func (c *compiler) retry(n ast.Node, path string, r ref) *v1.RetryPolicy {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	// `retry:` with nothing under it asks for the engine's retry defaults, which is
	// a legitimate thing to write.
	if _, empty := n.(*ast.NullNode); empty {
		return &v1.RetryPolicy{}
	}

	fields, ok := c.fields(n, path, ref{step: r.step, path: path, label: "retry"}, retryKeys)
	if !ok {
		return nil
	}

	retry := &v1.RetryPolicy{}

	if f, found := fields.get("attempts"); found {
		attempts, _ := c.integer(f.value, fieldPath(path, "attempts"),
			ref{step: r.step, path: fieldPath(path, "attempts"), label: "retry attempts"}, 0, 1<<31-1)
		retry.MaxAttempts = attempts
	}
	if f, found := fields.get("interval"); found {
		interval, ok := c.duration(f.value, fieldPath(path, "interval"),
			ref{step: r.step, path: fieldPath(path, "interval"), label: "retry interval"})
		if ok {
			retry.InitialInterval = interval
		}
	}
	if f, found := fields.get("backoff"); found {
		backoff, _ := c.number(f.value, fieldPath(path, "backoff"),
			ref{step: r.step, path: fieldPath(path, "backoff"), label: "retry backoff"})
		retry.BackoffCoefficient = backoff
	}
	if f, found := fields.get("max_interval"); found {
		maxInterval, ok := c.duration(f.value, fieldPath(path, "max_interval"),
			ref{step: r.step, path: fieldPath(path, "max_interval"), label: "retry max_interval"})
		if ok {
			retry.MaxInterval = maxInterval
		}
	}

	return retry
}

// recordTree records the span of a value and of everything nested inside it, so
// that a diagnostic about one entry of a map of headers can point at that entry.
func (c *compiler) recordTree(n ast.Node, path string) {
	n = c.resolveQuiet(n)
	if n == nil || !c.enter(n, ref{path: path}) {
		return
	}
	defer c.exit()
	c.pos.record(path, spanOfNode(n))

	switch node := n.(type) {
	case *ast.SequenceNode:
		for i, v := range node.Values {
			c.recordTree(v, indexPath(path, i))
		}
	case *ast.MappingNode:
		for _, v := range node.Values {
			if name, ok := v.Key.(*ast.StringNode); ok {
				c.recordTree(v.Value, fieldPath(path, name.Value))
			}
		}
	case *ast.MappingValueNode:
		if name, ok := node.Key.(*ast.StringNode); ok {
			c.recordTree(node.Value, fieldPath(path, name.Value))
		}
	}
}

// recordExpr records the span of an expression's own source text.
func (c *compiler) recordExpr(path string, span Span) {
	c.pos.recordExpr(path, span)
}
