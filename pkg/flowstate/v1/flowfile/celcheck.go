package flowfile

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Expressions in a Flowfile were parsed and never checked.
//
// A compiled workflow carries a `ParsedExpr`, and evaluation runs `env.Program` on
// it directly — cel-go's type checker is not in the path anywhere, in either
// driver. So `flow validate` said `ok` to every one of these:
//
//	${nosuchfunc(1)}          a function that does not exist
//	${1 + 'a'}                an addition with no overload
//	${size(1)}                a call with the wrong argument type
//	${string({'a': 1})}       the one that shipped, in an example
//
// Each fails at run time, every time, with nothing about the file changing in
// between. That is the definition of something a validator should have caught: the
// answer is knowable from the document alone.
//
// # Why this cannot produce a false diagnostic about scope
//
// The obvious way to type-check would be to build the environment a step will
// actually see, from the same model the reference walk uses. That would tie this to
// the accuracy of that model, and it would report an unknown name twice — once
// here, in cel-go's words, and once there, in a sentence written for the author.
//
// So it does the opposite. Every identifier the expression *mentions* is declared,
// as `dyn`, before the check runs. Scope stops mattering: a reference the file
// should not be allowed to make is still declared here, and still reported by
// [validateInputRefs], which is where that question belongs and where the better
// message lives. What survives is only what remains wrong once every name is
// assumed to exist and to be of any type — a missing overload, a wrong arity, a
// function nobody declared.
//
// `dyn` is also what makes this quiet where it must be. `string(steps.web.body)` is
// unknowable — the response is not in the file — and `dyn` is compatible with every
// overload, so nothing is reported. Only an expression whose types are *in the
// document* can fail here, which is exactly the set that can be judged.
//
// Measured over the shipped corpus before it was written: 60 expressions across 19
// examples, zero reported. `TestTypeCheckingIsQuietOnTheCorpus` keeps that true.
//
// # Why only here
//
// The other two CEL surfaces already do this. An egress rule and an auth policy are
// both `env.Compile`d — parsed *and* checked — when the configuration loads, which is
// what "rules compile and type-check when configuration loads rather than when a
// request arrives" means in CLAUDE.md. Each has its own environment, deliberately:
// they are separate languages over their own variables, and giving a policy the
// workflow libraries would widen a surface whose whole point is to be narrow.
//
// So the Flowfile was the one place that parsed without checking, and it was the one
// place an author writes the most. Swept for rather than assumed: `cel.NewEnv` and
// `Evaluator.Env` between them have no other caller that evaluates an expression
// somebody wrote in a workflow.
//
// # What it therefore does not cover
//
// A deferred input is checked like any other, because scope is irrelevant here:
// `expect: ${response.status_code == 200}` declares `response` as `dyn` and passes.
// But it also means an expression made entirely of references is never judged, and
// most real expressions are. This catches the knowable half and says nothing about
// the rest, which is the correct division — the other half is what running does.

// checkExpressionTypes reports expressions that cannot evaluate whatever the file
// means, across every position the language puts one.
//
// Walked here in one pass rather than threaded through the per-position reference
// checks, because this needs none of what they carry — no scope, no step index, no
// workflow.
//
// The positions come from [v1.WalkWorkflow], which is the whole of #508's point.
// "A position added to the language gets this for free, or it does not get it at
// all and the gap is in one visible place" is what this said before, and the gap
// was neither free nor visible: #491 added a webhook's `with:` arguments and its
// `idempotency_key`, nothing added them here, and `${nosuchfunc(event.body)}` in a
// trigger validated clean and then failed at three in the morning when a delivery
// arrived (#502). Now every position arrives here whether or not this file knows
// about it, and the `default` arm below type-checks it — so the direction a new
// schema branch fails in is "checked", not "silently skipped".
//
// The named arms are therefore the whole of what this check does *not* look at,
// each with its reason. They are what makes the omissions readable: before this, a
// position missing from the walk and a position deliberately left to another
// validator looked exactly alike, because both were simply absent.
func checkExpressionTypes(wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	v1.WalkWorkflow(wf, v1.Walk{
		Value: func(site v1.ValueSite) {
			switch site.Slot {
			case v1.SlotInputDefault, v1.SlotInputExample:
				// A declaration's own value is judged against the type the
				// declaration states, by [v1.CheckInputValue] and
				// [v1.CheckInputConstraints], which is a sharper answer than
				// cel-go's and is already reported when the file loads.

			case v1.SlotSignalSubject:
				// signals.go owns a computed `subject:`: it decides where the
				// expression is routed and narrows what a rule shaped that way may
				// match, and a second reporter here would say a weaker version of
				// the same thing on the same line.

			case v1.SlotWebhookVerify:
				// A `verify:` entry is a secret reference rather than an
				// expression, and resolving one is not something a validator does.

			case v1.SlotSwitchCaseValue:
				// A case value must be a literal, and validate_switch.go already
				// refuses a computed one with the sentence that says why cases are
				// literals. Type-checking it too would report one mistake twice, in
				// two voices.

			case v1.SlotCallArgument:
				// validate_call.go checks a `with:` argument against the callee's
				// own declarations, which is the check that can be specific about
				// it.

			default:
				ds = append(ds, typeErrors(site.Step, site.Field(), site.Value)...)
			}
		},
	})

	return ds
}

// checkNodeExpressions checks every expression a step carries, at any depth.
//
// The same rule [checkExpressionTypes] applies, over a subtree rather than a whole
// document: every position arrives, and the two this does not judge are named with
// their reasons rather than absent.
func checkNodeExpressions(nodes []*v1.Node) Diagnostics {
	var ds Diagnostics

	v1.WalkNodes(nodes, v1.Walk{
		Value: func(site v1.ValueSite) {
			switch site.Slot {
			case v1.SlotSwitchCaseValue, v1.SlotCallArgument:
				// See [checkExpressionTypes] for both.

			default:
				// Every input, including the ones a task evaluates itself. The
				// reference walk skips those because it cannot model their scope;
				// this does not model any scope, so there is nothing to skip. An
				// `undo:` input arrives under the `undo:` key rather than the
				// input's name, for the reason [validateUndoInputs] gives: an input
				// name here would be looked up among the *step's* inputs.
				ds = append(ds, typeErrors(site.Step, site.Field(), site.Value)...)
			}
		},
	})

	return ds
}

// typeErrors reports what remains wrong with one expression once every name it
// mentions is assumed to exist.
func typeErrors(stepID, field string, val *v1.Value) Diagnostics {
	parsed := val.GetExpr()
	if parsed == nil {
		return nil
	}

	env, err := envDeclaring(referencedNames(parsed.GetExpr()))
	if err != nil {
		// Building the environment failed, which is a defect in this build rather
		// than something the file did. Reporting it against the author's line would
		// blame them for it; saying nothing leaves the check to run time, which is
		// where it was before this existed.
		return nil
	}

	_, issues := env.Check(cel.ParsedExprToAst(parsed))
	if issues == nil || issues.Err() == nil {
		return nil
	}

	var ds Diagnostics
	for _, message := range celCheckMessages(issues.Err().Error()) {
		ds = append(ds, Diagnostic{
			Step:    stepID,
			Field:   field,
			Message: message,
			// What survives cel-go's checker once every name is assumed to exist is,
			// by this file's own account, a missing overload, a wrong arity, or a
			// function nobody declared — a type mismatch in every case, never a
			// missing name (that is [validateInputRefs]'s question, not this one's).
			Code: v1.DiagnosticCodeTypeMismatch,
		})
	}

	return ds
}

// envDeclaring returns the profile's environment with each given name declared as
// `dyn`.
func envDeclaring(names []string) (*cel.Env, error) {
	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	if err != nil {
		return nil, err
	}

	base, err := v1.DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil, err
	}

	key := strings.Join(names, "\x00")
	if env, ok := cachedEnv(key); ok {
		return env, nil
	}

	opts := make([]cel.EnvOption, 0, len(names))
	for _, name := range names {
		opts = append(opts, cel.Variable(name, cel.DynType))
	}

	env, err := base.Extend(opts...)
	if err != nil {
		return nil, err
	}
	cacheEnv(key, env)

	return env, nil
}

// The environments this builds are cached, because `Extend` rebuilds a declaration
// set of over a hundred functions and a file's expressions mostly mention the same
// few names. Measured over a 200-expression file: validation went from 30.6ms to
// 50.1ms with this check and no cache, and to 37.9ms with one — so the cache returns
// about two thirds of what the check costs.
//
// Bounded, because the key is a set of identifiers out of the document and the
// document is somebody else's. An unbounded map here would be a file's author
// choosing how much memory a long-lived language server holds, one distinct name set
// at a time. Past the cap nothing is stored and the cost is simply the uncached one,
// which is the right way for a cache to fail.
const maxCachedEnvs = 512

var (
	envCacheMu sync.RWMutex
	envCache   = map[string]*cel.Env{}
)

// cachedEnv returns a previously built environment for a set of names.
func cachedEnv(key string) (*cel.Env, bool) {
	envCacheMu.RLock()
	defer envCacheMu.RUnlock()

	env, ok := envCache[key]

	return env, ok
}

// cacheEnv records an environment while there is room for it.
func cacheEnv(key string, env *cel.Env) {
	envCacheMu.Lock()
	defer envCacheMu.Unlock()

	if len(envCache) >= maxCachedEnvs {
		return
	}
	envCache[key] = env
}

// referencedNames returns every identifier an expression mentions and this may
// declare.
//
// All but one: the qualifier of a namespaced function. cel-go parses
// `regex.replace(s, a, b)` as a call whose target is the identifier `regex`, and
// declaring that makes `regex` a variable — so `regex.replace` reads as a field
// selected from it and the checker answers `undeclared reference to 'replace'`. A
// false diagnostic about a documented function, which is what this file exists to
// avoid. Left undeclared, the qualified function resolves as itself.
//
// Excluded by *position* rather than by name, and the difference is a bug this had
// before the tests found it. The first version skipped any identifier that is a
// known namespace, which deletes the name everywhere — including where it is not a
// qualifier at all. `json` is a namespace, and a step declaring `vars: {json: loud}`
// and reading `${json}` is an ordinary file that was suddenly refused, by a check
// written not to have opinions about scope.
//
// So a qualifier is only a qualifier where it stands in front of a function that
// exists: `math` in `math.greatest(...)` is skipped, `math` alone is declared. That
// is the same rule cel-go applies, which is what makes the two agree.
func referencedNames(e *expr.Expr) []string {
	found := map[string]bool{}
	collectNames(e, found)

	return slices.Sorted(maps.Keys(found))
}

// qualifiedFunctions are the profile's function names that carry a qualifier.
//
// Derived from the environment's declarations rather than from the library names,
// for the reason [functionNamespaces] records: `encoders` declares `base64.encode`
// and `protos` declares `proto.getExt`, so a set built from library names is wrong
// in both directions at once.
var qualifiedFunctions = func() map[string]bool {
	out := map[string]bool{}

	env, err := v1.DefaultEvaluator().ProfileEnv(v1.CurrentProfile)
	if err != nil {
		// Answering with an empty set costs a false diagnostic on a namespaced
		// call; panicking would cost every command. The same trade [functionNamespaces]
		// makes, for the same reason.
		return out
	}

	for name := range env.Functions() {
		if strings.Contains(name, ".") {
			out[name] = true
		}
	}

	return out
}()

// qualifies reports whether an identifier is standing in front of a function rather
// than naming a value.
func qualifies(target *expr.Expr, function string) bool {
	ident, ok := target.GetExprKind().(*expr.Expr_IdentExpr)
	if !ok {
		return false
	}

	return qualifiedFunctions[ident.IdentExpr.GetName()+"."+function]
}

// collectNames walks an expression, recording identifiers.
//
// Every node kind that can hold a sub-expression is here. One that is missed would
// leave a name undeclared and turn this into the thing it is written not to be — a
// check that reports an unknown name — so a new kind belongs in this switch before
// it belongs anywhere else.
func collectNames(e *expr.Expr, found map[string]bool) {
	if e == nil {
		return
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		found[kind.IdentExpr.GetName()] = true
	case *expr.Expr_SelectExpr:
		collectNames(kind.SelectExpr.GetOperand(), found)
	case *expr.Expr_CallExpr:
		// The target is walked unless it is only there to qualify the function
		// being called. See [referencedNames].
		if !qualifies(kind.CallExpr.GetTarget(), kind.CallExpr.GetFunction()) {
			collectNames(kind.CallExpr.GetTarget(), found)
		}
		for _, arg := range kind.CallExpr.GetArgs() {
			collectNames(arg, found)
		}
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			collectNames(element, found)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectNames(entry.GetMapKey(), found)
			collectNames(entry.GetValue(), found)
		}
	case *expr.Expr_ComprehensionExpr:
		// What a macro expands to. `list.filter(x, x > 1)` becomes one of these,
		// and its parts hold the only occurrences of everything the macro's body
		// mentions.
		comprehension := kind.ComprehensionExpr
		collectNames(comprehension.GetIterRange(), found)
		collectNames(comprehension.GetAccuInit(), found)
		collectNames(comprehension.GetLoopCondition(), found)
		collectNames(comprehension.GetLoopStep(), found)
		collectNames(comprehension.GetResult(), found)
	}
}

// celCheckMessages turns cel-go's issue text into one sentence per problem.
//
// cel-go joins its issues with newlines and prefixes each with `ERROR: <input>:1:7:`
// — a position inside the expression source, which is not where the author is
// looking and reads like a second, contradictory answer beside the Flowfile line
// this diagnostic is placed at. The position is dropped for the same reason
// [celFailure] drops it.
//
// It also appends a caret line under the offending column, which is drawn against
// the expression rather than the document and lines up with nothing here.
func celCheckMessages(text string) []string {
	var out []string

	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "ERROR: ") {
			// A caret line, or the continuation of a message that wrapped.
			continue
		}
		if match := celErrorPattern.FindStringSubmatch(line); match != nil {
			out = append(out, forAnAuthor(strings.TrimSpace(match[3])))

			continue
		}
		out = append(out, forAnAuthor(strings.TrimPrefix(line, "ERROR: ")))
	}

	if len(out) == 0 {
		// Something whose shape this does not recognise. Passed through whole rather
		// than dropped: an unreadable diagnostic beats a silent one.
		out = append(out, fmt.Sprintf("expression cannot be evaluated: %s", strings.TrimSpace(text)))
	}

	return out
}

// containerNote is the parenthetical cel-go appends to an undeclared reference.
//
// It names the *container* — its notion of a namespace prefix — which this build
// never sets, so every one of these reads `(in container ”)`. True, internal, and
// nothing an author can act on.
var containerNote = regexp.MustCompile(` \(in container '[^']*'\)`)

// unknownFunction matches what cel-go says about a call to a function nobody
// declared.
var unknownFunction = regexp.MustCompile(`^undeclared reference to '([^']+)'`)

// stringOfAStructure matches `string()` applied to something it has no overload for.
//
// Narrow on purpose. This is the mistake that shipped in an example, it has one
// right answer, and the answer is not discoverable — `string()` is the obvious
// spelling and the working one is not. Every other missing overload gets cel-go's
// sentence unchanged, because inventing advice for a case nobody has hit is how a
// diagnostic ends up sending somebody the wrong way.
//
// `json.encode` and not `"%s".format([value])`, which also renders a structure and
// was what this said first. `format` produces CEL's own rendering —
// `{a: 1, b: [x, y]}` — which is a debug form rather than a document: no quotes, so
// nothing downstream can parse it back. A `fields:` value is read by a log sink, and
// `{"a":1,"b":["x","y"]}` is the one of the two it can do something with. `format`
// is for putting a *scalar* into a sentence.
// A null is matched alongside them because #413 made this reachable from a value
// that never calls `string()` in its source at all: interpolation desugars each
// fence to `string(<fence>)`, so `deployed by ${inputs.who}` on an input that can
// be absent arrives here as this message about a call the author did not write.
// The advice has to fit what they *did* write, which is why the null case gets a
// sentence about saying what a missing value should read as rather than one about
// rendering a structure.
var stringOfAStructure = regexp.MustCompile(`^found no matching overload for 'string' applied to '\((map|list|null_type)`)

// forAnAuthor turns one of cel-go's sentences into one written for the person who
// typed the expression.
//
// Two rules, and no more than two. What cel-go says is usually exactly right — a
// missing overload names the function and the types it got — and rewriting an
// accurate message is how the accuracy is lost. This removes what an author cannot
// act on, and adds the one next step that is not guessable.
func forAnAuthor(message string) string {
	message = containerNote.ReplaceAllString(message, "")

	if match := unknownFunction.FindStringSubmatch(message); match != nil {
		// Both venues named, because a diagnostic is read wherever validation
		// runs and this one is advice: an agent that reached this through
		// `flowstate_validate` has no shell to run `flow tasks` in, and was
		// being sent to a command it cannot run for the one answer it needs.
		// The catalog is the same catalog either way — GetCatalog is what
		// `flow tasks` prints — so naming it serves the reader in the terminal
		// and the reader over the wire with one sentence rather than a
		// venue-aware rewrite of it.
		return fmt.Sprintf(
			"no function called %q; the functions this profile provides are listed by "+
				"`flow tasks`, and by the GetCatalog RPC (`flowstate_get_catalog` over MCP)",
			match[1])
	}

	if match := stringOfAStructure.FindStringSubmatch(message); match != nil {
		if match[1] == "null_type" {
			return message + "; string() takes a value, and a null has none to render, " +
				"so say what a missing one should read as — `${x.orValue('unknown')}`, or a conditional"
		}
		return message + "; string() takes a scalar, so render a map or a list with json.encode(value)"
	}

	return message
}
