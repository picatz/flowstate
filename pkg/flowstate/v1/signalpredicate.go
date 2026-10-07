package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"

	"github.com/picatz/flowstate/pkg/flowstate/v1/celrule"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// `signals: <name>: allow: ${...}`, `debug: allow: ${...}` and
// `triggers: manual: allow: ${...}` — who may deliver a signal, hold a debug
// lease, or start a workload by hand, as one CEL predicate, evaluated where the
// action is accepted.
//
// All three stanzas compile and run through this file. `signals:` and `debug:`
// share one scope (the sender, the run's starter, the run's inputs) and both
// reach [SignalPolicyCheck]; `manual:` has no run yet, so its scope is the
// caller and the submitted inputs (see [CompileManualAllowPredicate]), and it
// reaches the same evaluator through [CheckManualStart]. There is one
// evaluator: the manual scope differs by one undeclared name, not by a second
// compile or a second run loop.
//
// # One evaluator
//
// [SignalPolicyCheck] is the one function every enforcement point reaches —
// the server's `Signal` and `GetGate`'s `may_answer`, the local driver
// ([LocalSignals]), `flow test`, `flow signal` rehearsal and MCP — and it
// routes a policy's [SignalPolicy.allow] here. There is no second
// evaluator: the predicate is compiled and run by [celrule], the machinery the
// egress, assumption, secret and task-shape policies share, over an environment
// this file owns (the one thing [celrule] leaves to each surface).
//
// # The scope is closed
//
//	sender.identity.{principal,subject,issuer,namespace,kind,claims,actions,actors,delegated}
//	run.identity.{principal,subject,issuer,namespace,kind,claims,actions,actors,delegated}   (the starter)
//	inputs                                                      (the run's arguments)
//
// Nothing else: no steps, vars, secrets or clock. An unknown root or field is a
// type error at compile time, so a misspelling is a refusal rather than a
// predicate that quietly never matches.
//
// `sender` is the server's own attestation of the delivering caller. Its
// claims are bound here, server-side, from that verified identity and are never
// recorded in a step's outputs (a wait's own `sender` deliberately omits them,
// see [signalSenderValue]); this scope is the only place they are readable.
//
// # Fail closed
//
// A refusal is the answer to everything that is not a clean `true`: a
// predicate that does not compile, a result that is not a bool, an evaluation
// error, a cost bound exceeded, and an unknown starter that the predicate reads
// (`run` is simply not bound when the run has no recorded starter, so reading
// it errors). None allows. The error is never allowed to carry what failed to
// evaluate: a conversion error quotes its operand, and an operand can be an
// input or a claim, so the refusal says what went wrong and not with which
// value.
//
// # The narrowing rule, syntactically
//
// Whoever starts a run chooses its inputs, so a predicate over `inputs` alone
// would let the starter name their own approver. A predicate that reads
// `inputs` must therefore also read `sender.identity.claims` or `run.identity`,
// something the starter's inputs cannot reach. The check is syntactic, so
// `claims.x == 1 || inputs.admin == "y"` satisfies it: authors write the
// narrowing as a conjunction. See [SignalPolicy.allow].

// SignalPolicyExprCostLimit bounds the CEL evaluation cost of one signal
// policy predicate, the same budget the other policy surfaces give a rule
// ([DefaultTaskPolicyRuleCostLimit]). It is spent on every delivery attempt,
// including those of a caller the predicate is about to refuse, so it is a
// work bound and not a reporting limit.
const SignalPolicyExprCostLimit uint64 = 50_000

// SignalPolicyExprTimeout bounds the wall-clock time one predicate evaluation
// may take, on top of [SignalPolicyExprCostLimit].
const SignalPolicyExprTimeout = time.Second

// MaxSignalPolicyScopeBytes bounds the encoded run scope (its inputs and its
// starter) a run records so a signal predicate can read them at delivery time
// ([SignalPolicyExprReads]). A run whose predicate reads them and whose scope
// exceeds it is refused at submit rather than recorded truncated: a predicate
// evaluated over a partial copy of its inputs would be a different predicate.
const MaxSignalPolicyScopeBytes = 64 << 10

// signalPolicyActor wraps a [principal.Caller] as the one field `sender` and
// `run` each
// expose.
type signalPolicyActor struct {
	Identity principal.Caller `cel:"identity"`
}

// refusedSignalPolicyActor is [signalPolicyActor] with its one field typed `any`,
// so it can hold [principal.Caller.Bind]'s stand-in for a caller whose claims
// were refused. cel-go reads the declared `identity` field by its index, so the
// stand-in is what `sender.identity` evaluates to.
type refusedSignalPolicyActor struct {
	Identity any `cel:"identity"`
}

// ext.NativeTypes names a type by the last element of its package *path*
// ("v1"), not its declared package name; pinned by a test.
const signalPolicyActorTypeName = "v1.signalPolicyActor"

var signalPolicyEnv = sync.OnceValues(func() (*cel.Env, error) { return allowPolicyEnv(true) })

// manualPolicyEnv is [signalPolicyEnv] without `run`: a manual start has no run
// yet, so `run` is an undeclared name there and a predicate that reads it is a
// compile error rather than one that errors at every start.
var manualPolicyEnv = sync.OnceValues(func() (*cel.Env, error) { return allowPolicyEnv(false) })

func allowPolicyEnv(withRun bool) (*cel.Env, error) {
	opts := []cel.EnvOption{
		ext.NativeTypes(append(principal.NativeTypeArgs(), reflect.TypeFor[signalPolicyActor]())...),
		cel.Variable("sender", cel.ObjectType(signalPolicyActorTypeName)),
		cel.Variable(InputsRoot, cel.MapType(cel.StringType, cel.DynType)),
		ext.Strings(ext.StringsVersion(5)),
		// The same literal check every other checker inherits from the shared
		// profile (see buildEnv): a `matches('[')` is refused where it is
		// written instead of denying every delivery at run time. It replaces
		// the coverage the retired computed `subject:` position had.
		cel.ASTValidators(cel.ValidateRegexLiterals()),
	}
	if withRun {
		opts = append(opts, cel.Variable("run", cel.ObjectType(signalPolicyActorTypeName)))
	}

	return cel.NewEnv(opts...)
}

// SignalPolicyPredicate is a compiled `allow:` predicate.
type SignalPolicyPredicate struct {
	rule  celrule.Rule
	reads SignalPolicyReads
}

// SignalPolicyReads is which per-run parts of the scope a predicate reads, so
// that submit records exactly those beside the policy and nothing else.
type SignalPolicyReads struct {
	// Inputs: the run's `inputs`.
	Inputs bool
	// InputNames are the top-level input names the predicate reads, sorted and
	// without repeats. Submit records exactly these and no other input: what a
	// predicate does not name is never copied into history, and a predicate that
	// names no input statically is refused (see [compileAllowPredicate]).
	InputNames []string
	// Run: `run.identity`, the starter, including its claims.
	Run bool
	// Claims: `sender.identity.claims`, which carries only the claims the
	// admitting auth policy entry carries (`carry_claims`, `groups_claim`).
	Claims bool
}

// Reads reports which per-run parts of the scope the predicate reads.
func (p SignalPolicyPredicate) Reads() SignalPolicyReads { return p.reads }

// CompileSignalPolicyPredicate type-checks src against the closed scope,
// requires a bool result, applies the narrowing rule and builds a program
// bounded by [SignalPolicyExprCostLimit].
//
// The one compile both validation (`flow validate`, the editor, submit) and
// enforcement call, so a file the validator accepts is a policy the server
// evaluates and the reverse. src is the expression without its `${` `}` fence.
//
// The refusal quotes the expression, which the author wrote, and nothing a run
// supplied.
func CompileSignalPolicyPredicate(src string) (SignalPolicyPredicate, error) {
	return compileAllowPredicate(src, false)
}

// CompileManualAllowPredicate is [CompileSignalPolicyPredicate] for `triggers:
// manual: allow: ${...}`: the same compile, bool requirement, cost bound and
// narrowing rule over a scope without `run`.
//
// A manual start has no run, so the scope is `sender.identity.{...}` (the
// caller) and `inputs` (the arguments submitted with this start), and nothing
// else; `run` is an undeclared name. With no run starter to compare against,
// the narrowing rule is that a predicate reading `inputs` must also read
// `sender.identity.claims`: the caller chooses the inputs, so a predicate over
// them alone would let them admit themselves.
func CompileManualAllowPredicate(src string) (SignalPolicyPredicate, error) {
	return compileAllowPredicate(src, true)
}

func compileAllowPredicate(src string, manual bool) (SignalPolicyPredicate, error) {
	envFn, scopeDescription := signalPolicyEnv, signalPolicyScopeDescription
	what := "signal policy"
	if manual {
		envFn, scopeDescription, what = manualPolicyEnv, manualPolicyScopeDescription, "manual start"
	}

	env, err := envFn()
	if err != nil {
		return SignalPolicyPredicate{}, fmt.Errorf("building the %s environment: %w", what, err)
	}

	if strings.TrimSpace(src) == "" {
		return SignalPolicyPredicate{}, errors.New("the predicate is empty, so this policy authorizes nobody")
	}

	checked, issues := env.Compile(src)
	if issues.Err() != nil {
		return SignalPolicyPredicate{}, fmt.Errorf("%w; a %s predicate reads only %s",
			issues.Err(), what, scopeDescription)
	}

	rule, err := celrule.Build(env, checked, src, SignalPolicyExprCostLimit)
	if err != nil {
		return SignalPolicyPredicate{}, err
	}

	analysis := analyzeSignalPredicate(checked)
	reads := analysis.reads
	if reads.opaqueInputs {
		return SignalPolicyPredicate{}, errors.New(
			"the predicate reads `inputs` without naming an input (`inputs[<computed>]`, `inputs` passed whole " +
				"or iterated); the run records only the inputs a predicate names, so write each as " +
				"`inputs.name` or `inputs[\"name\"]`")
	}
	if reads.inputs && !reads.claims && !reads.run {
		if manual {
			return SignalPolicyPredicate{}, errors.New(
				"the predicate reads `inputs` but not `sender.identity.claims`; the caller chooses the inputs " +
					"they submit, so as written they may admit themselves by what they pass. Also compare " +
					"`sender.identity.claims` (for example `sender.identity.claims.team == \"ops\"`)")
		}

		return SignalPolicyPredicate{}, errors.New(
			"the predicate reads `inputs` but nothing alongside it that the run's inputs cannot reach; " +
				"whoever starts the run chooses its inputs, so as written they may name themselves as their own " +
				"approver. Also compare `sender.identity.claims` or `run.identity` (for example " +
				"`sender.identity.principal != run.identity.principal`)")
	}

	return SignalPolicyPredicate{rule: rule, reads: SignalPolicyReads{
		Inputs:     reads.inputs,
		InputNames: slices.Sorted(maps.Keys(reads.inputNames)),
		Run:        reads.run,
		Claims:     reads.claims,
	}}, nil
}

const signalPolicyScopeDescription = "`sender.identity.{principal,subject,issuer,namespace,kind,claims,actions,actors,delegated}`, " +
	"`run.identity` (the starter, same fields) and `inputs`"

const manualPolicyScopeDescription = "`sender.identity.{principal,subject,issuer,namespace,kind,claims,actions,actors,delegated}` " +
	"(the caller) and `inputs` (the arguments submitted with this start); there is no `run` yet"

// CheckManualAllowExpr reports why src is not an acceptable `manual: allow`
// predicate, or nil.
func CheckManualAllowExpr(src string) error {
	_, err := CompileManualAllowPredicate(src)

	return err
}

// CheckSignalPolicyExpr reports why src is not an acceptable predicate, or nil.
func CheckSignalPolicyExpr(src string) error {
	_, err := CompileSignalPolicyPredicate(src)

	return err
}

// SignalPolicyExprReads reports which per-run parts of the scope any predicate
// in policies reads. An expression that does not compile reads nothing here: it
// is refused at submit before this is asked and denies at delivery if it
// somehow survived.
func SignalPolicyExprReads(policies map[string]*SignalPolicy) SignalPolicyReads {
	var reads SignalPolicyReads
	for _, policy := range policies {
		if policy.GetAllow() == "" {
			continue
		}
		if p, err := CompileSignalPolicyPredicate(policy.GetAllow()); err == nil {
			reads.Inputs = reads.Inputs || p.reads.Inputs
			reads.InputNames = append(reads.InputNames, p.reads.InputNames...)
			reads.Run = reads.Run || p.reads.Run
			reads.Claims = reads.Claims || p.reads.Claims
		}
	}

	slices.Sort(reads.InputNames)
	reads.InputNames = slices.Compact(reads.InputNames)

	return reads
}

// CheckPolicyInputsNotSensitive refuses a predicate that reads an input the
// workflow declares `sensitive:`. A predicate's inputs are recorded with the run
// so it can be evaluated at delivery, and a sensitive value never enters
// durable history (the rule `prompt:` and `fail: message:` already follow:
// refuse the reach). sensitive is [SensitiveInputNames]; where names the stanza.
//
// The refusal names the input, which the author wrote, and no value. A
// predicate that cannot be analysed (it does not compile) is not this check's
// to refuse; [CheckSignalPolicyExpr] does.
func CheckPolicyInputsNotSensitive(where string, policy *SignalPolicy, sensitive map[string]bool) error {
	if len(sensitive) == 0 || policy.GetAllow() == "" {
		return nil
	}

	p, err := CompileSignalPolicyPredicate(policy.GetAllow())
	if err != nil {
		return nil
	}

	for _, name := range p.reads.InputNames {
		if sensitive[name] {
			return fmt.Errorf("%s.allow reads the input %q, which is declared `sensitive:`; the inputs a "+
				"predicate reads are recorded with the run, and a sensitive input is never recorded. "+
				"Compare a claim or the starter instead, or drop `sensitive:` from that input", where, name)
		}
	}

	return nil
}

// CheckWorkflowPolicyInputs is [CheckPolicyInputsNotSensitive] over every
// `signals:` policy and `debug:` of wf, the one call submit makes.
func CheckWorkflowPolicyInputs(wf *Workflow) error {
	sensitive := SensitiveInputNames(wf)
	if len(sensitive) == 0 {
		return nil
	}

	for _, name := range slices.Sorted(maps.Keys(wf.GetSignals())) {
		if err := CheckPolicyInputsNotSensitive(fmt.Sprintf("signals[%q]", name), wf.GetSignals()[name], sensitive); err != nil {
			return err
		}
	}

	return CheckPolicyInputsNotSensitive("debug", wf.GetDebug(), sensitive)
}

type signalPolicyScopeReads struct {
	inputs, claims, run bool
	// inputNames are the top-level keys read as `inputs.k`, `inputs["k"]`,
	// `has(inputs.k)` or `"k" in inputs`; opaqueInputs is set by any other use
	// of `inputs`, which names no key.
	inputNames   map[string]struct{}
	opaqueInputs bool
}

// signalPredicateAnalysis is everything one walk of a checked `allow:`
// predicate learns: the scope it reads and the senders it can admit.
type signalPredicateAnalysis struct {
	reads signalPolicyScopeReads
	// principals and closed are the closed-principal result for the whole
	// predicate; see [SignalPolicyClosedPrincipals] for what closed means.
	principals map[string]struct{}
	closed     bool
}

// analyzeSignalPredicate is the single traversal of a checked predicate. Both
// results come from it, so the two cannot disagree about what the predicate
// contains or drift apart as the supported shapes grow.
//
// Reads. It reports which of the narrowing-relevant names the predicate reads.
// Only names that resolve to the environment's own variables count: a
// comprehension variable that happens to be spelled `run` is the author's local
// ([signalPolicyIsLocal]) and must not satisfy the narrowing rule.
//
// Closed principals. Each node is analyzed after its children, so the result of
// `||` and `&&` is built from their operands' results as they are produced; see
// [closedOfCall] for the rules and [SignalPolicyClosedPrincipals] for what is
// and is not covered.
//
// Bounded. The walk keeps an explicit stack instead of recursing, so its depth
// is heap, not goroutine stack, and it visits each node once: its work is
// linear in the predicate, which the parser's own recursion and size limits
// already cap before a checked AST exists. A shape the walk cannot analyze
// takes the conservative answer: open for principals, and an `inputs` read it
// cannot name is opaque.
func analyzeSignalPredicate(checked *cel.Ast) signalPredicateAnalysis {
	root := celast.NavigateAST(checked.NativeRep())

	global := func(e celast.NavigableExpr, name string) bool {
		return e.Kind() == celast.IdentKind && e.AsIdent() == name && !signalPolicyIsLocal(e)
	}

	out := signalPredicateAnalysis{reads: signalPolicyScopeReads{inputNames: map[string]struct{}{}}}
	reads := &out.reads

	// sets holds the closed-principal result of each node already analyzed that
	// has one, keyed by node ID and consumed by its parent.
	sets := map[int64]map[string]struct{}{}

	type frame struct {
		node     celast.NavigableExpr
		children []celast.NavigableExpr
		next     int
	}
	stack := []frame{{node: root, children: root.Children()}}

	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		if top.next < len(top.children) {
			child := top.children[top.next]
			top.next++
			stack = append(stack, frame{node: child, children: child.Children()})

			continue
		}

		e := top.node
		stack = stack[:len(stack)-1]

		switch {
		case global(e, InputsRoot):
			reads.inputs = true
			if name, ok := signalPolicyMapKey(e); ok {
				reads.inputNames[name] = struct{}{}
			} else {
				reads.opaqueInputs = true
			}
		case global(e, "run"):
			reads.run = true
		case e.Kind() == celast.SelectKind:
			sel := e.AsSelect()
			if sel.IsTestOnly() || sel.FieldName() != "claims" {
				continue
			}
			identity := e.Children()[0]
			if identity.Kind() != celast.SelectKind || identity.AsSelect().FieldName() != "identity" {
				continue
			}
			if global(identity.Children()[0], "sender") {
				reads.claims = true
			}
		case e.Kind() == celast.CallKind:
			if set, ok := closedOfCall(e.AsCall(), sets); ok {
				sets[e.ID()] = set
			}
		}
	}

	out.principals, out.closed = sets[root.ID()]

	return out
}

// signalPolicyMapKey reports the one key a map-valued node (the `inputs` identifier,
// or a `claims` select, see [IdentityClaimReads]) is
// used to read, from its parent: a field select, an index by a string literal,
// or the right side of `"k" in inputs`. Anything else (a computed key, a call
// taking the map, a comprehension over it, an optional select) names no key.
func signalPolicyMapKey(ident celast.NavigableExpr) (string, bool) {
	parent, ok := ident.Parent()
	if !ok {
		return "", false
	}

	switch parent.Kind() {
	case celast.SelectKind:
		if parent.AsSelect().Operand().ID() == ident.ID() {
			return parent.AsSelect().FieldName(), true
		}
	case celast.CallKind:
		call := parent.AsCall()
		args := call.Args()
		if len(args) != 2 {
			return "", false
		}
		key := func(e celast.Expr) (string, bool) {
			if e.Kind() != celast.LiteralKind {
				return "", false
			}
			s, ok := e.AsLiteral().(types.String)

			return string(s), ok
		}
		switch call.FunctionName() {
		case operators.Index:
			if args[0].ID() == ident.ID() {
				return key(args[1])
			}
		case operators.In:
			if args[1].ID() == ident.ID() {
				return key(args[0])
			}
		}
	}

	return "", false
}

// signalPolicyIsLocal reports whether the identifier is bound by a
// comprehension that encloses it: a name its iteration range or initial value
// cannot see, but its condition, step and result can.
func signalPolicyIsLocal(ident celast.NavigableExpr) bool {
	name := ident.AsIdent()
	child := ident
	for {
		parent, ok := child.Parent()
		if !ok {
			return false
		}
		if parent.Kind() == celast.ComprehensionKind {
			comp := parent.AsComprehension()
			inScope := child.ID() != comp.IterRange().ID() && child.ID() != comp.AccuInit().ID()
			if inScope && (comp.IterVar() == name || comp.IterVar2() == name || comp.AccuVar() == name) {
				return true
			}
		}
		child = parent
	}
}

// signalPolicyActivation binds the closed scope. `run` is bound only when the
// starter is known: an unbound name is an evaluation error, which is how an
// unknown starter denies a predicate that reads it and spares one that does
// not.
func signalPolicyActivation(identity, starter *WorkloadIdentity, hasStarter bool, inputs map[string]*Value) map[string]any {
	vars := map[string]any{"sender": newSignalPolicyActor(identity)}
	// inputs is unbound when the caller holds none (nil), not empty: a
	// predicate over an empty scope would read `!has(inputs.x)` as true. A run
	// that has inputs but none set passes an empty, non-nil map.
	if inputs != nil {
		vars[InputsRoot] = signalPolicyInputsValue(inputs)
	}
	if hasStarter {
		vars["run"] = newSignalPolicyActor(starter)
	}

	return vars
}

func newSignalPolicyActor(identity *WorkloadIdentity) any {
	// The same principal.Caller every policy surface binds as `identity`, so a
	// kind, a list claim or a nested claim reads the same here as in an egress
	// or task rule ([CallerOf]).
	caller := CallerOf(identity).Normalized()
	if caller.Claims.Refused() != nil {
		// A field of the typed actor can only hold the claims carrier, which
		// does not refuse `!=` or a left-hand `{} ==`; the bound caller does
		// ([principal.Caller.Bind]).
		return &refusedSignalPolicyActor{Identity: caller.Bind()}
	}

	return &signalPolicyActor{Identity: caller}
}

func signalPolicyInputsValue(inputs map[string]*Value) ref.Val {
	entries := make(map[ref.Val]ref.Val, len(inputs))
	for name, v := range refValues(inputs) {
		entries[types.String(name)] = v
	}

	return types.NewRefValMap(TypeAdapter, entries)
}

// signalPolicyExprAllows evaluates one predicate for one sender and returns nil
// only for a clean `true`. Every refusal it returns is a fixed sentence: it
// wraps nothing the evaluation produced. label names the stanza in the refusal
// ("signal", "debug policy").
func signalPolicyExprAllows(ctx context.Context, label, src string, identity, starter *WorkloadIdentity, hasStarter bool, inputs map[string]*Value) error {
	return allowPredicateAllowsWithin(ctx, SignalPolicyExprTimeout, label, false, src, identity, starter, hasStarter, inputs)
}

// signalPolicyExprAllowsWithin is [signalPolicyExprAllows] with the deadline a
// parameter, so a test can prove the deadline is what denies.
func signalPolicyExprAllowsWithin(ctx context.Context, timeout time.Duration, src string, identity, starter *WorkloadIdentity, hasStarter bool, inputs map[string]*Value) error {
	return allowPredicateAllowsWithin(ctx, timeout, "signal", false, src, identity, starter, hasStarter, inputs)
}

// allowPredicateAllowsWithin is the one run loop behind all three stanzas:
// compile (the scope picked by manual), bound by time, evaluate, and let only a
// clean true through. label names the stanza in the refusal ("signal", "debug
// policy", "manual start").
func allowPredicateAllowsWithin(ctx context.Context, timeout time.Duration, label string, manual bool, src string, identity, starter *WorkloadIdentity, hasStarter bool, inputs map[string]*Value) error {
	predicate, err := compileAllowPredicate(src, manual)
	if err != nil {
		return fmt.Errorf("this %s's allow predicate is not a valid policy, so no sender is authorized "+
			"until it is fixed", label)
	}

	// Bounded in time as well as in cost: the cost estimator counts steps, and a
	// few string functions are priced only statically, so a deadline is what
	// stops an evaluation that is cheap on paper and slow in fact. A timeout is
	// an error, and an error denies.
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	allowed, err := predicate.rule.Match(ctx, signalPolicyActivation(identity, starter, hasStarter, inputs))
	if err != nil {
		// Deliberately not wrapped, formatted in or inspected: cel-go's
		// conversion errors quote their operand, and an operand can be an input
		// or a claim. One sentence for every cause.
		return fmt.Errorf("this %s's allow predicate could not be evaluated for this sender "+
			"(it errored, for example by reading the run's starter when none is recorded or a claim or "+
			"input that is missing, or it exceeded its cost bound), so the sender is refused%s",
			label, senderClaimsHint(predicate.reads.Claims, identity))
	}
	if !allowed {
		return fmt.Errorf("the sender does not satisfy this %s's allow predicate%s",
			label, senderClaimsHint(predicate.reads.Claims, identity))
	}

	return nil
}

// senderClaimsHint explains a refusal of a predicate that reads
// `sender.identity.claims`: which claim names the sender identity carried, so an
// empty projection reads differently from a wrong value. Names only, never
// values. The sender identity holds only the claims its issuer entry carries, which
// is the coupling an operator otherwise cannot see.
func senderClaimsHint(readsClaims bool, sender *WorkloadIdentity) string {
	if !readsClaims {
		return ""
	}

	names := slices.Sorted(maps.Keys(sender.GetPrincipal().GetClaims()))
	carried := "no claims"
	if len(names) > 0 {
		carried = "only the claims " + strings.Join(names, ", ")
	}

	return fmt.Sprintf("; the predicate reads sender.identity.claims, and the sender identity carried %s "+
		"(a server carries a token's claim into it only when the admitting issuer entry lists it under `carry_claims` or `groups_claim` in the auth policy)", carried)
}

// manualAllowExprAllows decides a `manual: allow: ${...}` start: the caller and
// the submitted inputs, no run. Same loop as the other two stanzas.
func manualAllowExprAllows(ctx context.Context, src string, caller *WorkloadIdentity, inputs map[string]*Value) error {
	return allowPredicateAllowsWithin(ctx, SignalPolicyExprTimeout, "manual start", true, src, caller, nil, false, inputs)
}

// SignalPolicyClosedPrincipals reports the principals a policy's predicate can
// admit, and whether that set is exact enough to count: closed is false for no
// policy, a predicate that does not compile, and any predicate that admits a
// sender it cannot name.
//
// It is what lets a `quorum:` asking for more distinct approvers than the policy
// can ever admit be refused where somebody can fix it, and it comes from the
// same single walk ([analyzeSignalPredicate]) that reports the predicate's
// reads.
//
// Covered. Only `sender.identity.principal == "<literal>"` (either operand
// order) and `sender.identity.principal in ["<literal>", ...]` name principals.
// `||` unions what its sides admit. `&&` only narrows, so it is bounded by
// whichever side names principals, and by their intersection when both do; an
// operand that names none leaves the other's bound standing.
//
// Not covered. Everything else is open: a claims, namespace, kind or issuer
// comparison, `!=`, `!`, a method such as `startsWith`, a computed literal
// (`"x#" + inputs.who`), a list holding a non-literal, a macro, a ternary, and a
// predicate reading only `run` or `inputs`. The analysis never reasons about
// what such a term excludes, so it never lets one close a set.
//
// What open means for quorum. A predicate that is not closed counts as an open
// set: it may admit any number of distinct senders, so the quorum check has no
// upper bound to compare `approve:` against and does not refuse it. The check
// is therefore lost, not wrong: a `quorum:` that is unreachable because a claims
// term admits fewer approvers than `approve:` is accepted at validation and
// only ever stalls at run time. The same holds for a `||` with one open side
// (the whole disjunction is open) and for a predicate that does not compile.
// The bound is an upper bound only: it counts principals the predicate could
// admit, not how many of them the narrowing terms in an `&&` actually let
// through, so a closed set larger than `approve:` does not prove the quorum is
// reachable.
func SignalPolicyClosedPrincipals(policy *SignalPolicy) (principals []string, closed bool) {
	src := policy.GetAllow()
	if src == "" {
		return nil, false
	}

	env, err := signalPolicyEnv()
	if err != nil {
		return nil, false
	}

	checked, issues := env.Compile(src)
	if issues.Err() != nil {
		return nil, false
	}

	analysis := analyzeSignalPredicate(checked)
	if !analysis.closed {
		return nil, false
	}

	return slices.Sorted(maps.Keys(analysis.principals)), true
}

// closedOfCall is the closed-principal rule for one call node, given the
// results of its operands in sets (keyed by node ID, filled by the walk in
// operand-first order). ok is false when the call admits a sender it cannot
// name.
func closedOfCall(call celast.CallExpr, sets map[int64]map[string]struct{}) (map[string]struct{}, bool) {
	args := call.Args()

	switch call.FunctionName() {
	case operators.LogicalOr:
		if len(args) != 2 {
			return nil, false
		}
		left, lok := sets[args[0].ID()]
		right, rok := sets[args[1].ID()]
		if !lok || !rok {
			return nil, false
		}
		// Fold the smaller operand into the larger, and drop both entries: they
		// are consumed here, so a long chain neither re-copies its growing union
		// at every parent nor keeps every intermediate set alive.
		if len(left) < len(right) {
			left, right = right, left
		}
		maps.Copy(left, right)
		delete(sets, args[0].ID())
		delete(sets, args[1].ID())

		return left, true
	case operators.LogicalAnd:
		if len(args) != 2 {
			return nil, false
		}
		left, lok := sets[args[0].ID()]
		right, rok := sets[args[1].ID()]
		delete(sets, args[0].ID())
		delete(sets, args[1].ID())
		switch {
		case lok && rok:
			for principal := range left {
				if _, both := right[principal]; !both {
					delete(left, principal)
				}
			}

			return left, true
		case lok:
			return left, true
		case rok:
			return right, true
		default:
			return nil, false
		}
	case operators.Equals:
		if len(args) != 2 {
			return nil, false
		}
		for i, side := range args {
			if !isSenderPrincipal(side) {
				continue
			}
			if literal, ok := policyStringLiteral(args[1-i]); ok {
				return map[string]struct{}{literal: {}}, true
			}
		}

		return nil, false
	case operators.In:
		if len(args) != 2 || !isSenderPrincipal(args[0]) || args[1].Kind() != celast.ListKind {
			return nil, false
		}
		set := make(map[string]struct{})
		for _, element := range args[1].AsList().Elements() {
			literal, ok := policyStringLiteral(element)
			if !ok {
				return nil, false
			}
			set[literal] = struct{}{}
		}

		return set, true
	default:
		return nil, false
	}
}

// isSenderPrincipal reports whether e is exactly `sender.identity.principal`.
func isSenderPrincipal(e celast.Expr) bool {
	if e.Kind() != celast.SelectKind || e.AsSelect().FieldName() != "principal" {
		return false
	}
	identity := e.AsSelect().Operand()
	if identity.Kind() != celast.SelectKind || identity.AsSelect().FieldName() != "identity" {
		return false
	}
	root := identity.AsSelect().Operand()

	return root.Kind() == celast.IdentKind && root.AsIdent() == "sender"
}

func policyStringLiteral(e celast.Expr) (string, bool) {
	if e.Kind() != celast.LiteralKind {
		return "", false
	}
	s, ok := e.AsLiteral().Value().(string)

	return s, ok
}
