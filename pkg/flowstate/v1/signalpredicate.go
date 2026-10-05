package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"

	"github.com/picatz/flowstate/pkg/flowstate/v1/celrule"
)

// `signals: <name>: allow: ${...}` — who may deliver a signal, as one CEL
// predicate, evaluated where the signal is accepted.
//
// # One evaluator
//
// [SignalPolicyCheck] is the one function every enforcement point reaches —
// the server's `Signal` and `GetGate`'s `may_answer`, the local driver
// ([LocalSignals]), `flow test`, `flow signal` rehearsal and MCP — and it
// routes a policy that sets [SignalPolicy.allow_expr] here. There is no second
// evaluator: the predicate is compiled and run by [celrule], the machinery the
// egress, assumption, secret and task-shape policies share, over an environment
// this file owns (the one thing [celrule] leaves to each surface).
//
// # The scope is closed
//
//	sender.identity.{principal,subject,issuer,namespace,claims}
//	run.identity.{principal,subject,issuer,namespace,claims}   (the starter)
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
// would let the starter name their own approver — the fault
// `SignalPolicyRule.subject_from` is refused for. A predicate that reads
// `inputs` must therefore also read `sender.identity.claims` or `run.identity`,
// something the starter's inputs cannot reach. This is coarser than the per-rule
// check it replaces and is recorded as the cost; see [SignalPolicy.allow_expr].

// SignalPolicyExprCostLimit bounds the CEL evaluation cost of one signal
// policy predicate, the same budget the other policy surfaces give a rule
// ([DefaultTaskPolicyRuleCostLimit]). It is spent on every delivery attempt,
// including those of a caller the predicate is about to refuse, so it is a
// work bound and not a reporting limit.
const SignalPolicyExprCostLimit uint64 = 50_000

// MaxSignalPolicyScopeBytes bounds the encoded run scope (its inputs and its
// starter) a run records so a signal predicate can read them at delivery time
// ([SignalPolicyExprReads]). A run whose predicate reads them and whose scope
// exceeds it is refused at submit rather than recorded truncated: a predicate
// evaluated over a partial copy of its inputs would be a different predicate.
const MaxSignalPolicyScopeBytes = 64 << 10

// signalPolicyIdentity is the identity shape a predicate reads, for both
// `sender.identity` and `run.identity`: the fields of [IdentityShape] a
// predicate may compare, as a native CEL type so a misspelled one is refused
// at compile time.
type signalPolicyIdentity struct {
	Principal string            `cel:"principal"`
	Subject   string            `cel:"subject"`
	Issuer    string            `cel:"issuer"`
	Namespace string            `cel:"namespace"`
	Claims    map[string]string `cel:"claims"`
}

// signalPolicyActor wraps an identity as the one field `sender` and `run` each
// expose.
type signalPolicyActor struct {
	Identity *signalPolicyIdentity `cel:"identity"`
}

// ext.NativeTypes names a type by the last element of its package *path*
// ("v1"), not its declared package name; pinned by a test.
const signalPolicyActorTypeName = "v1.signalPolicyActor"

var signalPolicyEnv = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv(
		ext.NativeTypes(ext.ParseStructTag("cel"),
			reflect.TypeFor[signalPolicyActor](), reflect.TypeFor[signalPolicyIdentity]()),
		cel.Variable("sender", cel.ObjectType(signalPolicyActorTypeName)),
		cel.Variable("run", cel.ObjectType(signalPolicyActorTypeName)),
		cel.Variable(InputsRoot, cel.MapType(cel.StringType, cel.DynType)),
		ext.Strings(ext.StringsVersion(5)),
	)
})

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
	// Run: `run.identity`, the starter, including its claims.
	Run bool
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
	env, err := signalPolicyEnv()
	if err != nil {
		return SignalPolicyPredicate{}, fmt.Errorf("building the signal policy environment: %w", err)
	}

	if strings.TrimSpace(src) == "" {
		return SignalPolicyPredicate{}, errors.New("the predicate is empty, so this policy authorizes nobody")
	}

	checked, issues := env.Compile(src)
	if issues.Err() != nil {
		return SignalPolicyPredicate{}, fmt.Errorf("%w; a signal policy predicate reads only %s",
			issues.Err(), signalPolicyScopeDescription)
	}

	rule, err := celrule.Build(env, checked, src, SignalPolicyExprCostLimit)
	if err != nil {
		return SignalPolicyPredicate{}, err
	}

	reads := signalPolicyReads(checked)
	if reads.inputs && !reads.claims && !reads.run {
		return SignalPolicyPredicate{}, errors.New(
			"the predicate reads `inputs` but nothing alongside it that the run's inputs cannot reach; " +
				"whoever starts the run chooses its inputs, so as written they may name themselves as their own " +
				"approver. Also compare `sender.identity.claims` or `run.identity` (for example " +
				"`sender.identity.principal != run.identity.principal`)")
	}

	return SignalPolicyPredicate{rule: rule, reads: SignalPolicyReads{Inputs: reads.inputs, Run: reads.run}}, nil
}

const signalPolicyScopeDescription = "`sender.identity.{principal,subject,issuer,namespace,claims}`, " +
	"`run.identity` (the starter, same fields) and `inputs`"

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
		if policy.GetAllowExpr() == "" {
			continue
		}
		if p, err := CompileSignalPolicyPredicate(policy.GetAllowExpr()); err == nil {
			reads.Inputs = reads.Inputs || p.reads.Inputs
			reads.Run = reads.Run || p.reads.Run
		}
	}

	return reads
}

type signalPolicyScopeReads struct {
	inputs, claims, run bool
}

// signalPolicyReads reports which of the narrowing-relevant names a checked
// predicate reads. Only names that resolve to the environment's own variables
// count: a comprehension variable that happens to be spelled `run` is the
// author's local ([signalPolicyIsLocal]) and must not satisfy the narrowing
// rule.
func signalPolicyReads(checked *cel.Ast) signalPolicyScopeReads {
	root := celast.NavigateAST(checked.NativeRep())

	global := func(e celast.NavigableExpr, name string) bool {
		return e.Kind() == celast.IdentKind && e.AsIdent() == name && !signalPolicyIsLocal(e)
	}

	var reads signalPolicyScopeReads
	for _, e := range celast.MatchDescendants(root, celast.AllMatcher()) {
		switch {
		case global(e, InputsRoot):
			reads.inputs = true
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
		}
	}

	return reads
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
	vars := map[string]any{
		"sender":   newSignalPolicyActor(identity),
		InputsRoot: signalPolicyInputsValue(inputs),
	}
	if hasStarter {
		vars["run"] = newSignalPolicyActor(starter)
	}

	return vars
}

func newSignalPolicyActor(identity *WorkloadIdentity) *signalPolicyActor {
	// The same rendering `run.identity` and a wait's `sender.identity` read,
	// so principal means one thing everywhere ([IdentityShape]).
	shape := IdentityShape(identity)
	claims := make(map[string]string, len(identity.GetClaims()))
	maps.Copy(claims, identity.GetClaims())

	return &signalPolicyActor{Identity: &signalPolicyIdentity{
		Principal: shape["principal"].(string),
		Subject:   shape["subject"].(string),
		Issuer:    shape["issuer"].(string),
		Namespace: shape["namespace"].(string),
		Claims:    claims,
	}}
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
// wraps nothing the evaluation produced.
func signalPolicyExprAllows(ctx context.Context, src string, identity, starter *WorkloadIdentity, hasStarter bool, inputs map[string]*Value) error {
	predicate, err := CompileSignalPolicyPredicate(src)
	if err != nil {
		return errors.New("this signal's allow predicate is not a valid policy, so no sender is authorized " +
			"until it is fixed")
	}

	allowed, err := predicate.rule.Match(ctx, signalPolicyActivation(identity, starter, hasStarter, inputs))
	if err != nil {
		// Deliberately not wrapped, formatted in or inspected: cel-go's
		// conversion errors quote their operand, and an operand can be an input
		// or a claim. One sentence for every cause.
		return errors.New("this signal's allow predicate could not be evaluated for this sender " +
			"(it errored, for example by reading the run's starter when none is recorded or a claim or " +
			"input that is missing, or it exceeded its cost bound), so the sender is refused")
	}
	if !allowed {
		return errors.New("the sender does not satisfy this signal's allow predicate")
	}

	return nil
}

// checkSignalPolicyExprShape is [CheckPolicyShape]'s half for a policy that
// sets allow_expr: the rule list and the expression are two mechanisms for one
// answer, so a policy sets one.
func checkSignalPolicyExprShape(where string, policy *SignalPolicy) error {
	if len(policy.GetAllow()) > 0 {
		return fmt.Errorf(
			"%s sets both the `allow:` rule list and an `allow:` predicate; a policy answers "+
				"who may act one way, so write one or the other", where)
	}
	if err := CheckSignalPolicyExpr(policy.GetAllowExpr()); err != nil {
		return fmt.Errorf("%s.allow is not a usable predicate: %w", where, err)
	}

	return nil
}
