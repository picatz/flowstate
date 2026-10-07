package flowstatev1

import (
	"fmt"
	"iter"
	"slices"
	"strings"
	"unicode"
)

// A [CredentialRef] is a [SecretRef]'s sibling and is held to the same
// containment, which is why this file mirrors structure.go's secret walk
// rather than inventing a second discipline.
//
// What it names is a federation target in the deployment's trust policy. The
// worker running the task is the only party that exchanges its workload
// identity for a credential, at the moment the task needs one, so the
// specification, the control plane and workflow code carry a target name and
// never a credential (invariant 7). Workflow-side evaluation refuses to read
// one (see [StepsOutputActivation]), a `vars:` block refuses to hold one (see
// [CheckVarsHoldNoSecretRef]), and a submission cannot choose one (see
// [BindRunInputs]).
//
// Resolving one is deliberately not here. This package owns the reference, its
// walks and its preflight; the host that mints the credential for a task input
// that accepts one builds on [ValueHoldsCredentialRef], [CredentialRefsIn] and
// [ValidateCredentialTarget].

// MaxCredentialTargetLen bounds a [CredentialRef]'s target, matching the
// schema's own limit so a hand-built specification is held to the same one as
// a compiled Flowfile.
const MaxCredentialTargetLen = 128

// NewCredentialRef returns a Value naming the federation target. It does not
// check that the target is well formed or configured; see
// [ValidateCredentialTarget] and [ValidateCredentialTargets].
func NewCredentialRef(target string) *Value {
	return &Value{Kind: &Value_CredentialRef{CredentialRef: &CredentialRef{Target: target}}}
}

// ValidateCredentialTarget reports whether target is a well-formed name for a
// [CredentialRef]: non-empty, within [MaxCredentialTargetLen], and free of
// control characters.
//
// It checks shape only. Whether the deployment federates a target is a fact
// about its configuration and is [ValidateCredentialTargets]' question. The
// control-character rule is secrets.ValidateRef's for the same reason: a
// name that reaches a log or an audit record must not be able to forge lines
// in it.
func ValidateCredentialTarget(target string) error {
	switch {
	case target == "":
		return fmt.Errorf("credential target must not be empty")
	case len(target) > MaxCredentialTargetLen:
		return fmt.Errorf("credential target is longer than %d characters", MaxCredentialTargetLen)
	}
	if i := strings.IndexFunc(target, unicode.IsControl); i >= 0 {
		return fmt.Errorf("credential target contains a control character at offset %d", i)
	}
	return nil
}

// ValueHoldsCredentialRef reports whether v is a credential reference or
// contains one at any depth.
//
// It answers [ValueHoldsSecretRef]'s question for the federation half: "does
// executing this need the authority to mint a credential". A compiled Flowfile
// only ever writes one as the whole value of a task input, but a specification
// can be built by something that never was a Flowfile, so the walk looks inside
// structures too and a value nested past [MaxStructureDepth] answers true:
// too deep to inspect may hold one, and every consumer fails closed rather than
// open at depth.
func ValueHoldsCredentialRef(v *Value) bool {
	for range credentialRefs(v) {
		return true
	}
	return false
}

// CredentialRefsIn returns every federation target a task's inputs name, sorted
// and de-duplicated.
//
// Names and never credentials, so it is as safe to log or attach to a span as
// [SecretRefsIn]. A value too deep to walk is skipped here and answered
// conservatively by [ValueHoldsCredentialRef].
func CredentialRefsIn(task *Task) []string {
	var targets []string
	for _, value := range task.GetInputs() {
		for ref := range credentialRefs(value) {
			if ref == nil {
				continue
			}
			targets = append(targets, ref.GetTarget())
		}
	}

	slices.Sort(targets)
	return slices.Compact(targets)
}

// credentialRefs is every credential reference v holds, at any depth, in the
// order [secretRefs] reads a structure in. It yields nil where the walk hit
// [MaxStructureDepth] and cannot see below, for the reason [secretRefs]
// documents.
func credentialRefs(v *Value) iter.Seq[*CredentialRef] {
	return func(yield func(*CredentialRef) bool) { yieldCredentialRefs(v, 0, yield) }
}

func yieldCredentialRefs(v *Value, depth int, yield func(*CredentialRef) bool) bool {
	if v == nil {
		return true
	}
	if depth > MaxStructureDepth {
		return yield(nil)
	}

	switch kind := v.GetKind().(type) {
	case *Value_CredentialRef:
		return yield(kind.CredentialRef)
	case *Value_Structure_:
		for _, entry := range StructureValues(kind.Structure) {
			if !yieldCredentialRefs(entry, depth+1, yield) {
				return false
			}
		}
	}

	return true
}
