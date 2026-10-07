package auth

import (
	"cmp"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/picatz/flowstate/internal/textbound"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// WorkloadIdentity is who a running workload is, and on whose behalf it acts.
//
// It is the outbound counterpart to [Principal]. A Principal is a caller
// Flowstate authenticated; a WorkloadIdentity is the identity a Flowstate
// workload carries when it goes out and calls something else. The server derives
// one from the Principal that submitted the run, and it travels with the run, so
// the last step of a long workload acts as the same identity as the first.
//
// It holds identity and never credentials, which is what makes it safe to
// persist in workflow history and to log.
//
// This mirrors the flowstate.v1.WorkloadIdentity protobuf message and the
// flowstate.v1.Principal inside it. The root package converts one to the other
// (`AuthIdentity` and `ProtoWorkloadIdentity`); this package deliberately does
// not import the generated types, so that the package defining them can import
// this one. [WorkloadIdentity.Caller] renders it as the one [principal.Caller]
// every policy surface binds.
type WorkloadIdentity struct {
	// Subject is the principal the workload acts for: the "sub" of the caller
	// who submitted the run, such as
	// "repo:picatz/flowstate:ref:refs/heads/main".
	Subject string

	// Issuer is where that principal came from, the "iss" of the token that
	// authenticated it.
	Issuer string

	// Claims are the claims an operator chose to carry from the submitting
	// caller's token, such as "repository", "email" or a "groups" list. Values
	// are JSON-shaped (string, bool, float64, nil, []any, map[string]any) and
	// bounded; see [MaxCarriedClaims].
	Claims map[string]any

	// Kind is the policy-assigned sort of party the caller is ("human",
	// "workload", "agent"), or empty when the admitting entry assigned none.
	Kind string

	// Actions are the action scopes the caller was granted. Empty means none
	// were carried, which a rule reads as no action matched.
	Actions []string

	// IssuerEntry is the name of the trust policy entry that admitted the caller.
	IssuerEntry string

	// Actors is the RFC 8693 `act` chain the caller's token carried, current
	// actor first (see [Principal.Actors]). It is data about who is acting for
	// Subject and never a source of authority: Actions already has the actors'
	// narrowing applied. Nil when the caller acts for themselves.
	Actors []principal.Actor

	// Namespace is the tenant or environment the workload runs in.
	Namespace string

	// Deployment names the Flowstate deployment running the workload.
	Deployment string

	// unreadable is why some claims of a wire identity could not be read within the
	// walk bounds ([WorkloadIdentity.WithWireClaims]). Nil for every identity that
	// was not read from the wire, and for one whose claims all were.
	unreadable error

	// local marks an identity minted by `flow run local` rather than by a
	// server-attested run. It is unexported and has no setter: a struct literal
	// built outside this package can never set it, however many fields it names,
	// so a flag, an environment variable, or an operator cannot forge it. The
	// only way to produce one is [NewLocalWorkloadIdentity], which the local
	// driver calls and the server driver never does. See that function and
	// [WorkloadIdentity.SubjectFor] for why the run mode has to live somewhere a
	// flag cannot reach.
	local bool
}

// isNilPointer reports whether an interface value holds a nil pointer, the shape
// an unset protobuf message field takes once it is assigned to an interface.
func isNilPointer(value any) bool {
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}

// IdentityFromPrincipal derives the identity a run should act as from the caller
// that submitted it.
//
// The namespace comes from the caller whenever the trust policy determined one,
// and the namespace argument is only the fallback for a deployment whose policy
// names none, meaning a single tenant. That precedence is the tenant boundary: a
// namespace supplied by the submitting request, rather than derived from the
// verified token, would let a caller choose its own tenant.
//
// The principal's claims are carried whole, whatever their JSON shape: a string,
// a list such as `groups`, or a nested object. They are already only what the
// admitting trust policy entry names in carry_claims and groups_claim, because a
// workload's identity should assert what a downstream relying party needs to
// authorize it, not everything the submitting caller's token happened to
// contain, and claims copied here can end up in an assertion sent to a third
// party.
//
// A claim over the carried-claim bounds (see [MaxCarriedClaims]) is refused by
// [WorkloadIdentity.Validate], never trimmed: a truncated list would grant on a
// prefix.
func IdentityFromPrincipal(principal Principal, namespace, deployment string) WorkloadIdentity {
	if principal.Namespace != "" {
		namespace = principal.Namespace
	}

	identity := WorkloadIdentity{
		Subject:     principal.Subject,
		Issuer:      principal.Issuer,
		IssuerEntry: principal.IssuerName,
		Namespace:   namespace,
		Kind:        string(principal.Kind),
		Actions:     slices.Clone(principal.Actions),
		Actors:      slices.Clone(principal.Actors),
		Deployment:  deployment,
	}

	// A principal's claims are already only what its trust policy entry carries
	// (see [MapClaims]), so all of them are the identity's.
	for name, value := range principal.Claims {
		if identity.Claims == nil {
			identity.Claims = make(map[string]any, len(principal.Claims))
		}
		identity.Claims[name] = cloneClaim(value)
	}

	return identity
}

// NewLocalWorkloadIdentity returns the identity `flow run local` mints for a
// rehearsal run.
//
// This is the only constructor that can produce an identity whose
// [WorkloadIdentity.SubjectFor] carries the [localComponent] segment, because
// it is the only code outside this package that can set the unexported local
// field — a struct literal cannot. The local driver calls this; the server
// driver builds an identity through [IdentityFromPrincipal]
// instead, and that does not set it either. So the distinction between a
// local rehearsal and a server-attested run is not something either driver
// remembers to apply — it is which constructor the call site is, and only one
// of the two call sites is this one. See [WorkloadIdentity.SubjectFor] for why
// AWS, GCP, and every other RFC 8693 peer treat the two as unrelated
// principals as a result, and not merely as differently labeled ones.
func NewLocalWorkloadIdentity(subject, issuer, namespace, deployment string, claims map[string]string) WorkloadIdentity {
	var carried map[string]any
	if len(claims) > 0 {
		carried = make(map[string]any, len(claims))
		for name, value := range claims {
			carried[name] = value
		}
	}

	return WorkloadIdentity{
		Subject:    subject,
		Issuer:     issuer,
		Namespace:  namespace,
		Deployment: deployment,
		Claims:     carried,
		local:      true,
	}
}

// Caller renders the identity as the [principal.Caller] every policy surface
// binds as `identity`: egress, exec, task shape, secret access and credential
// assumption all read the same value, so a clause about the caller means one
// thing wherever it is written. Namespace is the raw attested value, never
// [defaultComponent]: that substitution belongs to a minted subject, and a
// `deny: identity.namespace == "_default"` rule must match the same unnamespaced
// caller on every surface.
func (w WorkloadIdentity) Caller() principal.Caller {
	return principal.Caller{
		Issuer:    w.Issuer,
		Subject:   w.Subject,
		Namespace: w.Namespace,
		Kind:      w.Kind,
		Principal: principal.Qualified(w.Issuer, w.Subject),
		Claims:    principal.NewClaims(w.Claims),
		Actions:   w.Actions,
		Actors:    w.Actors,
	}.Normalized()
}

// IsLocalRehearsal reports whether this identity was created by
// [NewLocalWorkloadIdentity]. It exposes the fact without exposing a setter:
// the unexported marker remains the trust source, and an identity assembled by
// a caller or decoded from a wire message cannot make this return true.
func (w WorkloadIdentity) IsLocalRehearsal() bool { return w.local }

// IsZero reports whether the identity is unset.
func (w WorkloadIdentity) IsZero() bool {
	return w.unreadable == nil && w.Subject == "" && w.Issuer == "" && w.Namespace == "" && w.Deployment == "" && len(w.Claims) == 0
}

// String returns the identity in the form used in messages: the principal the
// workload acts for, qualified by where it runs.
func (w WorkloadIdentity) String() string {
	if w.IsZero() {
		return "no identity"
	}
	return fmt.Sprintf("%s/%s acting for %s", w.Namespace, w.Deployment, w.Subject)
}

// LogValue implements [slog.LogValuer], recording the identity without its
// carried claims, which may hold personal data.
func (w WorkloadIdentity) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("subject", w.Subject),
		slog.String("issuer", w.Issuer),
		slog.String("namespace", w.Namespace),
		slog.String("deployment", w.Deployment),
	)
}

// Bounds on the claims a [WorkloadIdentity] may carry into a minted assertion.
//
// A claim set is a wire format: it is signed, it travels to relying parties we
// do not control, and it is cached by them. So it is bounded like every other
// input in this repository, and an identity that exceeds a bound is **refused**
// rather than trimmed to fit — a truncated claim set is a token that says
// something other than what was authorized, and it says it under a signature.
//
// These are the Go half of the protovalidate rules on
// flowstate.v1.WorkloadIdentity.claims, which state the same numbers where
// `buf breaking` guards them. They are stated once here and read by both
// [WorkloadIdentity.Validate] and `Issuer.mintFor`, per CLAUDE.md's rule that
// one constant cannot disagree with itself.
//
// The numbers come from measuring what legitimate identities carry, with
// headroom:
//
//   - The largest carried claim set anywhere in this repository is three
//     (`repository`, `ref`, `job_workflow_ref`, on a GitHub Actions identity),
//     and the nearest schema neighbour that bounds a claim map at all,
//     the claim map a signal policy rule used to carry, allowed sixteen.
//     [MaxCarriedClaims] is 32.
//   - The longest claim *name* measured is 18 bytes (`runner_environment`);
//     [MaxCarriedClaimNameBytes] is that neighbour's own 128.
//   - The longest claim *value* measured is 63 bytes (a `job_workflow_ref`);
//     [MaxCarriedClaimValueBytes] is 1024, more than the neighbour's 256
//     because a carried value is data rather than a match pattern.
//   - [MaxCarriedClaimBytes] bounds the total, because the per-claim bounds
//     multiply: 32 claims at their individual maxima would be 36 KiB, which is
//     most of a [maxTokenBytes] token spent on carried claims alone. 8 KiB is
//     twelve times the largest realistic claim set measured (~640 bytes) and
//     leaves a minted assertion comfortably inside what any verifier will read.
const (
	// MaxCarriedClaims is how many claims an identity may carry.
	MaxCarriedClaims = 32

	// MaxCarriedClaimNameBytes bounds one claim's name.
	MaxCarriedClaimNameBytes = 128

	// MaxCarriedClaimValueBytes bounds one claim's value.
	MaxCarriedClaimValueBytes = 1024

	// MaxCarriedClaimBytes bounds the names and values together, which is the
	// bound the per-claim ones do not imply.
	MaxCarriedClaimBytes = 8 << 10
)

// validateCarriedClaims reports whether a claim set is within the bounds above.
//
// Claim *values* never appear in the errors it returns — only names, and only
// truncated. This string travels wherever the refusal does, which on the
// durable driver means Temporal's failure conversion and therefore workflow
// history; a claim value is exactly the kind of caller-supplied data nobody
// audited before it got there. The name is enough to fix the configuration,
// and is the half the operator already wrote down. See
// [describePolicyIdentity] in the flowstate package for the same reasoning
// applied to the same data.
//
// [describePolicyIdentity]: https://github.com/picatz/flowstate/blob/main/pkg/flowstate/v1/taskpolicy.go
func validateCarriedClaims(claims map[string]any) error {
	if len(claims) > MaxCarriedClaims {
		return fmt.Errorf("%w: identity carries %d claims, and an assertion may carry at most %d",
			ErrInvalidIdentity, len(claims), MaxCarriedClaims)
	}

	total := 0
	for _, name := range slices.Sorted(maps.Keys(claims)) {
		switch {
		case name == "":
			return fmt.Errorf("%w: identity carries a claim with no name", ErrInvalidIdentity)
		case len(name) > MaxCarriedClaimNameBytes:
			return fmt.Errorf("%w: carried claim name %q is %d bytes, and at most %d are allowed",
				ErrInvalidIdentity, textbound.Truncate(name, 64), len(name), MaxCarriedClaimNameBytes)
		}

		// The value's size, never the value.
		if err := checkCarriedClaim(name, claims[name]); err != nil {
			return err
		}

		size, _ := claimBytes(claims[name])
		if text, ok := claims[name].(string); ok {
			size = len(text)
		}
		total += len(name) + size
	}

	if total > MaxCarriedClaimBytes {
		return fmt.Errorf("%w: identity carries %d bytes of claims, and at most %d are allowed",
			ErrInvalidIdentity, total, MaxCarriedClaimBytes)
	}

	return nil
}

// StepRef points at the unit of work an assertion is minted for: which step, of
// which run, of which workload.
//
// A credential is obtained for one step of one run, so that a relying party's
// authorization can be as narrow as one step of one workload, and so that a
// credential leaked from one step is not usable as another.
//
// It is deliberately not called Scope. flowstate.v1.Scope is the scope an
// expression is evaluated in, which is bound variables and earlier step outputs;
// this is a location in an execution. Two types called Scope, one of them a proto
// that travels to workers, would be a lasting source of confusion.
type StepRef struct {
	// Workflow is the workload's name, which is stable across runs and is
	// therefore what a relying party's policy names.
	Workflow string

	// Run identifies the individual execution. It is carried as a claim for
	// audit, and deliberately not part of the subject: a relying party cannot
	// enumerate run identifiers in advance.
	Run string

	// Step is the step within the workload that is asking for the credential.
	Step string
}

// IsZero reports whether the reference is unset.
func (s StepRef) IsZero() bool {
	return s == StepRef{}
}

// subjectSeparator joins the components of an assertion subject. Components may
// not contain it, so a subject can be split back into exactly the parts that
// produced it, and cannot be forged by a component that spans two levels.
const subjectSeparator = "/"

// subjectPrefix marks a subject as one Flowstate minted, so a relying party
// trusting several issuers can tell at a glance which one a subject belongs to.
const subjectPrefix = "flowstate:"

// defaultComponent stands in for a namespace or deployment an operator has not
// set, so that a subject always has the same number of components. Without it, a
// workload with no deployment would produce a subject that a prefix rule written
// for a different level would match.
//
// It begins with an underscore, which [ValidateNamespace] forbids, so no tenant
// can name itself into this component: a namespace literally called "default"
// mints "flowstate:default/...", never "flowstate:_default/...". Without the
// underscore, a deployment that ran single-tenant, had a trust policy written for
// "flowstate:default/prod/...", and later admitted a tenant that happened to be
// named "default" would find that tenant inheriting the single-tenant grant. This
// mirrors [secrets.DefaultNamespaceDir] in the file secrets provider, which
// solves the identical problem the identical way — see its doc comment.
const defaultComponent = "_default"

// localComponent marks a subject minted by `flow run local` rather than by a
// server-attested run, as the leading component of the subject:
//
//	flowstate:_local/<namespace>/<deployment>/<workflow>/<step>
//
// It begins with an underscore for the same reason [defaultComponent] does: no
// namespace can ever equal it, because [ValidateNamespace] forbids the
// character. That is what makes it a property of the subject a relying party
// can rely on rather than a convention an operator has to trust: a trust policy
// written for "flowstate:acme/..." cannot match "flowstate:_local/acme/...",
// on AWS, GCP, or any other RFC 8693 peer, because prefix and exact-match rules
// both compare bytes and the leading segment differs. A custom claim would not
// have this property — AWS ignores claims it was not asked to put in the
// subject or audience, so a marker that lived only in a claim would be
// unenforceable on the relying party this matters most for. See
// [WorkloadIdentity.SubjectFor].
const localComponent = "_local"

// SubjectFor returns the subject a minted assertion will carry for this identity
// and step:
//
//	flowstate:<namespace>/<deployment>/<workflow>/<step>          server-attested
//	flowstate:_local/<namespace>/<deployment>/<workflow>/<step>   flow run local
//
// The shape is fixed and hierarchical so that a relying party can authorize at
// whatever level it wants with a prefix match: a whole namespace, one
// deployment, one workload, or a single step. An operator writing a cloud trust
// policy needs the exact string, so this is exported and used by minting rather
// than being computed twice.
//
// Two components are reserved and begin with an underscore, which
// [ValidateNamespace] forbids in a namespace: [defaultComponent], substituted
// for a namespace or deployment nobody set, and [localComponent], prepended
// when the identity came from [NewLocalWorkloadIdentity]. Neither can be
// forged by an operator-chosen namespace, because the grammar that admits a
// namespace into this subject is the same grammar that refuses the underscore
// — see [ValidateNamespace]. A local rehearsal run therefore mints a subject
// that is byte-distinguishable from every server-attested one, by
// construction, and no trust policy written for the latter can ever match the
// former.
//
// The run identifier is not part of the subject; it travels as a claim.
func (w WorkloadIdentity) SubjectFor(ref StepRef) (string, error) {
	if err := ValidateNamespace(w.Namespace); err != nil {
		return "", fmt.Errorf("%w: namespace: %w", ErrInvalidIdentity, err)
	}

	components := []string{
		orDefault(w.Namespace),
		orDefault(w.Deployment),
		ref.Workflow,
		ref.Step,
	}

	names := []string{"namespace", "deployment", "workflow", "step"}
	for i, component := range components {
		switch {
		case component == "":
			return "", fmt.Errorf("%w: %s is required to name a workload", ErrInvalidIdentity, names[i])
		case i >= 2 && component == anyComponent:
			// A subject reduced to a coarser level spells the dropped
			// components this way, so a workflow or step with the name would
			// collide with it on every minting path, not only a levelled one.
			return "", fmt.Errorf("%w: %q is reserved for subjects reduced to a coarser level and cannot name a %s",
				ErrInvalidIdentity, anyComponent, names[i])
		case strings.ContainsAny(component, subjectSeparator+":"):
			// Otherwise one component could spell out several, and a subject
			// could be made to look like a different workload's.
			return "", fmt.Errorf("%w: %s %q must not contain %q or %q",
				ErrInvalidIdentity, names[i], textbound.Truncate(component, 64), subjectSeparator, ":")
		}
	}

	if w.local {
		components = append([]string{localComponent}, components...)
	}

	return subjectPrefix + strings.Join(components, subjectSeparator), nil
}

// anyComponent stands in for a subject component a [SubjectLevel] drops, so a
// level-reduced subject keeps the arity of the full one and a prefix or
// StringLike pattern written for the full shape still lines up with it:
//
//	flowstate:acme/prod/deploy/_any        workflow level
//	flowstate:acme/prod/_any/_any          deployment level
//
// It begins with an underscore for the reason [defaultComponent] does, and
// [WorkloadIdentity.SubjectAt] refuses a real workflow or step of this name,
// so a workload can never name itself into a coarser subject than the one its
// target asked for.
const anyComponent = "_any"

// A SubjectLevel is how much of a workload's position the assertion subject a
// federation target receives names. It exists because relying parties differ in
// what they can match: Azure federated identity credentials compare the subject
// as an exact string with an application quota in the tens, and GCP's
// `google.subject` is length-limited, so a per-step subject does not fit either,
// while an AWS trust policy matches prefixes and wants the finest grain it can
// get.
//
// The level changes the subject a relying party reads and nothing Flowstate
// decides: the assumption policy still evaluates the workload's real step, so a
// coarser subject never widens what is allowed, only what the relying party can
// tell apart.
type SubjectLevel string

const (
	// SubjectLevelStep names the whole position, which is the default and the
	// subject [WorkloadIdentity.SubjectFor] returns.
	SubjectLevelStep SubjectLevel = "step"

	// SubjectLevelWorkflow drops the step: every step of one workflow in one
	// deployment shares a subject.
	SubjectLevelWorkflow SubjectLevel = "workflow"

	// SubjectLevelDeployment drops the workflow and the step: every workflow of
	// one deployment shares a subject.
	SubjectLevelDeployment SubjectLevel = "deployment"
)

// Valid reports whether the level is one of the three spellings. The zero
// value is not valid here; a target that names no level is [SubjectLevelStep],
// and [SubjectLevel.OrDefault] says so.
func (l SubjectLevel) Valid() bool {
	switch l {
	case SubjectLevelStep, SubjectLevelWorkflow, SubjectLevelDeployment:
		return true
	}
	return false
}

// OrDefault returns [SubjectLevelStep] for the unset level.
func (l SubjectLevel) OrDefault() SubjectLevel { return cmp.Or(l, SubjectLevelStep) }

// SubjectAt returns the subject [WorkloadIdentity.SubjectFor] would, reduced to
// level: components below it are [anyComponent]. A level that is not one of the
// three is refused, never read as the default, so a misspelling cannot silently
// widen or narrow a trust grant.
//
// A workflow or step literally named "_any" is refused by [WorkloadIdentity.SubjectFor]
// whatever the level, since it would be indistinguishable from a dropped component.
func (w WorkloadIdentity) SubjectAt(ref StepRef, level SubjectLevel) (string, error) {
	level = level.OrDefault()
	if !level.Valid() {
		return "", fmt.Errorf("%w: subject level %q is not one of step, workflow, deployment",
			ErrInvalidIdentity, textbound.Truncate(string(level), 32))
	}

	subject, err := w.SubjectFor(ref)
	if err != nil || level == SubjectLevelStep {
		return subject, err
	}

	// SubjectFor validated every component and fixed the arity, so the dropped
	// components are the last of a split on the separator.
	parts := strings.Split(subject, subjectSeparator)
	dropped := 1
	if level == SubjectLevelDeployment {
		dropped = 2
	}
	for i := len(parts) - dropped; i < len(parts); i++ {
		parts[i] = anyComponent
	}

	return strings.Join(parts, subjectSeparator), nil
}

// orDefault substitutes the placeholder for an unset subject component.
func orDefault(component string) string {
	if component == "" {
		return defaultComponent
	}
	return component
}

// Validate reports whether the identity is usable for minting an assertion.
//
// An identity with no subject or issuer is one Flowstate never established.
// Minting for it would produce an assertion that asserts nothing about who the
// workload acts for, which a relying party would nonetheless accept as a
// Flowstate workload.
func (w WorkloadIdentity) Validate() error {
	if w.unreadable != nil {
		return w.unreadable
	}

	switch {
	case w.IsZero():
		return fmt.Errorf("%w: no identity was established for this workload", ErrInvalidIdentity)
	case w.Subject == "":
		return fmt.Errorf("%w: identity has no subject", ErrInvalidIdentity)
	case w.Issuer == "":
		return fmt.Errorf("%w: identity has no issuer", ErrInvalidIdentity)
	}

	for _, name := range slices.Sorted(maps.Keys(w.Claims)) {
		if slices.Contains(builtInClaimNames, name) {
			// A carried claim that shadowed a reserved one would let whoever
			// controls the submitting token dictate the minted assertion.
			return fmt.Errorf("%w: carried claim %q collides with the reserved claim of the same name",
				ErrInvalidIdentity, name)
		}
	}

	if err := validateActors(w.Actors); err != nil {
		return err
	}

	return validateCarriedClaims(w.Claims)
}

// validateActors holds an identity's actor chain to the bounds the verifier
// admits it under, so a chain that arrived some other way (a wire message, a
// hand-built identity) is refused rather than trusted to have been checked.
func validateActors(actors []principal.Actor) error {
	if len(actors) > MaxActorDepth {
		return fmt.Errorf("%w: identity names %d actors, and at most %d are allowed", ErrInvalidIdentity, len(actors), MaxActorDepth)
	}
	for _, actor := range actors {
		switch {
		case actor.Issuer == "" || actor.Subject == "":
			return fmt.Errorf("%w: an actor needs both an issuer and a subject", ErrInvalidIdentity)
		case len(actor.Issuer) > MaxActorFieldBytes || len(actor.Subject) > MaxActorFieldBytes:
			return fmt.Errorf("%w: an actor's issuer and subject are at most %d bytes", ErrInvalidIdentity, MaxActorFieldBytes)
		}
	}

	return nil
}
