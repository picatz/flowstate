// Package principal is the CEL-typed rendering of an authenticated caller.
//
// Today it is the shape egress, exec and task-shape rules share: they bind the
// same [Caller] type, so a field added here is readable in all three at once.
// Secret access, credential assumption and signal predicates do not bind it
// yet; they migrate in later slices, and until then they keep their own
// spellings. The variable's name stays per surface (`identity`,
// `sender.identity`, `run.identity`); the type does not.
//
// The package is a leaf: it imports only the standard library and cel-go, so
// the packages that evaluate rules (netpolicy, execpolicy) and the root
// package that renders a run's attested identity into a [Caller] can all depend
// on it without a cycle. How a Caller is established is outside this package.
//
// # The claims carrier
//
// Claims are JSON-shaped, not strings: a rule reads `identity.claims.groups`
// as a list and `identity.claims.slack.user` as a nested object, and
// `"k" in identity.claims` guards an absent key. cel-go's NativeTypes drops any
// struct field whose Go type has no CEL type, which rules out an
// interface-valued map, so [Claims] is a struct that implements the CEL value
// interface itself and declares `map(string, dyn)`. Chosen over a custom
// type provider (a second place that knows the field list) and over a
// string-only map (which cannot carry a group list): it keeps one Caller type
// and one rendering, and reading an absent claim is the standard CEL "no such
// key" error, so a rule that names a claim the caller lacks denies rather than
// permits.
package principal

import (
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
)

// TypeName is how [Caller] is named in CEL, which appears in a type error when
// a rule misuses a field. [ext.NativeTypes] derives it from the type's Go
// directory, which for this package is also its declared name; the tests pin it
// so a move cannot silently rename the type operators see.
const TypeName = "principal.Caller"

// Caller is who is calling, as a rule reads it: `identity.<field>`.
//
// The zero value is "no attested caller": every string empty, no claims, no
// actions. That is what a local run or a scope that predates identity presents,
// and a rule that selects a tenant, kind, or action does not match it, which is
// the fail-closed reading: a request denied by every allow rule is denied.
//
// Kind is never read from a token; it is the name ("human", "workload",
// "agent") the operator's trust policy assigned, and is empty when it assigned
// none, so a predicate cannot mistake the absence for a workload.
type Caller struct {
	// Issuer is the identity provider that vouched for the subject.
	Issuer string `cel:"issuer"`
	// Subject identifies the caller within the issuer.
	Subject string `cel:"subject"`
	// Namespace is the tenant the caller belongs to.
	Namespace string `cel:"namespace"`
	// Kind is the lowercase principal kind, or empty when none was assigned.
	Kind string `cel:"kind"`
	// Principal is `issuer#subject`, and empty unless both halves are present.
	Principal string `cel:"principal"`
	// Claims are the non-secret claims an operator configured to carry, of any
	// JSON shape; see [Claims] for how rules read them.
	Claims Claims `cel:"claims"`
	// Actions are the scopes the caller was granted.
	Actions []string `cel:"actions"`
}

// Normalized returns the caller a rule is evaluated against: the same fields
// with actions guaranteed non-nil. CEL cannot take the size of a null list, so
// `"x" in identity.actions` against a caller that carries none would error, and
// an errored rule denies, where the intent is for it simply not to match. An
// absent claim key still errors, which is the documented convention
// (`"k" in identity.claims` guards it); [Claims] is already empty, never null.
func (c Caller) Normalized() Caller {
	if c.Actions == nil {
		c.Actions = []string{}
	}

	return c
}

// Map renders the caller as the plain map an expression reads where the engine
// builds the value itself rather than binding the typed [Caller]: a wait's
// `sender.identity` and a run's `run.identity`. It has the same keys as the CEL
// type's fields, so one rendering serves every surface; the zero Caller renders
// every string empty and claims and actions empty.
func (c Caller) Map() map[string]any {
	c = c.Normalized()

	return map[string]any{
		"issuer":    c.Issuer,
		"subject":   c.Subject,
		"namespace": c.Namespace,
		"kind":      c.Kind,
		"principal": c.Principal,
		"claims":    c.Claims.Map(),
		"actions":   c.Actions,
	}
}

// EnvOptions registers [Caller] as a CEL native type. Declaring the fields is
// what makes a rule naming `identity.nonexistent` a compile-time error rather
// than one that silently never matches.
func EnvOptions() cel.EnvOption {
	return ext.NativeTypes(ext.ParseStructTag("cel"), reflect.TypeFor[Caller]())
}

// Var declares name as a variable of type [Caller]. It does not register the
// type; pair it with [EnvOptions].
func Var(name string) cel.EnvOption {
	return cel.Variable(name, cel.ObjectType(TypeName))
}

// Qualified is the `principal` a rule compares: `issuer#subject` when both
// halves are present, and "" otherwise. The rule lives here so every way of
// building a [Caller] spells it once; an unauthenticated or half-formed caller
// is "", never "#" or "issuer#", and a predicate must treat "" as missing.
//
// The join is injective because no trusted issuer contains '#' (policy
// validation refuses one), so the first '#' always ends the issuer.
func Qualified(issuer, subject string) string {
	if issuer == "" || subject == "" {
		return ""
	}

	return issuer + "#" + subject
}
