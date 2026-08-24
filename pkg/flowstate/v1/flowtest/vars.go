package flowtest

import (
	"fmt"
	"regexp"
	"strings"
)

// File-level `vars:` (#1072, slice 2): the values a suite states once and
// references everywhere — a URL, an id, a payload fragment — so the fixture
// is DRY without becoming a program.
//
// # Literals now, and the door computed fixtures will use later
//
// A var holds a literal, only: any `${` inside one is refused, including a
// reference to another var, so there is no evaluation order to define and no
// cycle to detect. #1072 records the trajectory — computed vars are the
// likely destination — and this slice's whole job is to fix the reference
// spelling and the scope naming so that arriving there respells nothing.
//
// # One spelling, two mechanisms, and the asymmetry that keeps it honest
//
// A *fixture* position — a case's `inputs:`, a trigger's fields, a scripted
// sender, `expect.outputs:` — references a var as a whole-value `${vars.x}`
// fence, and the reference is resolved AT LOAD, by substitution: what reaches
// the run is the literal, so the #416 fixture rule ("a default holds no
// expression") is not weakened, it is satisfied by the time it is checked. A
// *claim* position — `expect.check:` — reads `vars.x` at evaluation, bound as
// the check activation's `vars` root.
//
// A *stub* position (`where:`, `returns:`) is deliberately NEITHER: a stub's
// expressions evaluate against the run's own scope, where `vars.` has always
// meant the workflow's `vars:` block, and a load-time substitution there
// would silently hijack that meaning. A stub speaks the run's language;
// everywhere else in the test file, `vars.` is the file's. The
// disambiguation is pinned by TestAStubsVarsAreTheWorkflowsNotTheFiles.

// MaxVarsPerFile bounds how many vars one file may declare. A test file is
// untrusted input (CLAUDE.md); each var is substituted into every position
// that references it, so the resource the author controls is the walk this
// package does per reference, and 200 is far past what a fixture needs.
const MaxVarsPerFile = 200

// varReference matches a whole-value reference: `${vars.<name>}` and nothing
// around it. The name grammar is CEL's identifier grammar, because a var must
// also be reachable as `vars.<name>` inside a check.
var varReference = regexp.MustCompile(`^\$\{\s*vars\.([A-Za-z_][A-Za-z0-9_]*)\s*\}$`)

// varName is the same grammar, for declaration-side validation.
var varName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// checkVars validates the block itself: bounded, CEL-addressable names, and
// literal values all the way down.
func checkVars(vars map[string]any) error {
	if len(vars) > MaxVarsPerFile {
		return fmt.Errorf("this file declares %d vars, more than the limit of %d", len(vars), MaxVarsPerFile)
	}
	for name, value := range vars {
		if !varName.MatchString(name) {
			return fmt.Errorf("vars.%s: a var's name must be a CEL identifier (letters, digits, underscores, "+
				"not starting with a digit), or `vars.%s` could never be read back", name, name)
		}
		if err := checkNoExpressions("vars."+name, value, 0); err != nil {
			return err
		}
	}
	return nil
}

// resolveVars substitutes every whole-value `${vars.x}` reference in the
// file's fixture positions, in place, before tables expand and before
// `defaults:` is validated — so an inherited value resolves once and the
// fixture rule checks what the run will actually see.
func (f *File) resolveVars() error {
	// With no vars, a `${vars.x}` reference is a mistake worth naming rather
	// than a string worth passing through; the walk below answers that with
	// "names no var" either way, so it runs regardless.
	for i := range f.Tests {
		if err := resolveVarsInTest(fmt.Sprintf("tests[%d]", i), &f.Tests[i], f.Vars); err != nil {
			return err
		}
	}
	if d := f.Defaults; d != nil {
		if err := resolveVarsInString("defaults.workflow", &d.Workflow, f.Vars); err != nil {
			return err
		}
		if err := resolveVarsInMap("defaults.inputs", d.Inputs, f.Vars); err != nil {
			return err
		}
		if err := resolveVarsInIdentity("defaults.sender", d.Sender, f.Vars); err != nil {
			return err
		}
		// defaults.stubs and defaults.check: deliberately untouched, the
		// stub/claim halves of the asymmetry above.
	}
	return nil
}

// resolveVarsInTest covers one case's fixture positions — and its rows',
// because this runs before [expandTableEntries] and a row is a case.
func resolveVarsInTest(where string, test *Test, vars map[string]any) error {
	if err := resolveVarsInString(where+".workflow", &test.Workflow, vars); err != nil {
		return err
	}
	if err := resolveVarsInMap(where+".inputs", test.Inputs, vars); err != nil {
		return err
	}
	for name := range test.Secrets {
		value := test.Secrets[name]
		if err := resolveVarsInString(fmt.Sprintf("%s.secrets[%q]", where, name), &value, vars); err != nil {
			return err
		}
		test.Secrets[name] = value
	}
	if trigger := test.Trigger; trigger != nil {
		for field, target := range map[string]*string{
			".trigger.webhook": &trigger.Webhook, ".trigger.payload": &trigger.Payload,
			".trigger.kind": &trigger.Kind, ".trigger.name": &trigger.Name,
			".trigger.principal": &trigger.Principal, ".trigger.delivery_id": &trigger.DeliveryID,
		} {
			if err := resolveVarsInString(where+field, target, vars); err != nil {
				return err
			}
		}
	}
	for i := range test.Signals {
		signal := &test.Signals[i]
		prefix := fmt.Sprintf("%s.signals[%d]", where, i)
		if err := resolveVarsInString(prefix+".name", &signal.Name, vars); err != nil {
			return err
		}
		if err := resolveVarsInMap(prefix+".payload", signal.Payload, vars); err != nil {
			return err
		}
		if err := resolveVarsInIdentity(prefix+".sender", signal.Sender, vars); err != nil {
			return err
		}
	}
	if err := resolveVarsInIdentity(where+".starter", test.Starter, vars); err != nil {
		return err
	}
	if err := resolveVarsInMap(where+".expect.outputs", test.Expect.Outputs, vars); err != nil {
		return err
	}
	if err := resolveVarsInMap(where+".expect.inputs", test.Expect.Inputs, vars); err != nil {
		return err
	}
	// expect.check: a claim position; `vars.` binds at evaluation instead.
	// stubs: the run's language; see the package comment above.
	for i := range test.Cases {
		if err := resolveVarsInTest(fmt.Sprintf("%s.cases[%d]", where, i), &test.Cases[i], vars); err != nil {
			return err
		}
	}
	return nil
}

func resolveVarsInIdentity(where string, identity *ScriptedIdentity, vars map[string]any) error {
	if identity == nil {
		return nil
	}
	for field, target := range map[string]*string{
		".subject": &identity.Subject, ".issuer": &identity.Issuer, ".namespace": &identity.Namespace,
	} {
		if err := resolveVarsInString(where+field, target, vars); err != nil {
			return err
		}
	}
	for name := range identity.Claims {
		value := identity.Claims[name]
		if err := resolveVarsInString(fmt.Sprintf("%s.claims[%q]", where, name), &value, vars); err != nil {
			return err
		}
		identity.Claims[name] = value
	}
	return nil
}

// resolveVarsInMap substitutes through one decoded YAML tree, in place at the
// top level and by rebuild below it. Depth is bounded for the reason every
// walk here is ([maxDefaultsDepth]): the tree is an outside party's.
func resolveVarsInMap(where string, m map[string]any, vars map[string]any) error {
	for key, value := range m {
		resolved, err := resolveVarsInValue(fmt.Sprintf("%s.%s", where, key), value, vars, 0)
		if err != nil {
			return err
		}
		m[key] = resolved
	}
	return nil
}

func resolveVarsInValue(where string, value any, vars map[string]any, depth int) (any, error) {
	if depth > maxDefaultsDepth {
		return nil, fmt.Errorf("%s: nests more than %d levels deep", where, maxDefaultsDepth)
	}
	switch v := value.(type) {
	case string:
		return substituteVar(where, v, vars)
	case map[string]any:
		for key, inner := range v {
			resolved, err := resolveVarsInValue(fmt.Sprintf("%s.%s", where, key), inner, vars, depth+1)
			if err != nil {
				return nil, err
			}
			v[key] = resolved
		}
		return v, nil
	case []any:
		for i, inner := range v {
			resolved, err := resolveVarsInValue(fmt.Sprintf("%s[%d]", where, i), inner, vars, depth+1)
			if err != nil {
				return nil, err
			}
			v[i] = resolved
		}
		return v, nil
	default:
		return value, nil
	}
}

func resolveVarsInString(where string, target *string, vars map[string]any) error {
	resolved, err := substituteVar(where, *target, vars)
	if err != nil {
		return err
	}
	// A var holding a non-string cannot land in a position typed string: a
	// path, a subject, a signal name. Naming the type beats a later
	// far-from-here failure about a path that is a number.
	text, ok := resolved.(string)
	if !ok {
		return fmt.Errorf("%s references a var holding %T, and this position takes a string", where, resolved)
	}
	*target = text
	return nil
}

// substituteVar resolves one string: a whole-value `${vars.x}` becomes the
// var's literal (any type — a var can hold a map a payload position wants); a
// string merely *containing* `${vars.` is refused, because a partial
// substitution would be a template language this file deliberately is not;
// every other string passes through untouched, including the `${...}`
// expressions stub positions legitimately carry.
func substituteVar(where, s string, vars map[string]any) (any, error) {
	if match := varReference.FindStringSubmatch(s); match != nil {
		value, declared := vars[match[1]]
		if !declared {
			return nil, fmt.Errorf("%s references vars.%s, and this file's `vars:` names no %q",
				where, match[1], match[1])
		}
		return value, nil
	}
	if strings.Contains(s, "${vars.") {
		return nil, fmt.Errorf("%s mixes text with a vars reference (%q); a reference stands alone as the "+
			"whole value — build the combined text in the workflow, or state it literally", where, s)
	}
	return s, nil
}
