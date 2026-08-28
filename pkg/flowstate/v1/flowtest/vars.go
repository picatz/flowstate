package flowtest

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
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
//
// Every name is judged rather than the first bad one, and they are judged in
// sorted order, because a map's iteration order is not something a report may
// depend on — the rule [checkScriptedIdentity] already states for claims.
//
// Reports false when the count bound stopped it, which the loader takes as a
// refusal of the whole document: the walk below is per var, and a legal file
// can declare tens of thousands of them.
func checkVars(p *problems, vars map[string]any) bool {
	block := at("vars")
	if len(vars) > MaxVarsPerFile {
		p.report(site{at: block}, "this file declares %d vars, more than the limit of %d", len(vars), MaxVarsPerFile)

		return false
	}
	for _, name := range slices.Sorted(maps.Keys(vars)) {
		if !varName.MatchString(name) {
			p.reportKey(site{at: block.field(name)},
				"vars.%s: a var's name must be a CEL identifier (letters, digits, underscores, "+
					"not starting with a digit), or `vars.%s` could never be read back", name, name)
		}
		checkNoExpressions(p, site{at: block.field(name)}, "vars."+name, vars[name], 0)
	}

	return true
}

// resolveVars substitutes every whole-value `${vars.x}` reference in the
// file's fixture positions, in place, before tables expand and before
// `defaults:` is validated — so an inherited value resolves once and the
// fixture rule checks what the run will actually see.
//
// A reference that cannot resolve is reported and the walk carries on: the file
// is refused either way, so nothing downstream reads the half-substituted
// value, and an author gets every bad reference in one pass rather than one per
// run.
//
// Each position is named twice over, and deliberately: `where` is the prose a
// reader is given and `spot` is where the value was written. They are built one
// line apart at every call so a diagnostic cannot come to name one place and
// point at another. The two spellings differ where the prose already had its
// own (`secrets["vault:prod/db"]` quotes a key that holds a colon), which is
// the ambiguity [loc] exists to keep out of the addressing.
func (f *File) resolveVars(p *problems) {
	// With no vars, a `${vars.x}` reference is a mistake worth naming rather
	// than a string worth passing through; the walk below answers that with
	// "names no var" either way, so it runs regardless.
	for i := range f.Tests {
		resolveVarsInTest(p, fmt.Sprintf("tests[%d]", i), at("tests").item(i), &f.Tests[i], f.Vars)
	}
	if d := f.Defaults; d != nil {
		base := at("defaults")
		resolveVarsInString(p, "defaults.workflow", base.field("workflow"), &d.Workflow, f.Vars)
		resolveVarsInMap(p, "defaults.inputs", base.field("inputs"), d.Inputs, f.Vars)
		resolveVarsInIdentity(p, "defaults.sender", base.field("sender"), d.Sender, f.Vars)
		// defaults.stubs and defaults.check: deliberately untouched, the
		// stub/claim halves of the asymmetry above.
	}
}

// resolveVarsInTest covers one case's fixture positions — and its rows',
// because this runs before [expandTableEntries] and a row is a case.
func resolveVarsInTest(p *problems, where string, spot loc, test *Test, vars map[string]any) {
	resolveVarsInString(p, where+".workflow", spot.field("workflow"), &test.Workflow, vars)
	resolveVarsInMap(p, where+".inputs", spot.field("inputs"), test.Inputs, vars)
	for _, name := range slices.Sorted(maps.Keys(test.Secrets)) {
		value := test.Secrets[name]
		resolveVarsInString(p, fmt.Sprintf("%s.secrets[%q]", where, name),
			spot.field("secrets").field(name), &value, vars)
		test.Secrets[name] = value
	}
	if trigger := test.Trigger; trigger != nil {
		for _, field := range []struct {
			name   string
			target *string
		}{
			{"webhook", &trigger.Webhook}, {"payload", &trigger.Payload},
			{"kind", &trigger.Kind}, {"name", &trigger.Name},
			{"principal", &trigger.Principal}, {"delivery_id", &trigger.DeliveryID},
		} {
			resolveVarsInString(p, where+".trigger."+field.name,
				spot.field("trigger").field(field.name), field.target, vars)
		}
	}
	for i := range test.Signals {
		signal := &test.Signals[i]
		prefix := fmt.Sprintf("%s.signals[%d]", where, i)
		scripted := spot.field("signals").item(i)
		resolveVarsInString(p, prefix+".name", scripted.field("name"), &signal.Name, vars)
		resolveVarsInMap(p, prefix+".payload", scripted.field("payload"), signal.Payload, vars)
		resolveVarsInIdentity(p, prefix+".sender", scripted.field("sender"), signal.Sender, vars)
	}
	resolveVarsInIdentity(p, where+".starter", spot.field("starter"), test.Starter, vars)
	resolveVarsInMap(p, where+".expect.outputs", spot.field("expect").field("outputs"), test.Expect.Outputs, vars)
	resolveVarsInMap(p, where+".expect.inputs", spot.field("expect").field("inputs"), test.Expect.Inputs, vars)
	// expect.check: a claim position; `vars.` binds at evaluation instead.
	// stubs: the run's language; see the package comment above.
	for i := range test.Cases {
		resolveVarsInTest(p, fmt.Sprintf("%s.cases[%d]", where, i), spot.field("cases").item(i),
			&test.Cases[i], vars)
	}
}

func resolveVarsInIdentity(p *problems, where string, spot loc, identity *ScriptedIdentity, vars map[string]any) {
	if identity == nil {
		return
	}
	for _, field := range []struct {
		name   string
		target *string
	}{
		{"subject", &identity.Subject}, {"issuer", &identity.Issuer}, {"namespace", &identity.Namespace},
	} {
		resolveVarsInString(p, where+"."+field.name, spot.field(field.name), field.target, vars)
	}
	for _, name := range slices.Sorted(maps.Keys(identity.Claims)) {
		value := identity.Claims[name]
		resolveVarsInString(p, fmt.Sprintf("%s.claims[%q]", where, name),
			spot.field("claims").field(name), &value, vars)
		identity.Claims[name] = value
	}
}

// resolveVarsInMap substitutes through one decoded YAML tree, in place at the
// top level and by rebuild below it. Depth is bounded for the reason every
// walk here is ([maxDefaultsDepth]): the tree is an outside party's.
func resolveVarsInMap(p *problems, where string, spot loc, m map[string]any, vars map[string]any) {
	for _, key := range slices.Sorted(maps.Keys(m)) {
		m[key] = resolveVarsInValue(p, fmt.Sprintf("%s.%s", where, key), spot.field(key), m[key], vars, 0)
	}
}

// resolveVarsInValue returns the value with every reference in it resolved, or
// the value untouched where one could not be: a refused reference leaves what
// the author wrote in place, since the document is refused and nothing will
// read it.
func resolveVarsInValue(p *problems, where string, spot loc, value any, vars map[string]any, depth int) any {
	if depth > maxDefaultsDepth {
		p.report(site{at: spot}, "%s: nests more than %d levels deep", where, maxDefaultsDepth)

		return value
	}
	switch v := value.(type) {
	case string:
		return substituteVar(p, where, spot, v, vars)
	case map[string]any:
		for _, key := range slices.Sorted(maps.Keys(v)) {
			v[key] = resolveVarsInValue(p, fmt.Sprintf("%s.%s", where, key), spot.field(key), v[key], vars, depth+1)
		}

		return v
	case []any:
		for i, inner := range v {
			v[i] = resolveVarsInValue(p, fmt.Sprintf("%s[%d]", where, i), spot.item(i), inner, vars, depth+1)
		}

		return v
	default:
		return value
	}
}

func resolveVarsInString(p *problems, where string, spot loc, target *string, vars map[string]any) {
	resolved := substituteVar(p, where, spot, *target, vars)
	// A var holding a non-string cannot land in a position typed string: a
	// path, a subject, a signal name. Naming the type beats a later
	// far-from-here failure about a path that is a number.
	text, isText := resolved.(string)
	if !isText {
		p.report(site{at: spot}, "%s references a var holding %T, and this position takes a string", where, resolved)

		return
	}
	*target = text
}

// substituteVar resolves one string: a whole-value `${vars.x}` becomes the
// var's literal (any type — a var can hold a map a payload position wants); a
// string merely *containing* `${vars.` is refused, because a partial
// substitution would be a template language this file deliberately is not;
// every other string passes through untouched, including the `${...}`
// expressions stub positions legitimately carry.
//
// A refused reference resolves to the text as written, so that the caller's
// own type check does not then report a second problem about the same string.
func substituteVar(p *problems, where string, spot loc, s string, vars map[string]any) any {
	if match := varReference.FindStringSubmatch(s); match != nil {
		value, declared := vars[match[1]]
		if !declared {
			p.report(site{at: spot}, "%s references vars.%s, and this file's `vars:` names no %q",
				where, match[1], match[1])

			return s
		}

		return value
	}
	if strings.Contains(s, "${vars.") {
		p.report(site{at: spot}, "%s mixes text with a vars reference (%q); a reference stands alone as the "+
			"whole value — build the combined text in the workflow, or state it literally", where, s)
	}

	return s
}
