package celcomplete

import (
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// MaxCandidates bounds one answer.
//
// The resource is the answer itself, and the quantity is chosen by whoever
// wrote the document or started the run rather than by whoever is typing: a
// workflow with a thousand steps offers a thousand step ids after `steps.`, and
// a scope carrying a large `inputs:` offers one candidate per key. Bounded here
// rather than at each caller, because a bound each surface applies for itself
// is one a new surface forgets.
//
// Large enough that no document or run this repository has ever seen reaches it
// — the whole profile is about sixty functions, and the examples' largest
// workflow has eleven steps — so the bound is a stop against a pathological
// input rather than a limit an author meets. [Result.Truncated] reports having
// reached it, so a short list is never mistaken for a complete one.
const MaxCandidates = 512

// A Kind says what sort of name a candidate is, so a surface can show it the
// way that surface shows one: an icon in an editor's popup, a word at a
// terminal.
//
// Deliberately this package's own small vocabulary rather than the protocol's
// [lsp.CompletionItemKind], for the reason [flowdebug.Tone] is not
// [flowtest.TranscriptTone]: the protocol has twenty-five members, twenty-one
// of which no expression can ever be, and a shared type would invite a caller
// to reach for one.
type Kind int

const (
	// KindValue is a name that resolves to a value: a loop's iterator, `now`
	// inside a wait, a step under `steps`, a declared variable under `vars`.
	// The zero value, because it is what most names are.
	KindValue Kind = iota

	// KindRoot is a rooted namespace — `steps`, `vars`, `inputs` — which is
	// never the whole of a reference.
	KindRoot

	// KindField is a name reachable only inside another value: one of a step's
	// outputs. Distinguished from [KindValue] because it is not a name an
	// expression may write on its own.
	KindField

	// KindFunction is a function the profile declares.
	KindFunction

	// KindNamespace is a qualifier a function is written on, `math` in
	// `math.greatest(1, 2)`. Like a root, never the whole of an expression.
	KindNamespace
)

// A Candidate is one name that may be written at the cursor.
//
// It carries what it exposes after a dot ([Candidate.Members]) rather than a
// caller looking that up separately, which is what makes `steps.<id>.<output>`
// one structure with three levels instead of three lookups that can disagree
// about which step exists.
type Candidate struct {
	// Name is the name itself, and what a surface labels the offer with.
	Name string

	// Detail is the one line beside the name: a type, a kind of step, the
	// library a function came from.
	Detail string

	// Docs is the longer prose, for a surface that has room for it.
	Docs string

	// Insert is the text written when the candidate is accepted, where that
	// differs from Name. A root and a namespace are the two that differ: both
	// are only ever the front of something, so the dot that continues them is
	// written too — the same reason a key is offered with its colon.
	Insert string

	// Kind distinguishes the sorts of name, for a surface that shows the
	// difference.
	Kind Kind

	// Members are the names a dot after this one reaches. A candidate with
	// none — a loop iterator, whose element type is not statically known —
	// offers nothing after the dot rather than guessing.
	Members []Candidate

	// Truncated reports that Members is a *prefix* of what this name reaches,
	// because the caller bounded the collection rather than handing over
	// everything it had. Completing into such a candidate sets
	// [Result.Truncated], so an answer never quietly presents a prefix as the
	// whole of what a name offers.
	Truncated bool
}

// Text is what accepting this candidate writes.
func (c Candidate) Text() string {
	if c.Insert != "" {
		return c.Insert
	}

	return c.Name
}

// Continues reports whether accepting this candidate leaves the reference
// unfinished — true exactly of the names offered with their dot, which is what
// tells a terminal not to write a space after one.
func (c Candidate) Continues() bool {
	return strings.HasSuffix(c.Text(), ".")
}

// A Scope is what an expression may name at one point, in the two shapes the
// grammar keeps apart.
//
// It mirrors the refScope the validator carries, deliberately: a surface
// offering a name the compiler would refuse is the one failure this must not
// have, and a scope shaped like the compiler's cannot drift into one that
// merges the two namespaces again.
type Scope struct {
	// Profile is the language profile whose functions are offered. Empty reads
	// as [v1.OriginalProfile], the same answer [v1.ProfileLibraries] gives a
	// specification compiled before the field existed.
	Profile string

	// Locals are the names bound bare where the cursor is. Offered bare and
	// never after a root.
	Locals []Candidate

	// Roots are the rooted namespaces this scope has, in the order they should
	// be offered — [StepsRoot], [VarsRoot], [InputsRoot] and whatever a caller
	// adds. Each is offered bare, with the dot that continues it, and its
	// Members are what follow the dot.
	//
	// Which roots a scope has is the caller's answer rather than this
	// package's, because it differs by surface for a real reason: an editor
	// offers `steps` before any step exists, since the first step of a file is
	// written before there is anything to reference, and offers `vars` only
	// where the file declares one, since a root that resolves to an empty map
	// is a name nobody should be taught.
	Roots []Candidate
}

// A Result is one answer.
type Result struct {
	// Prefix is the partial name at the cursor, which accepting a candidate
	// replaces. It is the text after the last dot, or the whole word where
	// there is no dot.
	Prefix string

	// Candidates are the offers, in the order they should be shown.
	Candidates []Candidate

	// Truncated reports that [MaxCandidates] was reached and the answer is a
	// prefix of what matched.
	Truncated bool
}

// Complete offers what may be written at the end of text, which is the
// expression source up to the cursor.
//
// Three positions, because a root has depth: bare at the start of an
// expression, a root's members after `<root>.`, and one member's own members
// after `<root>.<member>.`. Splitting on the *last* dot is what makes the
// middle one reachable — the qualifier there is two segments, where before
// rooting every qualifier was one.
func Complete(text string, scope Scope) Result {
	word := TrailingWord(text)

	dot := strings.LastIndex(word, ".")
	if dot < 0 {
		return bound(word, bareCandidates(scope))
	}

	qualifier, member := word[:dot], word[dot+1:]

	// A root, and then one of its members. Answered before the profile's
	// namespaces below, so that a workflow whose author declared a var called
	// `math` still completes their var: the rooted namespaces are the
	// language's, and a function qualifier is only reached where no root
	// claims the name.
	if root, ok := find(scope.Roots, qualifier); ok {
		return carry(root, bound(member, root.Members))
	}
	if head, rest, nested := strings.Cut(qualifier, "."); nested {
		if root, ok := find(scope.Roots, head); ok {
			// One member deep, and the root's answer either way. A member that
			// is not there and a member with nothing under it come to the same
			// empty answer, which is the honest one: past a member is a value
			// whose shape nothing here describes, and past the root is a name
			// nothing produced. Guessing at either is how a surface starts
			// offering references the engine rejects.
			inner, _ := find(root.Members, rest)

			// Either level's truncation is the answer's: a member list that is
			// a prefix cannot be reported as complete just because the level
			// below it was whole.
			return carry(root, carry(inner, bound(member, inner.Members)))
		}
	}

	if fns := FunctionsAfter(scope.Profile, qualifier); fns != nil {
		return bound(member, fns)
	}

	// A bare qualifier. Either a binding, whose element type is not known
	// statically, or the retired spelling of a step reference — and offering
	// that one's members would keep an author writing a form `flow validate`
	// refuses.
	return Result{Prefix: member}
}

// bareCandidates is what may be written bare at the start of an expression: the
// names bound where the cursor is, the roots, and then the profile's functions.
//
// Bindings come first because they are the nearer thing — bound by the block
// the cursor stands in, where a root spans the whole document — and because
// inside a loop body the item is usually what is wanted.
//
// Functions come last, and there are a lot of them. That ordering is the whole
// of the design decision: someone who knows the name they want types it and the
// prefix filter does the work, while someone who does not gets the names in
// scope first rather than having to scroll past sixty functions to find the
// loop variable they bound two lines up.
func bareCandidates(scope Scope) []Candidate {
	candidates := slices.Clone(scope.Locals)
	candidates = append(candidates, scope.Roots...)

	return append(candidates, FunctionCandidates(scope.Profile)...)
}

// find returns the candidate with a name, if the list has one.
func find(candidates []Candidate, name string) (Candidate, bool) {
	for _, c := range candidates {
		if c.Name == name {
			return c, true
		}
	}

	return Candidate{}, false
}

// carry propagates a candidate's own truncation into an answer drawn from its
// members, so a prefix is never presented as the whole of what a name offers.
func carry(from Candidate, out Result) Result {
	out.Truncated = out.Truncated || from.Truncated

	return out
}

// bound filters candidates by prefix and applies [MaxCandidates].
//
// The order candidates arrive in is kept: it is the order they should be
// offered in, decided by whoever assembled the scope, and sorting here would
// throw away the one piece of judgement a list of names carries.
func bound(prefix string, candidates []Candidate) Result {
	out := Result{Prefix: prefix}
	for _, c := range candidates {
		if !strings.HasPrefix(c.Name, prefix) {
			continue
		}
		if len(out.Candidates) == MaxCandidates {
			out.Truncated = true

			break
		}
		out.Candidates = append(out.Candidates, c)
	}

	return out
}

// TrailingWord returns the reference being written at the end of s: the run of
// name characters and dots it ends with.
//
// Dots are part of the word because a reference is one name with parts, and the
// caller splits it. Everything else ends it, which is what makes `size(steps.`
// complete against `steps` rather than against `size(steps`.
func TrailingWord(s string) string {
	i := len(s)
	for i > 0 && isNameByte(s[i-1]) {
		i--
	}

	return s[i:]
}

// isNameByte reports whether c may appear in a reference.
func isNameByte(c byte) bool {
	return c == '_' || c == '.' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// StepsRoot describes the root every step's outputs hang from, carrying one
// candidate per step whose outputs are in scope.
//
// The prose changes with the list because the empty case is a different fact: a
// root that resolves and holds nothing is not a root that is missing, and an
// author reading "no step has run at this point" learns something the general
// sentence does not tell them.
func StepsRoot(steps []Candidate) Candidate {
	docs := "Every step's outputs, keyed by step id: write " + v1.StepsRoot + ".<id>.<output>. " +
		"Only steps that have already run are in scope here."
	if len(steps) == 0 {
		docs = "Every step's outputs, keyed by step id. No step has run at this point, so there " +
			"is nothing to select yet."
	}

	return Candidate{
		Name: v1.StepsRoot,
		// A value with named members, which is what the root is: a map from
		// step id to that step's outputs.
		Kind:   KindRoot,
		Detail: "step outputs",
		Docs:   docs,
		// The root is never the whole of a reference, so the dot that
		// continues it is written too — the same reason a key is offered with
		// its colon.
		Insert:  v1.StepsRoot + ".",
		Members: steps,
	}
}

// VarsRoot describes the workflow's declared variables.
func VarsRoot(vars []Candidate) Candidate {
	return Candidate{
		Name: v1.VarsRoot,
		// A value with named members, the same shape as the steps root.
		Kind:   KindRoot,
		Detail: "workflow variables",
		Docs: "The workflow's declared variables, keyed by name: write " + v1.VarsRoot +
			".<name>. They are evaluated once before the first step runs, so every step " +
			"sees the same values. A step's own `vars:` are written bare instead.",
		// The dot comes with it, as the steps root's does.
		Insert:  v1.VarsRoot + ".",
		Members: vars,
	}
}

// InputsRoot describes the arguments a run was started with.
func InputsRoot(inputs []Candidate) Candidate {
	return Candidate{
		Name:   v1.InputsRoot,
		Kind:   KindRoot,
		Detail: "run inputs",
		Docs: "The arguments this run was started with, keyed by name: write " + v1.InputsRoot +
			".<name>. They are checked against the workflow's declared inputs and defaulted " +
			"at submit, so every step sees the same values.",
		Insert:  v1.InputsRoot + ".",
		Members: inputs,
	}
}
