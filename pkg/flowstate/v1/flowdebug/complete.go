package flowdebug

import (
	"slices"
	"sort"
	"strings"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
)

// Completion is what the session offers at one cursor position.
//
// It is the answer a console renders; this package draws no prompt and knows no
// terminal. See [Console].
type Completion struct {
	// Prefix is the text an accepted candidate replaces: the partial word
	// immediately before the cursor.
	Prefix string

	// Candidates are the offers, in the order they should be shown.
	Candidates []Candidate

	// Truncated reports that the answer was bounded and is a prefix of what
	// matched. A console says how many it did not print, because a silently
	// short list is one nobody can tell from a complete one.
	Truncated bool
}

// A Candidate is one offer.
type Candidate struct {
	// Text is what replaces [Completion.Prefix].
	Text string

	// Detail is the one line beside it, for a console that lists several.
	//
	// It is a *description of the name* and never anything computed from a
	// value — see [Session.Complete] for why that is a rule rather than a
	// coincidence.
	Detail string

	// Continues reports that Text leaves the reference unfinished, which is
	// true exactly of the names written with the dot that continues them. A
	// console writes no space after one.
	Continues bool
}

// Complete offers what may be written at pos in line.
//
// This is the debugger's whole reason to have a prompt rather than a reader.
// The scope it completes over is the *paused run's own* — the steps that have
// actually produced outputs, the names those outputs actually have, the
// arguments this run was actually started with — where an editor can only offer
// what a task declares. The rules for where each of those may be written are
// [celcomplete]'s, shared with the editor, because an author who learned
// `steps.<id>.<output>` in one should not meet a second grammar in the other.
//
// # A candidate is a name, and never a value
//
// Nothing here reads a value. Not to render a preview, not to name a type, not
// to say how long something is. A debugger is a reveal and its printing is
// deliberately behind a redaction seam ([Session.SetRedactor],
// [Session.SetValueRedactor]); a completion popup is *also* printing, and one
// that derived anything from the datum would be a second door around the first
// — the shape #1109 closed for `inspect` and the step account, reopened by a
// keystroke that never even reaches a command. So the detail beside a name says
// what the name is, not what it holds.
//
// The names themselves are the file's rather than the run's: a step id, an
// output name, an input name and a var name are all written by an author, so
// offering them discloses the workflow to somebody already debugging it. The
// one case where that could be wrong is a caller whose redactor withholds a
// name too, and a candidate the redactor would change is dropped rather than
// offered — fail closed, because a completion has nowhere to print
// `[redacted]` that would not simply insert it.
//
// # Bounded
//
// The number of candidates grows with the run and the workflow rather than with
// what was typed, so the answer is bounded by [celcomplete.MaxCandidates] and
// says when it reached it.
func (s *Session) Complete(line string, pos int) Completion {
	if pos < 0 {
		pos = 0
	}
	if pos > len(line) {
		pos = len(line)
	}
	before := line[:pos]

	s.mu.Lock()
	at := s.at
	s.mu.Unlock()

	typed, rest, hasArgument := strings.Cut(before, " ")
	if !hasArgument {
		// Still on the first word: the verbs themselves.
		return s.offerCommands(at, typed)
	}

	known, ok := resolve(strings.TrimSpace(typed))
	if !ok {
		return Completion{}
	}
	switch known.completes {
	case completesExpression:
		return s.offerExpression(at, rest)
	case completesStep:
		return s.offerNames(lastWord(rest), s.reachableSteps(at), "a step this run may reach")
	case completesBreakpoint:
		return s.offerNames(lastWord(rest), s.breakpointIDs(), "a breakpoint this session holds")
	default:
		return Completion{}
	}
}

// offerCommands offers the verbs, which differ between a breakpoint and the
// autopsy: the movement verbs are gone at an autopsy, where there is no run
// left to move, so offering them would be teaching a command that does nothing
// but leave.
func (s *Session) offerCommands(at promptSubject, prefix string) Completion {
	out := Completion{Prefix: prefix}
	for _, c := range commands {
		if at.autopsy && !autopsyVerbs[c.verb] {
			continue
		}
		if !strings.HasPrefix(c.verb, prefix) {
			continue
		}
		out.add(Candidate{
			// The verb, with the space that separates it from its argument
			// where it takes one — the same reason a root is written with its
			// dot: stopping short leaves one character to type that produces
			// the next list.
			Text:      c.verb + argumentSpace(c),
			Detail:    c.help,
			Continues: c.argument != "",
		}, s.withheld)
	}

	return out
}

// autopsyVerbs are the commands the autopsy answers. It is the same reading
// [Session.Autopsy]'s own switch makes — `inspect`, `scope`, `help`, and
// leaving — written as the set the completer offers.
var autopsyVerbs = map[string]bool{
	"inspect": true,
	"scope":   true,
	"help":    true,
	"quit":    true,
}

// argumentSpace is the separator a verb that takes an argument is written with.
func argumentSpace(c command) string {
	if c.argument == "" {
		return ""
	}

	return " "
}

// offerExpression completes CEL against the paused run's live scope.
func (s *Session) offerExpression(at promptSubject, expression string) Completion {
	result := celcomplete.Complete(expression, s.completionScope(at))

	out := Completion{Prefix: result.Prefix, Truncated: result.Truncated}
	for _, c := range result.Candidates {
		out.add(Candidate{
			Text:      c.Text(),
			Detail:    c.Detail,
			Continues: c.Continues(),
		}, s.withheld)
	}

	return out
}

// offerNames offers a plain list of names, which is what `break`, `until` and
// `delete` take.
func (s *Session) offerNames(prefix string, names []string, detail string) Completion {
	out := Completion{Prefix: prefix}
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		out.add(Candidate{Text: name, Detail: detail}, s.withheld)
	}

	return out
}

// add appends a candidate unless the session is withholding it or the answer is
// already full.
func (c *Completion) add(candidate Candidate, withheld func(Candidate) bool) {
	if withheld(candidate) {
		return
	}
	if len(c.Candidates) >= celcomplete.MaxCandidates {
		c.Truncated = true

		return
	}
	c.Candidates = append(c.Candidates, candidate)
}

// withheld reports whether the caller's redactor would change anything this
// candidate prints, in which case it is dropped.
//
// Dropped rather than redacted, and that is the whole of the decision: a
// completion is text a console *inserts*, so an offer rendered as `[redacted]`
// would be a marker typed into somebody's expression. There is no third answer
// where a withheld name is still usable, and a session that cannot show a name
// safely should not offer it at all.
func (s *Session) withheld(candidate Candidate) bool {
	s.mu.Lock()
	redact := s.redact
	s.mu.Unlock()

	if redact == nil {
		return false
	}

	return redact(candidate.Text) != candidate.Text || redact(candidate.Detail) != candidate.Detail
}

// lastWord returns the partial name at the end of an argument, so completing a
// step id inside `break bui` replaces `bui` rather than the whole rest of the
// line.
func lastWord(rest string) string {
	if at := strings.LastIndexAny(rest, " \t"); at >= 0 {
		return rest[at+1:]
	}

	return rest
}

// completionScope renders the paused run's scope as the one [celcomplete]
// answers over.
//
// Every list here is read from the schema message the engine handed this
// session — `Scope.outputs`, `Scope.vars`, `Scope.ambient_vars`,
// `Scope.inputs` — and every list is *keys*. That is the containment argument
// stated as code: there is no path from here to a value, so there is nothing
// for a redactor to have to catch.
func (s *Session) completionScope(at promptSubject) celcomplete.Scope {
	scope := at.scope

	shared := celcomplete.Scope{
		// The run's own profile, not this build's current one. A specification
		// compiled by an older build has a smaller vocabulary, and a prompt
		// offering a function that run cannot evaluate would be completing a
		// name into an error.
		Profile: scope.GetProfile(),
		Locals:  namesOf(scope.GetVars(), "bound where this expression is written"),
		Roots:   []celcomplete.Candidate{celcomplete.StepsRoot(stepCandidates(scope))},
	}

	// The rooted namespaces, each offered only where the run has one, the rule
	// the editor applies to `vars:`: a root that resolves to an empty map is a
	// name nobody should be taught. `steps` above is the exception it is there
	// too — the root is how the language is spelled, and a session stopped at
	// the first step still needs to learn it.
	if vars := scope.GetAmbientVars(); len(vars) > 0 {
		shared.Roots = append(shared.Roots, celcomplete.VarsRoot(namesOf(vars, "a variable the workflow declares")))
	}
	if inputs := scope.GetInputs(); len(inputs) > 0 {
		shared.Roots = append(shared.Roots, celcomplete.InputsRoot(namesOf(inputs, "an argument this run was started with")))
	}

	// The autopsy's extra bindings, which are bare and win over everything
	// above — exactly the precedence [v1.Scope.ActivationWith] gives them, so
	// what is offered and what would resolve are one answer. `run` and `vars`
	// arrive this way after a case fails, carrying the bindings its
	// `expect.check:` was judged under.
	for _, name := range sortedKeys(at.extra) {
		members := mapMembers(at.extra[name])
		shared.Locals = append(shared.Locals, celcomplete.Candidate{
			Name:    name,
			Kind:    kindFor(members),
			Detail:  "bound for this autopsy",
			Insert:  name + dotIfMembers(members),
			Members: members,
		})
	}

	return shared
}

// kindFor says whether a binding is a value or a namespace, which is decided by
// whether anything follows a dot on it.
func kindFor(members []celcomplete.Candidate) celcomplete.Kind {
	if len(members) > 0 {
		return celcomplete.KindRoot
	}

	return celcomplete.KindValue
}

// dotIfMembers writes the dot with a binding that has members, for the reason a
// root is written with one.
func dotIfMembers(members []celcomplete.Candidate) string {
	if len(members) > 0 {
		return "."
	}

	return ""
}

// mapMembers returns the keys of a bound map, or nothing where the binding is
// not one.
//
// Keys, never values: a [traits.Mapper] is walked with its own iterator and
// only the key is read, which is what keeps this on the right side of the rule
// [Session.Complete] states. Bounded by [celcomplete.MaxCandidates] like every
// other list here, because the size of a bound map is the run's choice.
func mapMembers(bound ref.Val) []celcomplete.Candidate {
	mapper, ok := bound.(traits.Mapper)
	if !ok {
		return nil
	}

	var names []string
	for it := mapper.Iterator(); it.HasNext() == types.True && len(names) < celcomplete.MaxCandidates; {
		key, ok := it.Next().Value().(string)
		if !ok {
			continue
		}
		names = append(names, key)
	}
	sort.Strings(names)

	out := make([]celcomplete.Candidate, 0, len(names))
	for _, name := range names {
		out = append(out, celcomplete.Candidate{
			Name:   name,
			Kind:   celcomplete.KindField,
			Detail: "bound for this autopsy",
		})
	}

	return out
}

// stepCandidates renders the steps that have produced outputs, each carrying
// the names its outputs actually have.
//
// This is what a live scope buys over a document's: an editor offers what a
// task *declares*, and a shaping expression or a plugin's own answer can make
// that a different set. Here the names are the ones the run produced.
func stepCandidates(scope *v1.Scope) []celcomplete.Candidate {
	steps := scope.GetOutputs().GetStepValues()

	out := make([]celcomplete.Candidate, 0, len(steps))
	for _, id := range sortedKeys(steps) {
		out = append(out, celcomplete.Candidate{
			Name:   id,
			Kind:   celcomplete.KindValue,
			Detail: "a step that has run",
			// No type beside an output name, deliberately: the only source for
			// one here is the value itself, and reading a value is what this
			// completer does not do.
			Members: namesOfOutputs(steps[id]),
		})
	}

	return out
}

// namesOfOutputs renders one step's recorded output names.
func namesOfOutputs(outputs *v1.Node_Outputs) []celcomplete.Candidate {
	named := outputs.GetNamedValues()

	out := make([]celcomplete.Candidate, 0, len(named))
	for _, name := range sortedKeys(named) {
		out = append(out, celcomplete.Candidate{
			Name:   name,
			Kind:   celcomplete.KindField,
			Detail: "an output this step produced",
		})
	}

	return out
}

// namesOf renders a scope map's keys as candidates.
func namesOf(values map[string]*v1.Value, detail string) []celcomplete.Candidate {
	out := make([]celcomplete.Candidate, 0, len(values))
	for _, name := range sortedKeys(values) {
		out = append(out, celcomplete.Candidate{
			Name:   name,
			Kind:   celcomplete.KindValue,
			Detail: detail,
		})
	}

	return out
}

// reachableSteps are the ids `break` and `until` may name.
//
// Two sources, unioned, because neither is complete on its own. A caller that
// knows the workflow says so ([Options.Steps]), which is what makes
// `break <tab>` useful before anything has run — the whole point of a
// breakpoint being that it is set for a step the run has not reached. A caller
// that does not (an embedder handed a [v1.Debugger] seam and nothing else) gets
// the ids this session has actually seen go past, which is at least the run so
// far rather than nothing at all.
func (s *Session) reachableSteps(at promptSubject) []string {
	s.mu.Lock()
	ids := make([]string, 0, len(s.steps)+len(s.seen))
	ids = append(ids, s.steps...)
	for id := range s.seen {
		ids = append(ids, id)
	}
	s.mu.Unlock()

	// A step whose outputs are in scope has certainly run, so the paused run's
	// own scope is a third source that costs nothing to read.
	for id := range at.scope.GetOutputs().GetStepValues() {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return slices.Compact(ids)
}

// breakpointIDs are the ids `delete` may name.
func (s *Session) breakpointIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := sortedKeys(s.breakpoints)

	return ids
}
