package flowstatev1

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"
)

// This file is the one answer to "which values did this run's `sensitive:`
// declarations put into circulation, and how is a rendered string or a
// rendered structure cleared of them".
//
// # Why sensitivity has to be carried by value rather than by name
//
// `sensitive:` is declared on an *input*, and the value it names does not stay
// there. A `for_each` binds each element of a sensitive list to `as: customer`
// and runs a body with it in scope; a step's `vars:` copies one into a name of
// its own; a loop's `state:` carries whatever the body computed from it; the
// engine attaches the bound item to a tolerated failure as
// [StepErrorItemOutput], and a task's own failure sentence quotes the URL it
// was given. None of those places has the word `sensitive` anywhere near it,
// and none of them can be found by asking a declaration what it was called.
//
// So sensitivity propagates the way the value does: this set holds the
// declared value *and every value nested within it*, compared by content, with
// a textual backstop for the case where the material has been concatenated
// into a larger string. A loop item is a descendant of the list it came from,
// which is what makes it redacted by the same set that redacts the list —
// nothing re-declares it at the loop, and nothing has to.
//
// # What this is not
//
// It is not containment. The value is an ordinary part of the run's history
// exactly like any other input, and anyone with access to that history reads
// it in the clear; [Value] carrying a secret reference is the mechanism for a
// value that must never be there at all. What this bounds is what Flowstate
// itself *renders*: a terminal, an agent's answer, a test report. See
// `InputDeclaration.sensitive` in proto/flowstate/v1/workflow.proto for the
// contract, which this file is the enforcement half of.
//
// The mechanism was written for flowtest's stub diagnostics and its transcript
// (#956, #1052, #1109) and lives here, in the package both drivers and the CLI
// already import, because `flow run local` and `flow run` render the same
// run-failure sentence `flow test` was already clearing — one value with one
// meaning, which CLAUDE.md requires be written down once.

// SensitiveMarker is what a redacted value renders as: deliberately not shaped
// like a value a workload could have produced itself.
const SensitiveMarker = "[redacted]"

// minSensitiveSubstringRunes is the shortest *descendant* string
// [SensitiveValues.RedactSubstrings] will replace textually. A one-rune leaf
// is not a redaction, it is a shredder: replacing every `a` in the rendered
// line with the marker destroys the diagnostic while protecting nothing
// [SensitiveValues.IsSensitive] has not already caught by comparing that leaf
// on its own. A declared input's own value is exempt from this floor — it is
// the thing `sensitive:` names, and `"Bearer " + inputs.token` is precisely
// the shape the backstop exists for.
const minSensitiveSubstringRunes = 2

// maxSensitiveSubstringRedactionWork bounds one rendered value and
// maxSensitiveSubstringMatcherBytes bounds the retained matcher built from
// sensitive strings. Work past either bound withholds rather than weakening
// redaction.
const maxSensitiveSubstringRedactionWork = 1 << 20
const maxSensitiveSubstringMatcherBytes = 64 << 10

// maxSensitiveDescendants bounds how many values one redaction set may hold,
// counting both what the walk below has collected and what it still has
// queued. The queue is counted because it is memory the same input controls:
// bounding only the result would leave a wide, shallow value free to push
// millions of entries onto the stack on the way to a bounded answer, which is
// the "bounding one resource does not bound another" failure CLAUDE.md names.
//
// A workflow input may legitimately carry maxListElements = 10,000 elements
// (constraints.go), and every entry in the set costs one [reflect.DeepEqual]
// per node of a rendered structure plus, for a string, a full scan of the
// rendered text. 1024 sits far above any real credential — a token, a key
// pair, a small object of headers — and far below where building a *failure*
// message costs more than the run it is reporting on.
//
// Blowing the bound is not a reason to redact less: [SensitiveInputValues]
// answers [SensitiveValues.WithholdAll], so an input too large to enumerate
// withholds everything rather than printing the part of it the walk never
// reached. That is one mechanism serving two of CLAUDE.md's rules at once —
// bound the resource the attacker controls, and deny when you cannot decide.
const maxSensitiveDescendants = 1024

// sensitiveState is the built set: the values to compare against, the
// narrower list of strings to replace textually, and whether the set could be
// built at all.
//
// The two lists are deliberately different sizes. values is compared with
// [reflect.DeepEqual] against every node of a rendered structure, so it holds
// each sensitive input's whole value *and* every value nested within it.
// substrings is the textual backstop, which is a far blunter instrument — a
// string replaced everywhere it occurs — so it holds a narrower set; the walk
// in [SensitiveInputValues] decides which strings earn a place in it.
//
// withholdAll is the fail-closed answer: when the set could not be built
// completely, nothing can be shown to be safe, so every value the holder is
// asked about is withheld.
type sensitiveState struct {
	values           []any
	substrings       []string
	substringMatcher *sensitiveSubstringMatcher
	withholdAll      bool
}

// SensitiveValues is a run's redaction set: what a renderer must not print,
// and whether it could be enumerated at all.
//
// The zero value is valid and redacts nothing, which is the common case — a
// workflow that declares nothing sensitive — and costs one nil check per
// rendered value.
//
// # Why the material is held in a closure
//
// [fmt] cannot call a method on a value it reaches through an unexported
// field, so it prints the fields instead: a struct field here would mean that
// `%+v` on a [SensitiveValues], or on any struct holding one — and
// cmd/flow's run poller holds one across an entire follow loop — dumps every
// value this type exists to keep off the screen. That is CLAUDE.md's
// "reflection through unexported fields" leak class exactly, and secrets.Scrubber
// already answers it the same way: hold the state in a closure, which
// reflection cannot reach.
type SensitiveValues struct {
	// state closes over the built set. nil means the empty set.
	state func() sensitiveState
}

// held returns the set this value closes over, or the empty one.
func (s SensitiveValues) held() sensitiveState {
	if s.state == nil {
		return sensitiveState{}
	}

	return s.state()
}

// sensitiveValuesOf returns a [SensitiveValues] closing over state.
func sensitiveValuesOf(state sensitiveState) SensitiveValues {
	if !state.withholdAll {
		var ok bool
		state.substringMatcher, ok = newSensitiveSubstringMatcher(state.substrings)
		if !ok {
			state = sensitiveState{withholdAll: true}
		}
	}
	return SensitiveValues{state: func() sensitiveState { return state }}
}

// WithheldSensitiveValues is the fail-closed set: it can enumerate nothing, so
// it redacts everything it is asked about.
//
// It exists as a named constructor because a caller that could not build its
// set — an input it cannot read, a value past [maxSensitiveDescendants], a
// renderer with no arguments in hand — must be able to say so, and the
// alternative spelling of "I could not decide" is the zero value, which allows
// everything.
func WithheldSensitiveValues() SensitiveValues {
	return sensitiveValuesOf(sensitiveState{withholdAll: true})
}

// SensitiveInputValues returns, as native Go values comparable with
// [reflect.DeepEqual], every value the run's own `sensitive:` inputs carry:
// each such input's whole value and every value nested within it. These are
// what a renderer must not print even when they reach it under a different
// name, since `sensitive:` is a property of the value's origin, not of
// whatever a step, a loop binding or a task chose to call it.
//
// inputs are the run's bound inputs — [BindRunInputs]'s answer, so that a
// declared default is in the set exactly like a submitted value — and names is
// [SensitiveInputNames] of the workflow those inputs were bound against.
//
// The descendants are in there because a sensitive declaration can itself be
// structured. A `creds:` input marked `sensitive: true` and read as
// `${inputs.creds.token}` puts a leaf into a rendered value that is not
// [reflect.DeepEqual] to anything the scope holds, so a set of whole values
// alone prints that credential in the clear — and the substring backstop does
// not save it either, since the whole value there is a map rather than a
// string. The same walk is what makes a `for_each`'s bound item redacted: an
// element of a sensitive list is a descendant of that list.
//
// Returns the zero value when there is nothing to redact, which is the common
// case and costs one nil map read.
//
// # The cost of matching by value, and why it is still the right rule
//
// This matches by *content*, with no provenance: nothing about a native value
// records which declaration it came from, so a descendant that happens to
// equal an unrelated value redacts that one too. Sensitive
// `creds: {enabled: false}` puts `false` into the set, and an ordinary
// `follow_redirects: false` rendered beside it then reads as `[redacted]` —
// hiding one of the discriminating fields a diagnostic exists to show
// (Codex, #956).
//
// That cost is real, and it is chosen for the same reason cmd/flow's
// redactStepValues chooses the same trade at greater length. The precise
// alternative is to trace each value back to the declaration it came from,
// and a trace catches only what it can see: a sensitive leaf that arrives
// through a step's `vars:`, through another step's output, or concatenated
// into a larger string has no path back to `inputs.creds` at all. Such a rule
// would print those in the clear while implying that it traces sensitive
// data — a mechanism that looks precise and is not, which is worse than one
// that is honestly blunt, because a reader trusts the one that looks precise
// (CLAUDE.md, "fail closed").
//
// So the blunt rule stays and its cost is written down here, rather than being
// rediscovered by whoever next wonders why an unrelated `false` came back
// redacted.
func SensitiveInputValues(inputs map[string]*Value, sensitiveNames map[string]bool) SensitiveValues {
	if len(sensitiveNames) == 0 {
		return SensitiveValues{}
	}

	// node carries whether a queued value is the declared input itself rather
	// than something nested inside it, which only the substring floor above
	// cares about.
	type node struct {
		value any
		root  bool
	}

	var out sensitiveState
	for name, v := range inputs {
		if !sensitiveNames[name] {
			continue
		}

		// A sensitive input this cannot read is withheld whole, and takes
		// every other value with it. Skipping it — which is what a `continue`
		// here does — drops it out of the redaction set silently, so *nothing*
		// about that input is redacted anywhere: an allow-on-error in the one
		// function whose job is to deny (CLAUDE.md, "fail closed": a component
		// that allows when it cannot decide will eventually allow everything).
		lit := v.GetLiteral()
		if lit == nil {
			return WithheldSensitiveValues()
		}
		native, err := LiteralToGo(lit)
		if err != nil {
			return WithheldSensitiveValues()
		}

		// Sensitivity belongs to the declared input's origin, so it follows
		// every descendant when a loop binds one element or a task selects one
		// field out of a structured value. The container is kept as well: a
		// task may carry it whole.
		pending := []node{{value: native, root: true}}
		for len(pending) > 0 {
			if len(out.values)+len(pending) > maxSensitiveDescendants {
				return WithheldSensitiveValues()
			}

			n := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			out.values = append(out.values, n.value)

			switch value := n.value.(type) {
			case string:
				// "" is excluded whatever its origin: replacing it inserts
				// the marker between every rune of the rendered line.
				if value != "" && (n.root || utf8.RuneCountInString(value) >= minSensitiveSubstringRunes) {
					out.substrings = append(out.substrings, value)
				}
			case int64, uint64, float64, bool:
				// A non-string scalar's canonical text joins the backstop:
				// `${string(inputs.pin)}` turns the number into a string the
				// typed equality can never see (Codex, #1052). fmt.Sprint is
				// the spelling both CEL's string() of an int and this
				// package's own rendering produce; a reformatted spelling
				// (padding, precision) is past what a substring set can
				// enumerate, which is the boundary the withholdAll rule
				// already draws for sets that cannot be built at all. The
				// floor and root exemption apply exactly as for a string
				// descendant.
				text := fmt.Sprint(value)
				if n.root || utf8.RuneCountInString(text) >= minSensitiveSubstringRunes {
					out.substrings = append(out.substrings, text)
				}
			case map[string]any:
				// Keys are descendants too: sensitivity belongs to the whole
				// declared value, and a map whose *keys* carry the material —
				// account ids, say — leaks through a walk that only enqueues
				// what they map to (Codex, #1052). A key rides the queue as
				// any string descendant does, so the substring floor and the
				// descendant bound apply to it unchanged.
				for name, child := range value {
					pending = append(pending, node{value: name}, node{value: child})
				}
			case []any:
				for _, child := range value {
					pending = append(pending, node{value: child})
				}
			}
		}
	}

	return sensitiveValuesOf(out)
}

// WithValues returns a set holding everything this one holds plus each given
// plaintext, in both halves: the value comparison catches the whole, the
// substring backstop catches `"Bearer " + value`.
//
// It exists for a value that is sensitive without being a declared input — a
// test case's own `secrets:` plaintext, which a stub can echo into a step's
// outputs — and mirrors secrets.Scrubber.AddValue, which is the same
// affordance one layer down. An empty value adds nothing: it occurs at every
// position of every string, so redacting it would destroy the text while
// protecting nothing.
//
// A new set rather than a mutation, because a [SensitiveValues] is copied by
// value into whatever holds it and a set that changed under its holders would
// be a redaction rule that depends on when it was read.
func (s SensitiveValues) WithValues(plaintexts ...string) SensitiveValues {
	held := s.held()

	state := sensitiveState{
		values:      append([]any(nil), held.values...),
		substrings:  append([]string(nil), held.substrings...),
		withholdAll: held.withholdAll,
	}

	for _, plaintext := range plaintexts {
		if plaintext == "" {
			continue
		}
		state.values = append(state.values, plaintext)

		// The substring half takes the floor [minSensitiveSubstringRunes]
		// argues, which this path used to skip: a one-rune fixture secret
		// (`t`, `1` — short by nature, since nothing about a test's
		// `secrets:` is real material) marked every occurrence of that rune
		// in every rendered line, shredding the very diagnostic its author
		// was trying to read — and the substituted markers were themselves
		// shredded, nesting `[redacted]` inside `[redacted]`. The value
		// comparison above still holds at every length, so a rendered value
		// *equal* to the plaintext redacts exactly as before.
		//
		// What the floor concedes, it concedes deliberately and in the open:
		// a one-rune plaintext embedded in a *composite* string — the
		// `"Bearer " + value` shape the backstop exists for — now prints,
		// because there is no third option: redacting one rune everywhere it
		// occurs is the shredder above, and withholding the whole rendering
		// destroys the same diagnostic by a different door. This is the
		// standing every non-root descendant of a declared `sensitive:`
		// input already has (the walk above applies the identical floor to
		// them), extended to the values this path adds; a fixture that needs
		// the composite backstop uses a plaintext of two runes or more.
		if utf8.RuneCountInString(plaintext) >= minSensitiveSubstringRunes {
			state.substrings = append(state.substrings, plaintext)
		}
	}

	return sensitiveValuesOf(state)
}

// WithholdAll reports the fail-closed case: the set could not be built
// completely, so nothing can be shown to be safe.
func (s SensitiveValues) WithholdAll() bool {
	return s.held().withholdAll
}

// Empty reports that this set would change nothing it was given — no values,
// no substrings, and not withholding. A renderer uses it to skip the work, and
// to decide whether to tell a reader that anything is being withheld at all: a
// notice on a run that redacts nothing is noise.
func (s SensitiveValues) Empty() bool {
	held := s.held()

	return !held.withholdAll && len(held.values) == 0 && len(held.substrings) == 0
}

// IsSensitive reports whether v is one of the run's own sensitive values, by
// content rather than by the name whatever holds it happened to give it:
// `message: ${inputs.token}` is caught the same as `inputs.token` itself, and
// a `for_each`'s bound item the same as the list it was drawn from.
func (s SensitiveValues) IsSensitive(v any) bool {
	return isSensitiveValue(v, s.held().values)
}

// RedactTree walks v and replaces every value [SensitiveValues.IsSensitive]
// recognizes, at any depth, with [SensitiveMarker]. A withholding set replaces
// v whole.
func (s SensitiveValues) RedactTree(v any) any {
	held := s.held()
	if held.withholdAll {
		return SensitiveMarker
	}

	return redactSensitiveTree(v, held.values)
}

// RedactSubstrings replaces each sensitive string wherever it occurs in
// rendered. It is the backstop half only: a withholding set is *not* consulted
// here, because a caller that has to withhold has its own sentence to write
// about why, and this returning a bare marker would put that decision in the
// wrong place. See [SensitiveValues.RedactText] for the answer that includes
// it.
func (s SensitiveValues) RedactSubstrings(rendered string) string {
	return redactSensitiveSubstringsWithMatcher(rendered, s.held().substringMatcher)
}

// RedactText is the whole answer for one rendered line: withheld entirely when
// the set could not be enumerated, and otherwise cleared of every sensitive
// string it contains. withheld is what a withholding set renders as, which the
// caller supplies because only it knows what the reader is losing.
func (s SensitiveValues) RedactText(rendered, withheld string) string {
	if s.WithholdAll() {
		return withheld
	}

	return s.RedactSubstrings(rendered)
}

// isSensitiveValue reports whether v is one of sensitiveValues.
func isSensitiveValue(v any, sensitiveValues []any) bool {
	for _, sv := range sensitiveValues {
		if reflect.DeepEqual(v, sv) {
			return true
		}
	}

	return false
}

// redactSensitiveTree walks v and replaces every value [isSensitiveValue]
// recognizes, at any depth, with [SensitiveMarker].
//
// A `map[string]any` or `[]any` compares as a whole against reflect.DeepEqual,
// so a top-level check alone misses the far more common shape: a sensitive
// scalar carried *inside* a structured input, such as `headers: {Authorization:
// ${inputs.token}}`. The whole headers map is never equal to the token; only
// one leaf of it is, so the leaf has to be checked on its own (#386
// follow-up). The recursion mirrors the two structured shapes a native value
// converted by [LiteralToGo] can hold: a map keyed by string, and a list.
func redactSensitiveTree(v any, sensitiveValues []any) any {
	if isSensitiveValue(v, sensitiveValues) {
		return SensitiveMarker
	}

	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			// Keys redact by exact match at every depth, not only where a
			// renderer happens to print one at the top level: a sensitive
			// struct's key — including one below the substring floor — is as
			// much the material as the value it maps to (Codex, #1052). Two
			// sensitive keys folding into one marker entry lose a pair, which
			// is the redaction doing its job, not a collision to avoid.
			if isSensitiveValue(k, sensitiveValues) {
				k = SensitiveMarker
			}
			out[k] = redactSensitiveTree(e, sensitiveValues)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = redactSensitiveTree(e, sensitiveValues)
		}
		return out
	default:
		return v
	}
}

// redactSensitiveSubstrings is the backstop for a sensitive value that reaches
// a rendered line as part of a larger string rather than as the whole value or
// a map/list leaf: `"Bearer " + inputs.token` renders as one string, which
// [redactSensitiveTree] cannot see into because nothing about the concatenated
// result equals the token on its own. Each sensitive string's exact text is
// replaced wherever it occurs in rendered, so the material cannot survive by
// being wrapped in unrelated characters.
//
// It reads the substring list rather than the full value set on purpose: this
// replacement is textual and unanchored, so a string short enough to occur by
// accident does more damage to the diagnostic than it prevents.
// [SensitiveInputValues] is where that line is drawn, and
// [minSensitiveSubstringRunes] is where it is argued.
func redactSensitiveSubstrings(rendered string, substrings []string) string {
	// Every match of every sensitive string is found against the ORIGINAL
	// text, the intervals merged, and the merged spans spliced out in one
	// pass. Sequential ReplaceAll cannot be ordered into correctness: with
	// containment (`abcd` in `abcdef`) the shorter-first order splits the
	// longer into `[redacted]ef`, and with intersection (`ABCDE` and `CDEFG`
	// across `ABCDEFG`) *either* order leaves the other's fragment exposed —
	// both partial leaks, the second one whatever you sort by (Codex, #1052).
	// A union of matches has no order to get wrong. One site, so every
	// renderer shares the answer.
	matcher, ok := newSensitiveSubstringMatcher(substrings)
	if !ok {
		return SensitiveMarker
	}
	return redactSensitiveSubstringsWithMatcher(rendered, matcher)
}

func redactSensitiveSubstringsWithMatcher(rendered string, matcher *sensitiveSubstringMatcher) string {
	if matcher == nil {
		return rendered
	}
	if len(rendered) > maxSensitiveSubstringRedactionWork {
		return SensitiveMarker
	}

	redacted := make([]bool, len(rendered))
	if !matcher.markMatches(redacted, rendered) {
		return rendered
	}

	var b strings.Builder
	for i := 0; i < len(rendered); {
		if !redacted[i] {
			end := i + 1
			for end < len(rendered) && !redacted[end] {
				end++
			}
			b.WriteString(rendered[i:end])
			i = end
			continue
		}
		b.WriteString(SensitiveMarker)
		for i < len(rendered) && redacted[i] {
			i++
		}
	}

	return b.String()
}

// sensitiveSubstringMatcher is an immutable Aho-Corasick automaton. One is
// built per SensitiveValues set and shared by every rendering call, so a long
// transcript costs one pass over its text rather than one pass per sensitive
// descendant per line.
type sensitiveSubstringMatcher struct {
	nodes []sensitiveSubstringNode
}

type sensitiveSubstringNode struct {
	edges   []sensitiveSubstringEdge
	failure int
	longest int
}

type sensitiveSubstringEdge struct {
	byteValue byte
	next      int
}

func newSensitiveSubstringMatcher(substrings []string) (*sensitiveSubstringMatcher, bool) {
	unique := make(map[string]struct{}, len(substrings))
	totalBytes := 0
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if _, exists := unique[substring]; exists {
			continue
		}
		if len(substring) > maxSensitiveSubstringMatcherBytes-totalBytes {
			return nil, false
		}
		unique[substring] = struct{}{}
		totalBytes += len(substring)
	}
	if len(unique) == 0 {
		return nil, true
	}

	matcher := &sensitiveSubstringMatcher{nodes: []sensitiveSubstringNode{{}}}
	for substring := range unique {
		state := 0
		for i := 0; i < len(substring); i++ {
			next, ok := matcher.nextLinear(state, substring[i])
			if !ok {
				next = len(matcher.nodes)
				matcher.nodes = append(matcher.nodes, sensitiveSubstringNode{})
				matcher.nodes[state].edges = append(matcher.nodes[state].edges,
					sensitiveSubstringEdge{byteValue: substring[i], next: next})
			}
			state = next
		}
		matcher.nodes[state].longest = max(matcher.nodes[state].longest, len(substring))
	}
	for i := range matcher.nodes {
		sort.Slice(matcher.nodes[i].edges, func(a, b int) bool {
			return matcher.nodes[i].edges[a].byteValue < matcher.nodes[i].edges[b].byteValue
		})
	}

	queue := make([]int, 0, len(matcher.nodes)-1)
	for _, edge := range matcher.nodes[0].edges {
		queue = append(queue, edge.next)
	}
	for len(queue) > 0 {
		state := queue[0]
		queue = queue[1:]
		for _, edge := range matcher.nodes[state].edges {
			queue = append(queue, edge.next)
			failure := matcher.nodes[state].failure
			for failure != 0 {
				if next, ok := matcher.next(failure, edge.byteValue); ok {
					failure = next
					break
				}
				failure = matcher.nodes[failure].failure
			}
			if failure == 0 {
				if next, ok := matcher.next(0, edge.byteValue); ok && next != edge.next {
					failure = next
				}
			}
			matcher.nodes[edge.next].failure = failure
			matcher.nodes[edge.next].longest = max(
				matcher.nodes[edge.next].longest,
				matcher.nodes[failure].longest,
			)
		}
	}

	return matcher, true
}

func (m *sensitiveSubstringMatcher) nextLinear(state int, value byte) (int, bool) {
	for _, edge := range m.nodes[state].edges {
		if edge.byteValue == value {
			return edge.next, true
		}
	}
	return 0, false
}

func (m *sensitiveSubstringMatcher) next(state int, value byte) (int, bool) {
	edges := m.nodes[state].edges
	i := sort.Search(len(edges), func(i int) bool { return edges[i].byteValue >= value })
	if i < len(edges) && edges[i].byteValue == value {
		return edges[i].next, true
	}
	return 0, false
}

func (m *sensitiveSubstringMatcher) markMatches(redacted []bool, text string) bool {
	state := 0
	found := false
	coveredEnd := 0
	for i := 0; i < len(text); i++ {
		for state != 0 {
			if _, ok := m.next(state, text[i]); ok {
				break
			}
			state = m.nodes[state].failure
		}
		if next, ok := m.next(state, text[i]); ok {
			state = next
		}
		length := m.nodes[state].longest
		if length == 0 {
			continue
		}
		start, end := i+1-length, i+1
		for j := max(start, coveredEnd); j < end; j++ {
			redacted[j] = true
		}
		coveredEnd = max(coveredEnd, end)
		found = true
	}
	return found
}
