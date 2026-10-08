package flowdebug

import (
	"cmp"
	"context"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
)

// Complete offers what may be written at the end of line, over the target.
//
// It is [Session.Complete] for a front that has no session of its own: a durable
// run from `flow debug attach`, a retained MCP session. A prompt completes over
// the paused run's scope directly; a driver can only ask the target, so the
// answer here is built from what the target already says about itself — its
// snapshot for step ids and breakpoints, and [Target.Inspect] for the names in
// scope — and so it reaches exactly as far as the caller's own `inspect` does.
// A target that refuses an inspection (the caller lacks the durable
// `workload.debug_inspect` action, or the run is not held) leaves the verbs and
// step ids and drops the names, rather than failing the keystroke.
//
// # A candidate is a name, and never a value
//
// The same rule [Session.Complete] states holds here for the same reason. An
// inspection answers with each child's rendered value; this reads only the
// child's *name* from it, and describes the name rather than what it holds. A
// name the target's redactor withheld arrives as [v1.SensitiveMarker], which is
// no name an expression could use, so it is dropped rather than inserted.
//
// # Bounded
//
// The answer is bounded by [celcomplete.MaxCandidates] and says when it reached
// it, and each call is at most one snapshot and a few inspections: the root
// listing is remembered for the revision it was read at, because a person
// pressing tab twice at one stop is asking the same question.
func (d *Driver) Complete(ctx context.Context, line string) (Completion, error) {
	trimmed := strings.TrimLeft(line, " \t")
	typed, rest := cutWord(trimmed)
	if typed == trimmed {
		// Still on the first word: the verbs themselves.
		return d.offerCommands(trimmed), nil
	}

	known, ok := resolveOn(typed, frontDriver)
	if !ok {
		return Completion{}, nil
	}

	switch known.completes {
	case completesExpression:
		return d.offerExpression(ctx, rest)

	case completesStep:
		// The grammar is the verb's own, read the way [Driver.DoWith] reads it
		// and not the prompt's: `break` takes a `hit <count>` clause and a
		// condition, and past an `if` the argument is an expression, so a step id
		// inserted into a condition is a name it cannot mean. `until` and `log`
		// take no condition here — a driver refuses a conditional `until`, and a
		// logpoint's message is text with holes — so past the step id there is
		// nothing to complete.
		if known.verb == "break" {
			stripped, hit, err := cutHitClause(rest)
			if err != nil {
				return Completion{}, nil
			}
			_, condition, conditional, err := splitCondition(stripped, grammarBreak)
			if err != nil {
				return Completion{}, nil
			}
			if conditional {
				return d.offerExpression(ctx, condition)
			}
			if hit != "" {
				return Completion{}, nil
			}
		}
		if strings.ContainsAny(strings.TrimLeft(rest, " \t"), " \t") {
			return Completion{}, nil
		}

		snapshot, err := d.target.Snapshot(ctx)
		if err != nil {
			return Completion{}, err
		}

		return offerNamesFrom(rest, stepIDs(snapshot), "a step this run is at or holds a breakpoint on"), nil

	case completesBreakpoint:
		snapshot, err := d.target.Snapshot(ctx)
		if err != nil {
			return Completion{}, err
		}

		// The whole argument, not its last word: a logpoint is deleted under the
		// name `log <step>`, which has a space in it.
		return offerNamesFrom(strings.TrimLeft(rest, " \t"), breakpointSteps(snapshot), "a breakpoint this session holds"), nil

	default:
		return Completion{}, nil
	}
}

// CompleteExpression is [Driver.Complete] for a front whose text is an
// expression and not a command line: a debug console that evaluates whatever is
// typed. The names offered, and what is withheld, are the same as after `inspect
// ` at a driver.
func (d *Driver) CompleteExpression(ctx context.Context, expression string) (Completion, error) {
	return d.offerExpression(ctx, expression)
}

// offerCommands offers the verbs a driver answers, spelled as a driver spells
// them.
func (d *Driver) offerCommands(prefix string) Completion {
	out := Completion{Prefix: prefix}
	for _, c := range commandsOn(frontDriver) {
		if !strings.HasPrefix(c.verb, prefix) {
			continue
		}
		out.add(Candidate{
			Text:      c.verb + argumentSpace(c),
			Detail:    c.helpOn(frontDriver),
			Continues: c.argument != "",
		}, neverWithheld)
	}

	return out
}

// offerExpression completes the reference at the end of an expression by asking
// the target what is in scope there.
//
// Only a reference is completed: a path of names joined by dots, which is what
// scope is made of. Functions, operators and the members of a type are the
// profile's, and a driver has no profile to read them from; the prompt, which
// has the run's own scope, offers them.
func (d *Driver) offerExpression(ctx context.Context, expression string) (Completion, error) {
	path := trailingReference(expression)
	parent, prefix := "", path
	if dot := strings.LastIndexByte(path, '.'); dot >= 0 {
		parent, prefix = path[:dot], path[dot+1:]
	}
	out := Completion{Prefix: prefix}

	snapshot, err := d.target.Snapshot(ctx)
	if err != nil {
		return out, err
	}
	revision := snapshot.GetRevision()

	if parent == "" {
		for _, root := range d.rootNames(ctx, revision) {
			if strings.HasPrefix(root.Text, prefix) {
				out.add(root, neverWithheld)
			}
		}

		return out, nil
	}

	// Past the names an author wrote there is only data. A step's outputs are
	// named by the step, and a map under one of them is keyed by whatever the run
	// produced, so a key there is the datum and the prompt, which reads the same
	// rule off the scope it holds, offers none. Neither does a driver, whatever
	// the target would answer: the cut is made here, by the shape of the
	// reference, and not left to a redactor to catch.
	if !namesOnly(parent) {
		return out, nil
	}

	answer, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{
		Revision: revision, Expression: parent, Children: true, Limit: celcomplete.MaxCandidates,
	})
	if err != nil || answer.GetError() != "" {
		return out, nil
	}
	for _, child := range answer.GetChildren() {
		name := child.GetName()
		if !isReference(name) || !strings.HasPrefix(name, prefix) {
			continue
		}
		out.add(Candidate{Text: name, Detail: "a name under " + parent}, neverWithheld)
	}
	if int(answer.GetTotal()) > len(answer.GetChildren()) {
		out.Truncated = true
	}

	return out, nil
}

// namesOnly reports whether everything directly under parent is a name an author
// wrote: the members of a scope root (`steps`, `inputs`, `vars`), and the outputs
// of one step (`steps.<id>`). Anything deeper is the run's data.
func namesOnly(parent string) bool {
	segments := strings.Split(parent, ".")
	switch len(segments) {
	case 1:
		return isReference(segments[0])
	case 2:
		return segments[0] == "steps" && isReference(segments[1])
	default:
		return false
	}
}

// rootNames lists what an expression may begin with: each scope group's root
// written with the dot that continues it, and the bare names of a group that is
// not rooted. Read once per revision.
//
// A group's first member says which it is. A member whose expression begins with
// its own name is bare, so every member of that group is a root; any other group
// is reached through the one name its members' expressions share.
func (d *Driver) rootNames(ctx context.Context, revision uint64) []Candidate {
	if d.roots != nil && d.rootsRevision == revision {
		return d.roots
	}

	var out []Candidate
	seen := map[string]bool{}
	add := func(candidate Candidate) {
		if !seen[candidate.Text] && isReference(strings.TrimSuffix(candidate.Text, ".")) {
			seen[candidate.Text] = true
			out = append(out, candidate)
		}
	}

	// `steps` is how the language is spelled, so a run held at its first step,
	// before any has produced outputs, still teaches it — the exception the prompt
	// makes for the same root.
	add(Candidate{Text: "steps.", Detail: "a scope root", Continues: true})

	groups, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{Revision: revision})
	if err != nil {
		// Not remembered: a refusal or a failed round trip is not the stop's
		// answer, and the next tab should ask again.
		return out
	}
	for _, group := range groups.GetChildren() {
		handle := group.GetValue().GetExpression()
		first, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{Revision: revision, Expression: handle, Limit: 1})
		if err != nil || len(first.GetChildren()) == 0 {
			continue
		}
		member := first.GetChildren()[0]
		root := firstSegment(member.GetValue().GetExpression())
		if root != member.GetName() {
			add(Candidate{Text: root + ".", Detail: "a scope root", Continues: true})

			continue
		}

		all, err := d.target.Inspect(ctx, &v1.DebugInspectRequest{Revision: revision, Expression: handle, Limit: celcomplete.MaxCandidates})
		if err != nil {
			continue
		}
		for _, child := range all.GetChildren() {
			add(Candidate{Text: child.GetName(), Detail: "a name in scope"})
		}
	}
	slices.SortFunc(out, func(a, b Candidate) int { return cmp.Compare(a.Text, b.Text) })

	d.roots, d.rootsRevision = out, revision

	return out
}

// offerNamesFrom is [Session.offerNames] for names a driver already holds.
func offerNamesFrom(prefix string, names []string, detail string) Completion {
	out := Completion{Prefix: prefix}
	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			out.add(Candidate{Text: name, Detail: detail}, neverWithheld)
		}
	}

	return out
}

// neverWithheld is the withholding rule for a driver: it holds no redactor of its
// own. The names it offers were listed by a target that already drops a scope
// name its redactor would change, and [isReference] refuses the marker that
// stands in for one it withheld.
func neverWithheld(Candidate) bool { return false }

// stepIDs are the steps a snapshot names: where the run is held, each frame
// above it, and every breakpoint's step. A driver cannot know what the run may
// reach, which is the program's and not the snapshot's, so it offers what the
// target has said.
func stepIDs(snapshot *v1.DebugSnapshot) []string {
	var ids []string
	add := func(id string) {
		if id != "" && !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	if path := snapshot.GetOccurrence().GetSite().GetPath(); len(path) > 0 {
		add(path[len(path)-1])
	}
	for _, frame := range snapshot.GetFrames() {
		if path := frame.GetOccurrence().GetSite().GetPath(); len(path) > 0 {
			add(path[len(path)-1])
		}
	}
	for _, step := range breakpointSteps(snapshot) {
		add(step)
	}
	slices.Sort(ids)

	return ids
}

// breakpointSteps are the names the snapshot's breakpoints answer to: what
// `breakpoints` lists and `delete` takes.
func breakpointSteps(snapshot *v1.DebugSnapshot) []string {
	var steps []string
	for _, state := range snapshot.GetBreakpoints() {
		if id := state.GetId(); id != "" && !slices.Contains(steps, id) {
			steps = append(steps, id)
		}
	}
	slices.Sort(steps)

	return steps
}

// trailingReference is the dotted path of names at the end of expression, which
// is what a completion replaces the last segment of.
func trailingReference(expression string) string {
	start := len(expression)
	for start > 0 {
		c := expression[start-1]
		if c == '.' || c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
			start--

			continue
		}

		break
	}

	return expression[start:]
}

// firstSegment is the name an expression begins with.
func firstSegment(expression string) string {
	if end := strings.IndexAny(expression, ".["); end >= 0 {
		return expression[:end]
	}

	return expression
}

// isReference reports whether name can be written as a bare segment of a
// reference. It refuses the marker for a withheld name along with everything
// else an expression would need quoting to hold.
func isReference(name string) bool {
	if name == "" || name == v1.SensitiveMarker {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		letter := c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		if !letter && (i == 0 || c < '0' || c > '9') {
			return false
		}
	}

	return true
}
