package flowdebug

import (
	"context"
	"fmt"
	"math"
	"strings"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A session's answers as the schema spells them (`proto/flowstate/v1/debug.proto`,
// #928's durable-debug arc, stage 1).
//
// # Why this is a bridge rather than a replacement
//
// Proto-first says the shape of a type describing the system comes from the
// schema, and the honest reading of that here is that the *wire* shape does.
// The session's own types stay: [Names] carries an unexported `listing` that is
// a fact about the text prompt rather than about the scope, so proto-ifying it
// wholesale produces a message plus a Go companion — two shapes where there is
// one, which #1120's follow-up named as the concrete obstruction and which
// replacing the types would not remove. The cost of the bridge, stated plainly:
// two shapes for one fact, and a chance for them to drift.
//
// What keeps them from drifting is that these functions are *derived* rather
// than parallel. Each reads the same field, list or resolver the session's own
// accessor reads — [Session.StepWindowProto] and [Session.Steps] are one
// computation ([Session.stepWindow]) rendered twice, and every rendered value
// here comes back through [Session.Evaluate] rather than from the scope
// directly. A conformance-shaped test in `cmd/flow/internal/debugpane` drives
// one session and asserts the local pane's frame and these messages carry the
// same content, which is the claim that would fail first if they drifted.
//
// # Narrowing counts
//
// Go counts are `int` and the schema's are `int32`. Every count here is a slice
// length or an index into one, so it is bounded by what an inventory fits in
// memory long before it reaches 2^31. The one number an embedder chooses freely
// is [Step.Declaration], and the position's copy of it carries explicit
// presence — so an unrepresentable declaration is reported as *not
// attributable*, which is a legal answer this design already has, rather than
// as a wrapped number naming the wrong row.

// PositionProto is [Session.Paused] as a wire message, with the declaration the
// position resolves to when it resolves to one.
//
// The second return is [Session.Paused]'s boolean and means the same thing:
// there is no position at all, as distinct from a position with no step.
//
// The declaration is filled here rather than by a consumer because resolving a
// position against rows is one function on this side ([positionIn]), and two
// resolvers is how the local surface came to mark both rows of a callee invoked
// twice. Absent means this session could not tell which row — the same
// fail-closed answer the pane draws by marking none.
func (s *Session) PositionProto() (*v1.DebugPosition, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	subject := s.at
	if subject.scope == nil {
		return nil, false
	}

	position := &v1.DebugPosition{
		StepId:   subject.step,
		Workflow: subject.workflow,
		Kind:     subject.kind,
		Autopsy:  subject.autopsy,
	}

	// An autopsy needs no arm of its own, and writing one would be an arm no
	// test could reach: an autopsy holds a scope and no step, and [positionIn]
	// already answers -1 for an empty id. One resolver, asked the same question
	// it is asked everywhere else.
	order := s.inventory()
	index := positionIn(order, subject.workflow, subject.step)
	if index < 0 {
		return position, true
	}

	// Narrowed without a check, because [New] refused an inventory whose
	// declarations the wire cannot say — see [validDeclaration].
	position.Declaration = proto.Int32(int32(order[index].Declaration))

	return position, true
}

// StepWindowProto is [Session.Steps] as a wire message, with the held row's
// index inside the window it returns.
//
// Held is the answer rather than the inputs to it. A consumer re-deriving it
// from ids would be a second resolver of "which row is this", and the one this
// package has already answers -1 both when nothing matches and when more than
// one row does.
//
// The offset and limit are the caller's, clamped exactly as [Session.Steps]
// clamps them: this API is shaped for a caller that does not exist yet, so the
// limit is untrusted, and a window measured as offset+limit wraps negative on a
// limit near the maximum.
func (s *Session) StepWindowProto(offset, limit int) *v1.DebugStepWindow {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, held := s.stepWindow(offset, limit)

	window := &v1.DebugStepWindow{
		Steps:        make([]*v1.DebugStep, 0, len(list.Steps)),
		Offset:       int32(list.Offset),
		Total:        int32(list.Total),
		Unattributed: int32(list.Unattributed),
		Truncated:    list.Truncated,
	}

	for _, step := range list.Steps {
		window.Steps = append(window.Steps, &v1.DebugStep{
			Workflow: step.Workflow,
			// Narrowed without a check for [Session.PositionProto]'s reason:
			// the inventory was refused at the door if it could not be said.
			Declaration: int32(step.Declaration),
			Via:         step.Via,
			StepId:      step.ID,
			State:       stepStates[step.State],
		})
	}

	// Set only where the held row is at an index of *this* window. Outside it,
	// the row is still real and the position's declaration says so — the two
	// absences are told apart there rather than conflated here.
	if held >= list.Offset && held < list.Offset+len(list.Steps) {
		window.Held = proto.Int32(int32(held - list.Offset))
	}

	return window
}

// ScopeProto is [Session.Scope] as a wire message, with the value of the first
// limit names resolved.
//
// # Why the caller says how many
//
// Resolving a value is an evaluation, so how many to resolve is a budget rather
// than a property of the scope — the same reason [Session.Steps] takes its
// window from its caller and [MaxScopeNames] is applied by a renderer instead
// of here. A negative limit resolves every name; a zero limit resolves none and
// answers with the names alone, which is what a debug adapter's `scopes`
// request wants before anyone has expanded a pane.
//
// Every group keeps its total whatever the budget does, so a group entirely
// past it arrives with no bindings rather than vanishing: that the group exists
// is part of the answer to what kind of name a run can reach.
//
// Every value goes through [Session.Evaluate], which is the redacting door, and
// a failed evaluation becomes the binding's error rather than dropping the
// binding — so this message cannot come to list fewer names than the scope
// holds.
func (s *Session) ScopeProto(ctx context.Context, limit int) (*v1.DebugScope, error) {
	groups, err := s.Scope()
	if err != nil {
		return nil, err
	}

	scope := &v1.DebugScope{Groups: make([]*v1.DebugScopeGroup, 0, len(groups))}

	resolved := 0
	for _, group := range groups {
		out := &v1.DebugScopeGroup{
			Group:    group.Group,
			Root:     group.Root,
			Bindings: make([]*v1.DebugBinding, 0, len(group.Names)),
			Total:    int32(len(group.Names)),
		}
		scope.Total += int32(len(group.Names))

		for _, name := range group.Names {
			binding := &v1.DebugBinding{Name: name, Expression: expressionFor(group.Root, name)}
			out.Bindings = append(out.Bindings, binding)

			if limit >= 0 && resolved >= limit {
				// Counted past the budget by [DebugScopeGroup.total] above,
				// deliberately: the total is what makes an elision honest, and
				// a name whose value nobody asked for is still a name the run
				// can reach. The binding's answer stays unset, which is the
				// oneof's third state and means exactly this.
				continue
			}
			resolved++

			text, _, evalErr := s.Evaluate(ctx, binding.GetExpression())
			if evalErr != nil {
				// The name is real — the run said so — and only its value
				// could not be produced. The error is already redacted by the
				// same seam the value would have been, and capped here because
				// nothing else caps it: a rendered value is capped by
				// [Session.Evaluate] and an error is not, and a wire message
				// must not carry a string whose length a peer's failure chose.
				binding.Answer = &v1.DebugBinding_Error{Error: capRunes(evalErr.Error(), MaxInspectRunes)}

				continue
			}

			binding.Answer = &v1.DebugBinding_Rendered{Rendered: text}
		}

		scope.Groups = append(scope.Groups, out)
	}

	return scope, nil
}

// SessionProto is who is debugging which run, for a session of this package.
//
// Every field but [v1.DebugSession.local] is the zero value, and that is the
// answer rather than a gap: this package's sessions hold *local* runs — see the
// package doc on [v1.Debugger] being a local-driver seam — so nothing attested
// a caller, there is no durable execution to address, and there is no lease,
// because #928's recorded decision is that local debugging is the author's own
// process and always on. Stage 2's durable producer is what fills the rest.
func (s *Session) SessionProto() *v1.DebugSession {
	return &v1.DebugSession{Local: true}
}

// expressionFor is what would be typed to ask for one name again.
//
// The session's own answer for what a group's names hang from, rather than a
// switch over group labels: two renderers kept such a switch, and one of them
// said in its own comment that it was the same fact read for a different
// renderer.
func expressionFor(root, name string) string {
	if root == "" {
		return name
	}

	return root + "." + name
}

// validDeclaration reports whether a declaration number is one the wire can
// say, which is what [New] refuses an inventory for.
//
// Non-negative because a declaration numbers a walk's descents from the root's
// zero upward, and `DebugStep.declaration` carries `gte: 0` to say so; bounded
// above because the schema's field is an int32 and Go's is an int, and a
// wrapped declaration names a *different invocation*, which is the one thing
// this whole design refuses to do.
//
// A function taking its input, and checked at the door rather than where a row
// is built, for the two reasons CLAUDE.md gives. Every inventory this
// repository produces holds small numbers, so a check written where those are
// read is one no test could reach; and the door is the only place with an
// error to return, so it is the only place the refusal can name what is wrong.
func validDeclaration(n int) bool {
	return n >= 0 && n <= math.MaxInt32
}

// stepStates maps this package's outcome vocabulary onto the schema's.
//
// Written out rather than computed from the iota, because the two are
// deliberately offset: [StepPending] is the Go zero value and means "this
// session watched nothing happen here", while the schema's zero is
// DEBUG_STEP_STATE_UNSPECIFIED and means "the producer did not say". A
// numeric identity between them would report every unwatched step as an answer
// nobody gave. TestEveryStepStateHasAWireValue walks both vocabularies against
// this table.
var stepStates = map[StepState]v1.DebugStepState{
	StepPending:   v1.DebugStepState_DEBUG_STEP_STATE_PENDING,
	StepRunning:   v1.DebugStepState_DEBUG_STEP_STATE_RUNNING,
	StepDone:      v1.DebugStepState_DEBUG_STEP_STATE_DONE,
	StepTolerated: v1.DebugStepState_DEBUG_STEP_STATE_TOLERATED,
	StepFailed:    v1.DebugStepState_DEBUG_STEP_STATE_FAILED,
	StepSkipped:   v1.DebugStepState_DEBUG_STEP_STATE_SKIPPED,
}

// verbs maps the schema's command vocabulary onto the canonical spellings in
// [commands], which is the one place a verb is written down.
//
// A table rather than a string conversion off the enum name, because the enum
// name is a wire fact and the verb is what `dispatch` switches on: deriving one
// from the other by text would make renaming either a silent change to the
// other. TestEveryCommandVerbIsOnTheWire walks [commands] and this enum's own
// descriptor against this table in both directions, so a verb cannot come to be
// understood by the prompt and unknown on the wire, or the reverse.
var verbs = map[v1.DebugCommandVerb]string{
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_STEP:        "step",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_CONTINUE:    "continue",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_UNTIL:       "until",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_BREAK:       "break",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_DELETE:      "delete",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_BREAKPOINTS: "breakpoints",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_INSPECT:     "inspect",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_SCOPE:       "scope",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_COMPLETE:    "complete",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_INFO:        "info",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_QUIT:        "quit",
	v1.DebugCommandVerb_DEBUG_COMMAND_VERB_HELP:        "help",
}

// CommandProto is one command line as a wire message, reporting whether the
// line is a command this session understands.
//
// It answers false for a comment and for an unknown verb, which are the two
// things a line can be that a command is not. An empty line is [step], because
// that is what the prompt does with a bare newline and a wire client sending
// one means what a person pressing return means.
//
// Aliases resolve, so `s` and `p` arrive as the verbs they are short for: the
// alias table is the prompt's, and the wire's vocabulary is canonical.
func CommandProto(line string) (*v1.DebugCommand, bool) {
	// The session's own two refusals about a *line*, made before anything is
	// read out of it, so that this function cannot build a message out of input
	// the session would not have accepted ([Session.takeControl]).
	//
	// Length first, and first of everything: a line of nothing but whitespace,
	// however long, trims to the empty string and would otherwise be normalized
	// into `step` — an over-long line the console rejects, turned into a valid
	// command by the very step that was meant to read it (Codex, #1194).
	if len(line) > MaxCommandBytes {
		return nil, false
	}

	// One command is one line. A caller handing two of them at once is handing
	// something no prompt could have produced, with the second landing wherever
	// the first left the run.
	if strings.ContainsAny(line, "\r\n") {
		return nil, false
	}

	if IsComment(line) {
		return nil, false
	}

	typed, rest := split(line)
	if typed == "" {
		return &v1.DebugCommand{Verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_STEP}, true
	}

	known, ok := resolve(typed)
	if !ok {
		return nil, false
	}

	// A named survivor. TestEveryCommandVerbIsOnTheWire forbids a table verb
	// with no wire value, so no mutation can reach this arm and deleting it
	// survives — but what it would leave is a `DebugCommand` carrying the
	// unspecified verb, reported as understood, on exactly the day somebody
	// edits one table and not the other. Fail closed, written down rather than
	// left as coverage nobody can account for (CLAUDE.md). Its twin is in
	// [CommandLine], for the same reason in the other direction.
	verb, ok := verbFor(known.verb)
	if !ok {
		return nil, false
	}

	// `complete`'s argument is taken from the raw line rather than from the
	// trimmed remainder, because trailing space is the thing that says the
	// current word is empty — the same distinction [cutWord] exists for, and
	// the one [Session.dispatch] makes at this exact verb. Trimming it here
	// would move a remote client's cursor three characters left of where it is.
	if known.verb == "complete" {
		_, text := cutWord(strings.TrimLeft(line, " \t"))

		return &v1.DebugCommand{Verb: verb, Argument: text}, true
	}

	// A verb the table gives no argument gets none, whatever followed it.
	//
	// Dropped rather than carried, and dropped rather than refused, because
	// dropping is what the *session* does: `dispatch` reads no remainder for
	// these verbs and records the bare verb — `s.record("scope")`, not the line
	// that was typed. So `scope anything` means `scope`, here as there, and the
	// message this returns renders back to the line the session would have
	// recorded.
	//
	// Refusing instead would make a line the prompt happily runs into "not a
	// command", and carrying it would make a message [CommandLine] refuses:
	// this function must only ever produce messages that one can render, or the
	// pair is not a pair (Codex, #1194).
	if known.argument == "" {
		return &v1.DebugCommand{Verb: verb}, true
	}

	return &v1.DebugCommand{Verb: verb, Argument: strings.TrimSpace(rest)}, true
}

// CommandLine is a wire command as the line [Session.dispatch] understands.
//
// The canonical line and not a rendering of it: a session records the lines it
// accepted and replays a run from them, so what comes back here is the same
// artifact `flow debug replay` reads and a person pastes into an issue.
//
// It refuses two things rather than guessing. A command with no verb is not a
// command — the same answer the prompt gives an unknown one, for the reason a
// misspelled key in a file is reported rather than ignored. And an argument on
// a verb that takes none is refused rather than dropped, because dropping it
// would run a command the caller did not send; the table's own `argument` field
// is what says which verbs those are, so this cannot disagree with `help`.
//
// A *missing* argument is not refused, deliberately: `until` with nothing after
// it is a line the prompt answers with a usage sentence, and a wire client
// sending one should meet that same answer rather than a different refusal
// here.
//
// # The two refusals a field rule cannot make
//
// [Session.takeControl] refuses a line past [MaxCommandBytes] and a line
// holding a line break, and neither is expressible as a rule on
// `DebugCommand.argument`: the first is about the *line*, which is the verb and
// a separator longer than the argument, and the second is about a character
// this message has no reason to forbid in isolation. So a message can satisfy
// every schema rule and still be undeliverable to the session it names, which
// is a refusal arriving one layer too late (Codex, #1194).
//
// They are made here, where the line is built, and they are the session's own
// two checks rather than a second opinion about them — the bound is
// [MaxCommandBytes] and the character set is the one `takeControl` names.
func CommandLine(command *v1.DebugCommand) (string, error) {
	verb, ok := verbs[command.GetVerb()]
	if !ok {
		return "", fmt.Errorf("flowdebug: %v is not a command verb", command.GetVerb())
	}

	argument := command.GetArgument()
	if strings.ContainsAny(argument, "\r\n") {
		return "", fmt.Errorf("flowdebug: a command is one line, and %s's argument holds a line break", verb)
	}
	if argument == "" {
		return verb, nil
	}

	// [CommandProto]'s named survivor, in the other direction: a wire verb the
	// table no longer spells resolves to the zero command, whose argument is
	// empty, so it refuses here too. TestEveryCommandVerbIsOnTheWire forbids
	// that state, which is why the `!ok` half is unreachable and a mutation
	// deleting it survives — it is kept because the answer it gives is the
	// fail-closed one and the alternative is rendering a line for a verb
	// nothing understands.
	known, ok := resolve(verb)
	if !ok || known.argument == "" {
		return "", fmt.Errorf("flowdebug: %s takes no argument, and %q was sent with it", verb, argument)
	}

	line := verb + " " + argument
	if len(line) > MaxCommandBytes {
		return "", fmt.Errorf(
			"flowdebug: a command may be %d bytes and this one is %d; the verb and its separator count toward it, so an argument at the field's own limit does not fit",
			MaxCommandBytes, len(line))
	}

	return line, nil
}

// verbFor is the wire verb for a canonical spelling, the reverse of [verbs].
//
// Derived from that map rather than written beside it: a second table is how
// the two directions come to disagree about one verb.
func verbFor(canonical string) (v1.DebugCommandVerb, bool) {
	for verb, spelling := range verbs {
		if spelling == canonical {
			return verb, true
		}
	}

	return v1.DebugCommandVerb_DEBUG_COMMAND_VERB_UNSPECIFIED, false
}
