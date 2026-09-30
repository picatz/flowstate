package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// Dynamic occurrence identity for the step debugger (#2123), and the one
// address grammar every debugger surface reads and writes (#1439).
//
// A [DebugSite] is a place in the compiled program; a [DebugOccurrence] is one
// arrival there. The local driver records the dynamic nesting an arrival
// happened in — the call, the iteration, the parallel branch, the switch arm —
// on the run's own context, and only while a [Debugger] is installed, so a run
// nobody is debugging pays one context lookup per container and allocates
// nothing for it.
//
// # The address grammar
//
// An occurrence address is its segments and its step joined by `/`:
//
//	pages[2]/page          the step `page` in iteration 2 of the loop `pages`
//	checks#1/check_quota   the step in branch 1 of the parallel `checks`
//	route?0/chosen         the step in case 0 of the switch `route`
//	fan_out(child)/greet   the step `greet` in the workflow `child` that the call `fan_out` ran
//
// A [DebugTarget] is the same grammar with every qualifier optional, matched
// against the *end* of an address: `page` is every `page`, `pages/page` is the
// `page` in `pages`, and `pages[2]/page` is that one iteration's. So a bare id
// arms every site that declares it (a caller's `build` and a callee's alike,
// #1439), and anything longer narrows it — never a guess between two.

// MaxDebugSegments bounds the dynamic nesting an occurrence carries, which is
// the schema's own bound on [DebugOccurrence.Segments].
const MaxDebugSegments = 128

// MaxDebugAddressRunes is the schema's bound on an occurrence address. A longer
// address keeps its innermost end, which names the step, behind an elision.
const MaxDebugAddressRunes = 4096

// contextWithSegment returns ctx with one more level of dynamic nesting, when a
// debugger is installed, and ctx itself otherwise.
func contextWithSegment(ctx context.Context, kind DebugSegmentKind, stepID string, index int) context.Context {
	if DebuggerFromContext(ctx) == nil {
		return ctx
	}

	position, _ := ctx.Value(executingWorkflowKey{}).(executingPosition)
	if len(position.segments) >= MaxDebugSegments {
		return ctx
	}
	position.segments = append(slices.Clip(position.segments), &DebugSegment{
		Kind:     kind,
		StepId:   stepID,
		Workflow: position.workflow,
		Index:    int32(index),
	})

	return context.WithValue(ctx, executingWorkflowKey{}, position)
}

// ExecutingOccurrenceFromContext returns the occurrence of node being reached
// under ctx. The segments are those recorded while a debugger was installed;
// without one, the occurrence names only the step in its workflow.
func ExecutingOccurrenceFromContext(ctx context.Context, node *Node) *DebugOccurrence {
	position, _ := ctx.Value(executingWorkflowKey{}).(executingPosition)

	return NewDebugOccurrence(position.workflow, position.segments, node.GetId(), NodeKind(node))
}

// ExecutingAddressFromContext is the address of the step id being reached
// under ctx, as [ExecutingOccurrenceFromContext] would write it, without the
// occurrence around it.
func ExecutingAddressFromContext(ctx context.Context, id string) string {
	position, _ := ctx.Value(executingWorkflowKey{}).(executingPosition)

	return FormatDebugAddress(position.segments, id)
}

// NewDebugOccurrence builds the occurrence of step, declared by workflow,
// reached under segments (outermost first). The site's path is the containers
// after the innermost call, since a callee's steps belong to the callee.
func NewDebugOccurrence(workflow string, segments []*DebugSegment, step, kind string) *DebugOccurrence {
	start := 0
	for i, segment := range segments {
		if segment.GetKind() == DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL {
			start = i + 1
		}
	}

	path := make([]string, 0, len(segments)-start+1)
	for _, segment := range segments[start:] {
		path = append(path, segment.GetStepId())
	}
	path = append(path, step)

	return &DebugOccurrence{
		Site:     &DebugSite{Workflow: workflow, Path: path, Kind: kind},
		Segments: slices.Clone(segments),
		Address:  FormatDebugAddress(segments, step),
	}
}

// FormatDebugAddress writes an occurrence address in the grammar above.
func FormatDebugAddress(segments []*DebugSegment, step string) string {
	var b strings.Builder
	for _, segment := range segments {
		b.WriteString(segment.GetStepId())
		index := strconv.Itoa(int(segment.GetIndex()))
		switch segment.GetKind() {
		case DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION:
			b.WriteString("[" + index + "]")
		case DebugSegmentKind_DEBUG_SEGMENT_KIND_BRANCH:
			b.WriteString("#" + index)
		case DebugSegmentKind_DEBUG_SEGMENT_KIND_CASE:
			b.WriteString("?" + index)
		case DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL:
			b.WriteString("(" + segment.GetCallee() + ")")
		}
		b.WriteByte('/')
	}
	b.WriteString(step)

	address := []rune(b.String())
	if len(address) <= MaxDebugAddressRunes {
		return b.String()
	}
	const elision = "…/"

	return elision + string(address[len(address)-(MaxDebugAddressRunes-len([]rune(elision))):])
}

// DebugSiteKey is a site's canonical text: its workflow, then its path joined
// by `/`. Two sites are the same site exactly when their keys are equal.
func DebugSiteKey(site *DebugSite) string {
	return site.GetWorkflow() + ":" + strings.Join(site.GetPath(), "/")
}

// DebugTarget is a parsed breakpoint or run-until target: a suffix of an
// occurrence address, each part optionally qualified.
type DebugTarget struct {
	parts []targetPart
}

type targetPart struct {
	id string

	// kind is the qualifier's segment kind, or unspecified for none.
	kind DebugSegmentKind
	// index is the qualifier's index, for an iteration, branch, or case.
	index int
	// callee is the qualifier's callee, for a call.
	callee string
}

// MaxDebugTargetBytes bounds a target's text.
const MaxDebugTargetBytes = 4096

// ParseDebugTarget parses text in the address grammar. Every part but the last
// may carry a qualifier; the last is a step id.
func ParseDebugTarget(text string) (DebugTarget, error) {
	if text == "" {
		return DebugTarget{}, errors.New("a target names a step, and this one is empty")
	}
	if len(text) > MaxDebugTargetBytes {
		return DebugTarget{}, fmt.Errorf("a target may be %d bytes, and this one is %d", MaxDebugTargetBytes, len(text))
	}

	raw := strings.Split(text, "/")
	if len(raw) > MaxDebugSegments+1 {
		return DebugTarget{}, fmt.Errorf("a target may nest %d levels, and this one nests %d", MaxDebugSegments, len(raw)-1)
	}

	parts := make([]targetPart, 0, len(raw))
	for i, word := range raw {
		part, err := parseTargetPart(word, i == len(raw)-1)
		if err != nil {
			return DebugTarget{}, fmt.Errorf("target %q: %w", text, err)
		}
		parts = append(parts, part)
	}

	return DebugTarget{parts: parts}, nil
}

func parseTargetPart(word string, last bool) (targetPart, error) {
	end := strings.IndexAny(word, "[#?(")
	if end < 0 {
		if err := checkTargetID(word); err != nil {
			return targetPart{}, err
		}

		return targetPart{id: word}, nil
	}
	if last {
		return targetPart{}, fmt.Errorf("the last part names a step, and %q carries a qualifier", word)
	}

	id, qualifier := word[:end], word[end:]
	if err := checkTargetID(id); err != nil {
		return targetPart{}, err
	}

	switch qualifier[0] {
	case '(':
		callee, ok := strings.CutSuffix(qualifier[1:], ")")
		if !ok || callee == "" || strings.ContainsAny(callee, "()[]#?/") {
			return targetPart{}, fmt.Errorf("%q: a call is written `id(callee)`", word)
		}

		return targetPart{id: id, kind: DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL, callee: callee}, nil

	case '[':
		digits, ok := strings.CutSuffix(qualifier[1:], "]")
		if !ok {
			return targetPart{}, fmt.Errorf("%q: an iteration is written `id[n]`", word)
		}

		return indexedPart(word, id, DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, digits)

	case '#':
		return indexedPart(word, id, DebugSegmentKind_DEBUG_SEGMENT_KIND_BRANCH, qualifier[1:])

	default:
		return indexedPart(word, id, DebugSegmentKind_DEBUG_SEGMENT_KIND_CASE, qualifier[1:])
	}
}

func indexedPart(word, id string, kind DebugSegmentKind, digits string) (targetPart, error) {
	index, err := strconv.Atoi(digits)
	if err != nil || index < 0 || strconv.Itoa(index) != digits {
		return targetPart{}, fmt.Errorf("%q: an index is a non-negative decimal integer", word)
	}

	return targetPart{id: id, kind: kind, index: index}, nil
}

func checkTargetID(id string) error {
	if id == "" {
		return errors.New("a part of a target is empty")
	}
	for _, r := range id {
		if r <= ' ' || r == 0x7f || strings.ContainsRune("/[]#?()", r) {
			return fmt.Errorf("step id %q holds %q, which the address grammar reserves or cannot quote", id, r)
		}
	}

	return nil
}

// DebugTargetForStep is the target naming every site whose step id is id,
// taken literally rather than parsed: a step id the grammar cannot spell (one
// holding a space, say) is still a step a person may break at.
func DebugTargetForStep(id string) DebugTarget {
	return DebugTarget{parts: []targetPart{{id: id}}}
}

// ParseDebugTargetOrStep parses text in the address grammar, falling back to
// [DebugTargetForStep] for text the grammar cannot read.
func ParseDebugTargetOrStep(text string) DebugTarget {
	target, err := ParseDebugTarget(text)
	if err != nil {
		return DebugTargetForStep(text)
	}

	return target
}

// String writes the target back in the address grammar.
func (t DebugTarget) String() string {
	var b strings.Builder
	for i, part := range t.parts {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(part.id)
		switch part.kind {
		case DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION:
			b.WriteString("[" + strconv.Itoa(part.index) + "]")
		case DebugSegmentKind_DEBUG_SEGMENT_KIND_BRANCH:
			b.WriteString("#" + strconv.Itoa(part.index))
		case DebugSegmentKind_DEBUG_SEGMENT_KIND_CASE:
			b.WriteString("?" + strconv.Itoa(part.index))
		case DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL:
			b.WriteString("(" + part.callee + ")")
		}
	}

	return b.String()
}

// Step is the target's step id: its last part.
func (t DebugTarget) Step() string {
	if len(t.parts) == 0 {
		return ""
	}

	return t.parts[len(t.parts)-1].id
}

// Matches reports whether the occurrence is one this target names.
func (t DebugTarget) Matches(occurrence *DebugOccurrence) bool {
	if len(t.parts) == 0 {
		return false
	}

	segments := occurrence.GetSegments()
	step := occurrence.GetSite().GetPath()
	if len(step) == 0 {
		return false
	}
	if t.parts[len(t.parts)-1].id != step[len(step)-1] {
		return false
	}

	containers := t.parts[:len(t.parts)-1]
	if len(containers) > len(segments) {
		return false
	}
	offset := len(segments) - len(containers)
	for i, part := range containers {
		segment := segments[offset+i]
		if part.id != segment.GetStepId() {
			return false
		}
		if part.kind == DebugSegmentKind_DEBUG_SEGMENT_KIND_UNSPECIFIED {
			continue
		}
		if part.kind != segment.GetKind() {
			return false
		}
		if part.kind == DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL {
			if part.callee != segment.GetCallee() {
				return false
			}

			continue
		}
		if part.index != int(segment.GetIndex()) {
			return false
		}
	}

	return true
}

// DebugStaticSite is one site of a compiled program, with the static chain of
// containers that reaches it from the root workflow.
type DebugStaticSite struct {
	// Site is the site itself.
	Site *DebugSite

	// Chain is every container from the root workflow's top level down,
	// including calls, as segments without indices: a segment's Index is zero
	// and meaningless here.
	Chain []*DebugSegment

	// Locals are the bare names bound where the site's `if:` is evaluated,
	// which is where a breakpoint's condition is evaluated too: what each
	// enclosing container in the site's own workflow binds for its body
	// ([BodyLocals]). A call binds none of its caller's, and the site's own
	// `vars:` are not among them, because both drivers evaluate a condition
	// before installing them. Nil where nothing is bound.
	Locals *DebugBindings
}

// DebugBindings is the bare names bound at a site, as a chain: the names one
// container binds for its body, linked to the scope the container sits in.
//
// Linked rather than flattened, so a container costs its own names and no
// more. A flattened list copies every enclosing name into each container, and
// a specification the size bound admits can nest enough containers, each
// binding enough names, to make that copying the largest thing a worker
// holds. Every container's sites share its one node, and nothing writes one
// once the walk has built it.
type DebugBindings struct {
	names  []string
	parent *DebugBindings
}

// All yields every name the scope binds, innermost container first. A name
// two containers both bind is yielded for each.
func (s *DebugBindings) All() iter.Seq[string] {
	return func(yield func(string) bool) {
		for scope := s; scope != nil; scope = scope.parent {
			for _, name := range scope.names {
				if !yield(name) {
					return
				}
			}
		}
	}
}

// debugScopesOf yields each distinct scope node the sites reach, once, so a
// question asked of many sites costs the program's names rather than the
// sites times the names each can see.
func debugScopesOf(sites []DebugStaticSite) iter.Seq[*DebugBindings] {
	return func(yield func(*DebugBindings) bool) {
		seen := map[*DebugBindings]bool{}
		for _, site := range sites {
			for scope := site.Locals; scope != nil && !seen[scope]; scope = scope.parent {
				seen[scope] = true
				if !yield(scope) {
					return
				}
			}
		}
	}
}

// BodyLocals returns the bare names node binds for the steps inside it: its
// own `vars:`, a `for_each`'s [IteratorName], and a `loop:`'s `state:`. These
// are the bindings both drivers install around a container's body, and the
// rules [promptWalk.nodes] documents; a leaf binds its `vars:` for its own
// inputs, which no step inside it reads because it has none. The order is
// unspecified.
func BodyLocals(node *Node) []string {
	names := slices.Collect(maps.Keys(node.GetVars()))
	switch kind := node.GetKind().(type) {
	case *Node_ForEach:
		names = append(names, IteratorName(kind.ForEach))
	case *Node_Loop:
		if state := kind.Loop.GetState(); state != "" {
			names = append(names, state)
		}
	}

	return names
}

// MaxDebugStaticSites bounds how many sites [DebugStaticSites] enumerates.
const MaxDebugStaticSites = 1 << 16

// DebugStaticSites enumerates every step site of wf in document order,
// descending into loop bodies, parallel branches, switch arms, and called
// workflows. The second result reports that the enumeration stopped at
// [MaxDebugStaticSites].
func DebugStaticSites(wf *Workflow) ([]DebugStaticSite, bool) {
	var (
		sites     []DebugStaticSite
		truncated bool
	)

	var walk func(workflow *Workflow, chain []*DebugSegment, locals *DebugBindings, nodes []*Node, depth int)
	walk = func(workflow *Workflow, chain []*DebugSegment, locals *DebugBindings, nodes []*Node, depth int) {
		for _, node := range nodes {
			if len(sites) >= MaxDebugStaticSites {
				truncated = true

				return
			}

			occurrence := NewDebugOccurrence(workflow.GetName(), chain, node.GetId(), NodeKind(node))
			sites = append(sites, DebugStaticSite{Site: occurrence.GetSite(), Chain: slices.Clone(chain), Locals: locals})

			if len(chain) >= MaxDebugSegments {
				continue
			}
			into := func(kind DebugSegmentKind) []*DebugSegment {
				return append(slices.Clip(chain), &DebugSegment{
					Kind: kind, StepId: node.GetId(), Workflow: workflow.GetName(),
				})
			}

			// One node per container that binds anything, holding only what
			// it binds, shared by its body's sites. A call is left out: its
			// body is the callee's, which binds none of these.
			inner := locals
			switch node.GetKind().(type) {
			case *Node_ForEach, *Node_Loop, *Node_Parallel, *Node_Switch:
				if bound := BodyLocals(node); len(bound) > 0 {
					slices.Sort(bound)
					inner = &DebugBindings{names: slices.Clip(slices.Compact(bound)), parent: locals}
				}
			}

			switch kind := node.GetKind().(type) {
			case *Node_ForEach:
				walk(workflow, into(DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION), inner, kind.ForEach.GetBody(), depth)
			case *Node_Loop:
				walk(workflow, into(DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION), inner, kind.Loop.GetBody(), depth)
			case *Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					walk(workflow, into(DebugSegmentKind_DEBUG_SEGMENT_KIND_BRANCH), inner, branch.GetSteps(), depth)
				}
			case *Node_Switch:
				for _, arm := range kind.Switch.GetCases() {
					walk(workflow, into(DebugSegmentKind_DEBUG_SEGMENT_KIND_CASE), inner, arm.GetSteps(), depth)
				}
				walk(workflow, into(DebugSegmentKind_DEBUG_SEGMENT_KIND_CASE), inner, kind.Switch.GetDefault().GetSteps(), depth)
			case *Node_Call:
				if depth >= MaxCallDepth {
					continue
				}
				callee := kind.Call.GetWorkflow()
				call := into(DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL)
				call[len(call)-1].Callee = callee.GetName()
				// Isolated: a callee sees its own arguments and none of
				// the caller's bare names ([CallScope]).
				walk(callee, call, nil, callee.GetSteps(), depth+1)
			}
		}
	}
	walk(wf, nil, nil, wf.GetSteps(), 0)

	return sites, truncated
}

// DebugDeclaredSteps yields the id of every step wf declares, and every step a
// workflow it calls declares: at a top level, or in a loop body, parallel
// branch or switch arm, in document order, with repeats. It is the question a
// truncated [DebugStaticSites] leaves open, asked of the program as written
// rather than of its sites. A callee is walked again only from a shallower
// call than any before, which may reach calls the deeper one could not: a
// workflow built in memory may share one callee among many calls, and walking
// it once per call would expand exponentially through [MaxCallDepth] levels.
// So the walk is bounded by the program's own size, times the call depth. Calls
// are followed to [MaxCallDepth], as [DebugStaticSites] follows them.
func DebugDeclaredSteps(wf *Workflow) iter.Seq[string] {
	return func(yield func(string) bool) {
		shallowest := map[*Workflow]int{}
		var walk func(nodes []*Node, depth int) bool
		walk = func(nodes []*Node, depth int) bool {
			for _, node := range nodes {
				if !yield(node.GetId()) {
					return false
				}
				switch kind := node.GetKind().(type) {
				case *Node_ForEach:
					if !walk(kind.ForEach.GetBody(), depth) {
						return false
					}
				case *Node_Loop:
					if !walk(kind.Loop.GetBody(), depth) {
						return false
					}
				case *Node_Parallel:
					for _, branch := range kind.Parallel.GetBranches() {
						if !walk(branch.GetSteps(), depth) {
							return false
						}
					}
				case *Node_Switch:
					for _, arm := range kind.Switch.GetCases() {
						if !walk(arm.GetSteps(), depth) {
							return false
						}
					}
					if !walk(kind.Switch.GetDefault().GetSteps(), depth) {
						return false
					}
				case *Node_Call:
					callee := kind.Call.GetWorkflow()
					if seen, ok := shallowest[callee]; depth >= MaxCallDepth || (ok && seen <= depth+1) {
						continue
					}
					shallowest[callee] = depth + 1
					if !walk(callee.GetSteps(), depth+1) {
						return false
					}
				}
			}

			return true
		}
		walk(wf.GetSteps(), 0)
	}
}

// DeclaredIn reports whether wf, or a workflow it calls, declares a step this
// target could name: one whose id is the target's step, inside containers
// whose ids end with the target's qualifiers, each of the kind it names and a
// call of the callee it names — `bogus/last` and `each#0/touch`, for a loop
// `each`, name nothing however many steps are called `last` or `touch`. It is
// the question a truncated [DebugStaticSites] leaves open, asked of the
// program as written. A callee is walked once for each depth and each chain of
// containers the target's qualifiers can see around it, since the answer
// inside depends on nothing else: a workflow built in memory may share one
// callee among many calls, and walking it once per call would expand
// exponentially through [MaxCallDepth] levels. Only indices, which no program declares, are not compared, so it may accept
// a target [DebugTarget.Resolve] would not, never the reverse. Calls are
// followed to [MaxCallDepth], as the sites are.
func (t DebugTarget) DeclaredIn(wf *Workflow) bool { return t.declaredIn(wf, true) }

// DeclaredOutsideBodiesIn is [DebugTarget.DeclaredIn] asking only of the steps
// declared outside every loop body, parallel branch and switch arm: at wf's
// top level, or at the top level of a workflow a call reaches. Those are the
// only steps a durable run holds at, so a target it rejects is one a durable
// session can never stop at, however far past a truncated [DebugStaticSites]
// the step lies.
func (t DebugTarget) DeclaredOutsideBodiesIn(wf *Workflow) bool { return t.declaredIn(wf, false) }

// declaredIn is [DebugTarget.DeclaredIn], descending into loop bodies,
// parallel branches and switch arms only when bodies is set.
func (t DebugTarget) declaredIn(wf *Workflow, bodies bool) bool {
	if len(t.parts) == 0 {
		return false
	}
	step, qualifiers := t.parts[len(t.parts)-1].id, t.parts[:len(t.parts)-1]
	// matches reports whether the chain's last len(parts) segments are the
	// containers parts name, in order.
	matches := func(chain []*DebugSegment, parts []targetPart) bool {
		if len(chain) < len(parts) {
			return false
		}
		tail := chain[len(chain)-len(parts):]
		for i, part := range parts {
			switch {
			case tail[i].GetStepId() != part.id:
				return false
			case part.kind != DebugSegmentKind_DEBUG_SEGMENT_KIND_UNSPECIFIED && part.kind != tail[i].GetKind():
				return false
			case part.callee != "" && part.callee != tail[i].GetCallee():
				return false
			}
		}

		return true
	}
	qualified := func(chain []*DebugSegment) bool { return matches(chain, qualifiers) }

	// visited is each callee walked, with the depth it was walked at and
	// how the chain around it can still complete the qualifiers: for each j,
	// whether its last j segments are the first j qualifiers, which is all a
	// match deeper inside depends on. Keyed by that rather than by the
	// segments themselves, so a target with many qualifiers cannot make every
	// path its own key. A walk that found the target returned at once, so
	// each recorded one found nothing.
	type visit struct {
		callee *Workflow
		depth  int
		open   string
	}
	visited := map[visit]bool{}
	open := func(chain []*DebugSegment) string {
		var b strings.Builder
		for j := 1; j <= len(qualifiers) && j <= len(chain); j++ {
			if matches(chain, qualifiers[:j]) {
				fmt.Fprintf(&b, "%d,", j)
			}
		}

		return b.String()
	}

	var walk func(nodes []*Node, chain []*DebugSegment, depth int) bool
	walk = func(nodes []*Node, chain []*DebugSegment, depth int) bool {
		for _, node := range nodes {
			if node.GetId() == step && qualified(chain) {
				return true
			}
			into := func(kind DebugSegmentKind, callee string) []*DebugSegment {
				return append(slices.Clip(chain), &DebugSegment{Kind: kind, StepId: node.GetId(), Callee: callee})
			}
			if _, call := node.GetKind().(*Node_Call); !call && !bodies {
				continue
			}
			var found bool
			switch kind := node.GetKind().(type) {
			case *Node_ForEach:
				found = walk(kind.ForEach.GetBody(), into(DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, ""), depth)
			case *Node_Loop:
				found = walk(kind.Loop.GetBody(), into(DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, ""), depth)
			case *Node_Parallel:
				inner := into(DebugSegmentKind_DEBUG_SEGMENT_KIND_BRANCH, "")
				found = slices.ContainsFunc(kind.Parallel.GetBranches(), func(branch *Parallel_Branch) bool {
					return walk(branch.GetSteps(), inner, depth)
				})
			case *Node_Switch:
				inner := into(DebugSegmentKind_DEBUG_SEGMENT_KIND_CASE, "")
				found = slices.ContainsFunc(kind.Switch.GetCases(), func(arm *Switch_Case) bool {
					return walk(arm.GetSteps(), inner, depth)
				}) || walk(kind.Switch.GetDefault().GetSteps(), inner, depth)
			case *Node_Call:
				if depth >= MaxCallDepth {
					break
				}
				callee := kind.Call.GetWorkflow()
				inner := into(DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL, callee.GetName())
				key := visit{callee: callee, depth: depth + 1, open: open(inner)}
				if visited[key] {
					break
				}
				visited[key] = true
				found = walk(callee.GetSteps(), inner, depth+1)
			}
			if found {
				return true
			}
		}

		return false
	}

	return walk(wf.GetSteps(), nil, 0)
}

// Resolve returns the static sites this target can ever match, in document
// order. A qualifier that names the wrong kind of container matches nothing.
func (t DebugTarget) Resolve(sites []DebugStaticSite) []DebugStaticSite {
	var matched []DebugStaticSite
	for _, site := range sites {
		if t.matchesStatically(site) {
			matched = append(matched, site)
		}
	}

	return matched
}

// matchesStatically is [DebugTarget.Matches] with every index qualifier
// accepted, since a static chain has no indices to compare.
func (t DebugTarget) matchesStatically(site DebugStaticSite) bool {
	relaxed := DebugTarget{parts: make([]targetPart, len(t.parts))}
	for i, part := range t.parts {
		if part.kind != DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL && part.kind != DebugSegmentKind_DEBUG_SEGMENT_KIND_UNSPECIFIED {
			if i < len(t.parts)-1 {
				// The kind still has to agree; only the index is unknowable.
				offset := len(site.Chain) - (len(t.parts) - 1)
				if offset < 0 || site.Chain[offset+i].GetKind() != part.kind {
					return false
				}
			}
			part.kind = DebugSegmentKind_DEBUG_SEGMENT_KIND_UNSPECIFIED
		}
		relaxed.parts[i] = part
	}

	return relaxed.Matches(&DebugOccurrence{Site: site.Site, Segments: site.Chain})
}

// SwitchArmIndex is the index of the arm whose body is body: a case's own
// index, or the number of cases for the default arm.
func SwitchArmIndex(sw *Switch, body []*Node) int {
	if len(body) == 0 {
		return 0
	}
	for i, arm := range sw.GetCases() {
		if steps := arm.GetSteps(); len(steps) > 0 && steps[0] == body[0] {
			return i
		}
	}

	return len(sw.GetCases())
}
