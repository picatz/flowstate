package flowfile

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml/ast"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Waiting, as an author writes it.
//
// Three spellings rather than one `wait:` block with a nested choice, because each
// of them is a different thing to say and the shortest way to say it should be the
// way it reads:
//
//	- id: settle
//	  sleep: 30s
//
//	- id: window
//	  wait_until: ${maintenance.opens_at}
//
//	- id: approval
//	  wait_for_signal: deploy-approved
//
//	- id: approval
//	  wait_for_signal:
//	    name: deploy-approved
//	    timeout: 24h
//
// A signal takes either form: the scalar for the common case, and the mapping when
// there is more to say. That is worth the extra branch in the parser, because
// `wait_for_signal: deploy-approved` is what someone writes when they are learning
// the DSL and having to write a two-line mapping to say one thing is the kind of
// friction that makes a feature feel heavier than it is.

// signalKeys are the keys of the mapping form of wait_for_signal.
var signalKeys = []string{"name", "timeout"}

// parseDuration reads a duration the way the DSL writes one: Go's syntax, plus
// days.
//
// Go's time.ParseDuration stops at hours, which was fine while the longest thing
// anyone wrote was an activity timeout. It is not fine now that the headline
// capability is waiting a week for someone to approve a deploy: `168h` is the same
// duration as `7d` and communicates none of it, and an author who writes `7d` and
// is told it is not a duration will conclude the feature does not do what it says.
//
// Days are the last unit worth adding. Weeks are ambiguous enough in scheduling
// that spelling them `7d` is clearer, and months are not a duration at all.
func parseDuration(s string) (time.Duration, error) {
	converted, err := expandDays(s)
	if err != nil {
		return 0, err
	}
	return time.ParseDuration(converted)
}

// expandDays rewrites day components as hours, so that Go's parser can read the
// rest.
//
// No unit Go accepts contains the letter d, so a d in a duration is either this
// unit or a typo — which means rewriting it cannot corrupt an otherwise valid
// duration, and anything left over still fails to parse and still gets a
// diagnostic.
func expandDays(s string) (string, error) {
	var out strings.Builder

	for i := 0; i < len(s); {
		if s[i] != 'd' {
			out.WriteByte(s[i])
			i++
			continue
		}

		// Walk back over the number this d belongs to.
		written := out.String()
		start := len(written)
		for start > 0 && (isDigit(written[start-1]) || written[start-1] == '.') {
			start--
		}
		if start == len(written) {
			return "", fmt.Errorf("%q has a d with no number before it", s)
		}

		days, err := strconv.ParseFloat(written[start:], 64)
		if err != nil {
			return "", fmt.Errorf("%q is not a number of days", written[start:])
		}

		out.Reset()
		out.WriteString(written[:start])
		// Written as hours rather than as a scaled duration so that a fractional
		// day keeps its precision through Go's own parser.
		fmt.Fprintf(&out, "%gh", days*24)
		i++
	}

	return out.String(), nil
}

// isDigit reports whether b is an ASCII digit.
func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// StepKinds returns the keys that spell the kinds of work a step can be, in the
// order a diagnostic lists them.
//
// Exported because this package is not the only surface that has to describe the
// DSL. The language server carries its own table of the document shape — the shape
// lives in unexported structs here, so it has no choice — and that table told
// authors a step was "one of task, for_each, and parallel" for as long as waiting
// had existed. Three shipped kinds, reachable from a Flowfile and exercised by
// examples in CI, that the editor said were not there.
//
// A copy is returned because the caller must not be able to edit the DSL by
// editing a slice header.
func StepKinds() []string {
	return stepKindKeys()
}

// StepKindList renders the kinds a step may be as prose: "a, b, or c".
//
// Built from [stepKindKeys] rather than written out, so that adding a kind cannot
// leave a diagnostic — or an editor — describing the DSL as it used to be.
func StepKindList() string {
	kinds := stepKindKeys()
	if len(kinds) < 2 {
		return strings.Join(kinds, "")
	}
	return strings.Join(kinds[:len(kinds)-1], ", ") + ", or " + kinds[len(kinds)-1]
}

// stepKindList is the internal spelling, kept so the diagnostics that call it read
// unchanged.
func stepKindList() string { return StepKindList() }

// sleep compiles `sleep: 30s` into a durable timer.
func (c *compiler) sleep(n ast.Node, path string, r ref) *v1.Wait {
	duration, ok := c.duration(n, path, ref{step: r.step, path: path, label: "sleep"})
	if !ok {
		return nil
	}

	return &v1.Wait{Kind: &v1.Wait_Duration{Duration: duration}}
}

// waitUntil compiles `wait_until: <expression>` into a timer to a moment.
func (c *compiler) waitUntil(n ast.Node, path string, r ref) *v1.Wait {
	until := c.exprValue(n, path, ref{step: r.step, path: path, label: "wait_until"})
	if until == nil {
		return nil
	}

	return &v1.Wait{Kind: &v1.Wait_Until{Until: until}}
}

// waitForSignal compiles `wait_for_signal`, in either the scalar or the mapping
// form.
func (c *compiler) waitForSignal(n ast.Node, path string, r ref) *v1.Wait {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	// The scalar form: the signal's name and nothing else.
	if _, isMapping := n.(*ast.MappingNode); !isMapping {
		if _, isValue := n.(*ast.MappingValueNode); !isValue {
			name, ok := c.text(n, path, ref{step: r.step, path: path, label: "wait_for_signal"})
			if !ok {
				return nil
			}
			return c.signalWait(n, name, r, path)
		}
	}

	fields, ok := c.fields(n, path, r, signalKeys)
	if !ok {
		return nil
	}

	var name string
	if f, found := fields.get("name"); found {
		name, _ = c.text(f.value, fieldPath(path, "name"),
			ref{step: r.step, path: fieldPath(path, "name"), label: "signal name"})
	} else {
		c.report(spanOfNode(n), ref{step: r.step, path: path, label: "wait_for_signal"},
			"needs a name, which is what a sender addresses; write `wait_for_signal: deploy-approved`, or give the mapping a `name:`")
		return nil
	}

	wait := c.signalWait(n, name, r, path)
	if wait == nil {
		return nil
	}

	if f, found := fields.get("timeout"); found {
		timeout, ok := c.duration(f.value, fieldPath(path, "timeout"),
			ref{step: r.step, path: fieldPath(path, "timeout"), label: "wait_for_signal timeout"})
		if !ok {
			return nil
		}
		wait.Timeout = timeout
	}

	return wait
}

// signalWait builds the wait, reporting a name the schema will not accept.
//
// Checked here rather than left to schema validation, because a diagnostic that
// names the line is worth more than one that names a field path — and because the
// name is part of the workload's contract with whoever approves it, so a typo in it
// is a workload that waits for something nobody will ever send.
func (c *compiler) signalWait(n ast.Node, name string, r ref, path string) *v1.Wait {
	signalRef := ref{step: r.step, path: path, label: "wait_for_signal"}

	switch {
	case name == "":
		c.report(spanOfNode(n), signalRef, "needs a signal name; it is what a sender addresses")
		return nil
	case len(name) > 128:
		c.report(spanOfNode(n), signalRef, "signal name is longer than 128 characters")
		return nil
	case !validSignalName(name):
		c.report(spanOfNode(n), signalRef,
			"signal name %q may only contain letters, digits, dashes, and underscores, and must start with a letter or digit", name)
		return nil
	}

	return &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: name}}}
}

// validSignalName reports whether a name matches what the schema permits.
func validSignalName(name string) bool {
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case (c == '-' || c == '_') && i > 0:
		default:
			return false
		}
	}
	return true
}

// checkWaitPolicy reports per-step policy that does nothing on a wait.
//
// A wait schedules no activity, so the two policy keys that bound and retry one
// have nothing to act on: a `timeout:` beside a `sleep:` is not a shorter sleep,
// and `retry:` cannot re-run a timer. Silently ignoring them would leave an author
// believing they had bounded something. A wait's own bound is
// `wait_for_signal.timeout`, and the diagnostic says so.
func (c *compiler) checkWaitPolicy(step *v1.Node, fields *fieldSet, path string, r ref) {
	if _, isWait := step.GetKind().(*v1.Node_Wait); !isWait {
		return
	}

	for _, name := range []string{"timeout", "retry"} {
		f, found := fields.get(name)
		if !found {
			continue
		}

		advice := "a wait is bounded by `wait_for_signal:`'s own `timeout:`, or by the duration of a `sleep:`"
		if name == "retry" {
			advice = "there is no activity to attempt again; a wait either happens or times out"
		}

		c.report(spanOfNode(f.key), ref{step: r.step, path: fieldPath(path, name), label: name},
			"does nothing on a waiting step: %s", advice)
	}
}
