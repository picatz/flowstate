package graph

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Bounds on a reference, matching the schema's own, so a reference that parses
// is one the schema accepts.
const (
	// MaxRefNameRunes is the longest workflow name, workflow id or run id.
	MaxRefNameRunes = 256

	// MaxRefStepRunes is the longest step address, the schema's bound on a
	// debug address.
	MaxRefStepRunes = v1.MaxDebugAddressRunes

	// MaxRefBytes is the longest text a parser reads. Every id is escaped at
	// most threefold, so this is above the longest valid spelling and below
	// anything worth allocating for.
	MaxRefBytes = 3 * (3*MaxRefNameRunes + 4*MaxRefStepRunes + 64)
)

// uriScheme is the scheme of the canonical spelling. It extends the
// `flowstate://docs/...` and `flowstate://catalog/...` resources MCP already
// serves.
const uriScheme = "flowstate://"

// Level is how far in a [v1.GraphRef] points.
type Level int

// The levels, from the widest view to the narrowest.
const (
	LevelFleet Level = iota
	LevelWorkflow
	LevelRun
	LevelStep
	LevelAttempt
)

func (l Level) String() string {
	switch l {
	case LevelFleet:
		return "fleet"
	case LevelWorkflow:
		return "workflow"
	case LevelRun:
		return "run"
	case LevelStep:
		return "step"
	case LevelAttempt:
		return "attempt"
	default:
		return "level(" + strconv.Itoa(int(l)) + ")"
	}
}

// RefLevel validates ref and returns its level. A reference that mixes
// addresses (a workflow name beside a workflow id), skips a part (a step
// without a run id) or exceeds a bound is refused, with the field named.
func RefLevel(ref *v1.GraphRef) (Level, error) {
	if ref == nil {
		return LevelFleet, nil
	}
	for _, f := range []struct {
		name  string
		value string
		max   int
	}{
		{"workflow_name", ref.GetWorkflowName(), MaxRefNameRunes},
		{"workflow_id", ref.GetWorkflowId(), MaxRefNameRunes},
		{"run_id", ref.GetRunId(), MaxRefNameRunes},
		{"step", ref.GetStep(), MaxRefStepRunes},
	} {
		if !utf8.ValidString(f.value) {
			return 0, fmt.Errorf("%s is not valid UTF-8", f.name)
		}
		if n := utf8.RuneCountInString(f.value); n > f.max {
			return 0, fmt.Errorf("%s is %d characters, over the limit of %d", f.name, n, f.max)
		}
	}

	var (
		named   = ref.GetWorkflowName() != ""
		hasID   = ref.GetWorkflowId() != ""
		hasRun  = ref.GetRunId() != ""
		hasStep = ref.GetStep() != ""
		hasTry  = ref.Attempt != nil
	)
	switch {
	case named && (hasID || hasRun || hasStep || hasTry):
		return 0, errors.New("workflow_name addresses a definition and cannot be combined with workflow_id, run_id, step or attempt")
	case named:
		return LevelWorkflow, nil
	case !hasID && (hasRun || hasStep || hasTry):
		return 0, errors.New("run_id, step and attempt need a workflow_id")
	case !hasID:
		return LevelFleet, nil
	case hasStep && !hasRun:
		return 0, errors.New("step needs a run_id")
	case hasTry && !hasStep:
		return 0, errors.New("attempt needs a step")
	case hasTry:
		return LevelAttempt, nil
	case hasStep:
		return LevelStep, nil
	default:
		return LevelRun, nil
	}
}

// FormatURI writes ref in its canonical spelling:
//
//	flowstate://fleet
//	flowstate://workflow/<name>
//	flowstate://run/<workflow_id>[/<run_id>]
//	flowstate://run/<workflow_id>/<run_id>/step/<address>[/attempt/<n>]
//
// Each segment is percent-encoded, so an id holding `/` or `%` is still one
// segment. [ParseURI] is its inverse.
func FormatURI(ref *v1.GraphRef) (string, error) {
	level, err := RefLevel(ref)
	if err != nil {
		return "", err
	}
	esc := url.PathEscape
	switch level {
	case LevelFleet:
		return uriScheme + "fleet", nil
	case LevelWorkflow:
		return uriScheme + "workflow/" + esc(ref.GetWorkflowName()), nil
	case LevelRun:
		out := uriScheme + "run/" + esc(ref.GetWorkflowId())
		if ref.GetRunId() != "" {
			out += "/" + esc(ref.GetRunId())
		}
		return out, nil
	default:
		out := uriScheme + "run/" + esc(ref.GetWorkflowId()) + "/" + esc(ref.GetRunId()) + "/step/" + esc(ref.GetStep())
		if level == LevelAttempt {
			out += "/attempt/" + strconv.FormatUint(uint64(ref.GetAttempt()), 10)
		}
		return out, nil
	}
}

// ParseURI reads the spelling [FormatURI] writes. The reference it returns has
// passed [RefLevel], and a malformed one is refused with the segment named.
func ParseURI(s string) (*v1.GraphRef, error) {
	if len(s) > MaxRefBytes {
		return nil, fmt.Errorf("reference is %d bytes, over the limit of %d", len(s), MaxRefBytes)
	}
	rest, ok := strings.CutPrefix(s, uriScheme)
	if !ok {
		return nil, fmt.Errorf("reference %q does not start with %s", clip(s), uriScheme)
	}
	parts := strings.Split(rest, "/")

	segment := func(i int, what string) (string, error) {
		if i >= len(parts) || parts[i] == "" {
			return "", fmt.Errorf("reference is missing the %s", what)
		}
		v, err := url.PathUnescape(parts[i])
		if err != nil {
			return "", fmt.Errorf("the %s is not valid percent-encoding: %w", what, err)
		}
		return v, nil
	}
	extra := func(n int) error {
		if len(parts) > n {
			return fmt.Errorf("reference has %d unexpected trailing segment(s)", len(parts)-n)
		}
		return nil
	}

	ref := &v1.GraphRef{}
	switch parts[0] {
	case "fleet":
		if err := extra(1); err != nil {
			return nil, err
		}
	case "workflow":
		name, err := segment(1, "workflow name")
		if err != nil {
			return nil, err
		}
		if err := extra(2); err != nil {
			return nil, err
		}
		ref.WorkflowName = name
	case "run":
		id, err := segment(1, "workflow id")
		if err != nil {
			return nil, err
		}
		ref.WorkflowId = id
		if len(parts) == 2 {
			break
		}
		if ref.RunId, err = segment(2, "run id"); err != nil {
			return nil, err
		}
		if len(parts) == 3 {
			break
		}
		if parts[3] != "step" {
			return nil, fmt.Errorf("expected step after the run id, got %q", clip(parts[3]))
		}
		if ref.Step, err = segment(4, "step address"); err != nil {
			return nil, err
		}
		if len(parts) == 5 {
			break
		}
		if parts[5] != "attempt" {
			return nil, fmt.Errorf("expected attempt after the step address, got %q", clip(parts[5]))
		}
		n, err := segment(6, "attempt number")
		if err != nil {
			return nil, err
		}
		if ref.Attempt, err = parseAttempt(n); err != nil {
			return nil, err
		}
		if err := extra(7); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown reference kind %q (want fleet, workflow or run)", clip(parts[0]))
	}
	if _, err := RefLevel(ref); err != nil {
		return nil, err
	}
	return ref, nil
}

// shorthandEscape escapes the three characters the shorthand uses as
// separators, and the escape character.
var shorthandEscape = strings.NewReplacer("%", "%25", "@", "%40", ":", "%3A", "!", "%21")

// FormatShorthand writes a run, step or attempt reference as it is typed on a
// command line:
//
//	<workflow_id>[@<run_id>][:<address>[!<attempt>]]
//
// `%`, `@`, `:` and `!` inside an id or address are written as `%25`, `%40`,
// `%3A` and `%21`. The fleet and a workflow definition have no shorthand; use
// [FormatURI].
func FormatShorthand(ref *v1.GraphRef) (string, error) {
	level, err := RefLevel(ref)
	if err != nil {
		return "", err
	}
	if level < LevelRun {
		return "", fmt.Errorf("a %s reference has no shorthand; use the flowstate:// form", level)
	}
	out := shorthandEscape.Replace(ref.GetWorkflowId())
	if ref.GetRunId() != "" {
		out += "@" + shorthandEscape.Replace(ref.GetRunId())
	}
	if ref.GetStep() != "" {
		out += ":" + shorthandEscape.Replace(ref.GetStep())
	}
	if level == LevelAttempt {
		out += "!" + strconv.FormatUint(uint64(ref.GetAttempt()), 10)
	}
	return out, nil
}

// ParseShorthand reads the spelling [FormatShorthand] writes.
func ParseShorthand(s string) (*v1.GraphRef, error) {
	if len(s) > MaxRefBytes {
		return nil, fmt.Errorf("reference is %d bytes, over the limit of %d", len(s), MaxRefBytes)
	}
	head, attempt, hasAttempt := strings.Cut(s, "!")
	head, step, hasStep := strings.Cut(head, ":")
	id, run, hasRun := strings.Cut(head, "@")

	if strings.ContainsAny(run, "@") || strings.ContainsAny(step, "@") {
		return nil, errors.New("a literal @ inside an id must be written %40")
	}
	if strings.Contains(attempt, ":") || strings.Contains(attempt, "@") || strings.Contains(attempt, "!") {
		return nil, errors.New("the attempt must be a number after a single !")
	}

	unescape := func(v, what string) (string, error) {
		out, err := url.PathUnescape(v)
		if err != nil {
			return "", fmt.Errorf("the %s is not valid percent-encoding: %w", what, err)
		}
		return out, nil
	}
	ref := &v1.GraphRef{}
	var err error
	if ref.WorkflowId, err = unescape(id, "workflow id"); err != nil {
		return nil, err
	}
	if hasRun {
		if run == "" {
			return nil, errors.New("the run id after @ is empty")
		}
		if ref.RunId, err = unescape(run, "run id"); err != nil {
			return nil, err
		}
	}
	if hasStep {
		if step == "" {
			return nil, errors.New("the step address after : is empty")
		}
		if ref.Step, err = unescape(step, "step address"); err != nil {
			return nil, err
		}
	}
	if hasAttempt {
		if ref.Attempt, err = parseAttempt(attempt); err != nil {
			return nil, err
		}
	}
	if ref.WorkflowId == "" {
		return nil, errors.New("the workflow id is empty")
	}
	if _, err := RefLevel(ref); err != nil {
		return nil, err
	}
	return ref, nil
}

func parseAttempt(s string) (*uint32, error) {
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("attempt %q is not a number", clip(s))
	}
	v := uint32(n)
	return &v, nil
}

// clip keeps untrusted text quoted in an error short.
func clip(s string) string {
	const limit = 64
	if len(s) <= limit {
		return s
	}
	return strings.ToValidUTF8(s[:limit], "") + "…"
}
