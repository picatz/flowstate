// Package policytest puts cases to a deployment policy and reports which rule
// denied each one that was denied, for `flow policy test`.
//
// # Why this exists
//
// A policy is the one artifact in the repository that had no test verb: `flow
// test` runs workflows, `flow validate` checks them, `flow signals check`
// rehearses a workflow's own signal predicates, and nothing ran a deployment's
// egress, task-shape or exec policy against cases. So the rule most worth
// asserting - that team-b cannot reach team-a's partner API - was checkable only
// by reading the policy carefully (#548).
//
// # No second evaluator
//
// Each case is decided by the function the engine enforces that policy with:
// [netpolicy.Policy.CheckURL] and [netpolicy.Policy.CheckAddr] for egress,
// [v1.TaskPolicy.Check] for task shape, [execpolicy.Policy.Check] for exec. This
// package decodes a file, asks, and renders the answer; it holds no rule.
//
// # Fail closed
//
// An evaluator that returns an error it does not type as a refusal is a denial,
// as it is when the engine meets it. The one exception is a decision that never
// finished (the context ended): that is reported as the case failing to be
// decided, not as a denial the policy never made.
package policytest

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/picatz/flowstate/internal/strictyaml"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// MaxSuiteBytes bounds a suite file, before it is parsed (invariant 5). The case
// count and every field are bounded again by the schema ([v1.PolicyTestSuite]),
// which is where those numbers are written.
const MaxSuiteBytes = 256 << 10

// MaxCases mirrors the schema's limit on cases, so a caller and a test can name
// it. A test holds it to the schema's own boundary.
const MaxCases = 512

// The surfaces a suite can name.
const (
	SurfaceEgress = "egress"
	SurfaceTask   = "task"
	SurfaceExec   = "exec"
)

// Outcomes a case expects, and a decision reports.
const (
	Allow = "allow"
	Deny  = "deny"
)

// Suite is a decoded, validated test file.
type Suite struct {
	Surface string
	Cases   []Case
}

// Case is one request put to the policy.
type Case struct {
	Name   string
	Expect string

	// Rule is the rule a denial must name; empty asserts nothing about which.
	Rule string

	subject subject
	req     *v1.PolicyTestRequest

	// url and addr are the egress request as the evaluator takes it. addr is
	// invalid when no address check applies.
	url  *url.URL
	addr netip.AddrPort
}

// subject is a case's identity as each evaluator takes it.
type subject struct {
	// egress is read by egress rules and by exec rules, which both take the
	// netpolicy rendering.
	egress netpolicy.Identity

	// task is read by task-shape rules.
	task *v1.WorkloadIdentity
}

// subjectOf is the one place a case's identity fields become the evaluators'
// identity types. The four policy surfaces each read `identity.<field>` through a
// type of their own; mapping the case file onto them here, and nowhere else,
// keeps the day those types become one a change to this function alone.
func subjectOf(id *v1.PolicyTestIdentity) subject {
	return subject{
		egress: netpolicy.Identity{
			Subject:   id.GetSubject(),
			Issuer:    id.GetIssuer(),
			Namespace: id.GetNamespace(),
			Claims:    id.GetClaims(),
		},
		task: &v1.WorkloadIdentity{
			Subject:   id.GetSubject(),
			Issuer:    id.GetIssuer(),
			Namespace: id.GetNamespace(),
			Claims:    id.GetClaims(),
		},
	}
}

// ParseSuite reads and validates a suite document.
//
// Everything that can be refused without a policy is refused here: size, the YAML
// constructs that expand or are silently ignored, an unknown or misspelled key,
// the schema's rules, duplicate names, a request that carries the wrong
// surface's fields or lacks the one it needs, and a `rule:` on a case that does
// not expect a denial. A misspelled `expect:` would otherwise assert nothing
// while reading as if it asserted something.
func ParseSuite(data []byte) (*Suite, error) {
	if len(data) > MaxSuiteBytes {
		return nil, fmt.Errorf("the suite is %d bytes, over the %d byte limit", len(data), MaxSuiteBytes)
	}

	if err := refuseUnsafeYAML(data); err != nil {
		return nil, err
	}

	var doc v1.PolicyTestSuite
	if err := strictyaml.UnmarshalProto(data, &doc); err != nil {
		return nil, fmt.Errorf("the suite is not a document of `surface:` and `cases:`: %w", err)
	}

	if err := v1.Validate(&doc); err != nil {
		return nil, validationError(err)
	}

	suite := &Suite{Surface: doc.GetSurface(), Cases: make([]Case, 0, len(doc.GetCases()))}
	seen := make(map[string]struct{}, len(doc.GetCases()))

	for i, held := range doc.GetCases() {
		if _, dup := seen[held.GetName()]; dup {
			return nil, fmt.Errorf("case %d: the name %q is used twice; a name identifies the case in the output", i+1, held.GetName())
		}
		seen[held.GetName()] = struct{}{}

		c := Case{
			Name:    held.GetName(),
			Expect:  held.GetExpect(),
			Rule:    held.GetRule(),
			subject: subjectOf(held.GetIdentity()),
			req:     held.GetRequest(),
		}

		if c.Rule != "" && c.Expect != Deny {
			return nil, fmt.Errorf("case %q: rule: says which rule denies, so it needs expect: deny", c.Name)
		}

		if err := c.bind(suite.Surface); err != nil {
			return nil, fmt.Errorf("case %q: %w", c.Name, err)
		}

		suite.Cases = append(suite.Cases, c)
	}

	return suite, nil
}

// bind checks the request carries what the surface needs and nothing of another
// surface's, and prepares the egress request.
func (c *Case) bind(surface string) error {
	req := c.req

	fields := map[string][]string{
		SurfaceEgress: {"url", "method", "ip"},
		SurfaceTask:   {"task"},
		SurfaceExec:   {"argv", "dir", "env"},
	}

	given := map[string]bool{
		"url":    req.GetUrl() != "",
		"method": req.GetMethod() != "",
		"ip":     req.GetIp() != "",
		"task":   req.GetTask() != "",
		"argv":   len(req.GetArgv()) > 0,
		"dir":    req.GetDir() != "",
		"env":    len(req.GetEnv()) > 0,
	}

	for name, present := range given {
		if present && !slices.Contains(fields[surface], name) {
			return fmt.Errorf("request.%s belongs to another surface; a %s case takes %s",
				name, surface, strings.Join(fields[surface], ", "))
		}
	}

	switch surface {
	case SurfaceEgress:
		return c.bindEgress()
	case SurfaceTask:
		if req.GetTask() == "" {
			return errors.New("request.task is required: the qualified task name dispatched")
		}
	case SurfaceExec:
		if len(req.GetArgv()) == 0 || req.GetArgv()[0] == "" {
			return errors.New("request.argv is required: the program name, then its arguments")
		}
	}

	return nil
}

func (c *Case) bindEgress() error {
	req := c.req

	if req.GetUrl() == "" {
		return errors.New("request.url is required")
	}

	u, err := url.Parse(req.GetUrl())
	if err != nil {
		return fmt.Errorf("request.url is not a URL: %w", err)
	}
	c.url = u

	var ip netip.Addr

	switch {
	case req.GetIp() != "":
		ip, err = netip.ParseAddr(req.GetIp())
		if err != nil {
			return errors.New("request.ip is not an IP address")
		}
	default:
		// A host that is an address is judged as one. Any other host is not
		// address-checked: that takes DNS, which this verb does not do.
		ip, _ = netip.ParseAddr(u.Hostname())
	}

	if !ip.IsValid() {
		return nil
	}

	port, ok := portOf(u)
	if !ok {
		return errors.New("an address is checked with the request's port, and request.url names none and its scheme has no default; write the port in the URL")
	}

	c.addr = netip.AddrPortFrom(ip, port)

	return nil
}

// portOf is the port a request to u dials: the one it names, else its scheme's.
func portOf(u *url.URL) (uint16, bool) {
	if p := u.Port(); p != "" {
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil || n == 0 {
			return 0, false
		}
		return uint16(n), true
	}

	switch strings.ToLower(u.Scheme) {
	case "https":
		return 443, true
	case "http":
		return 80, true
	}

	return 0, false
}

// refuseUnsafeYAML refuses what a suite has no use for and a decoder would
// mishandle: a second document (silently ignored), and anchors, aliases and
// merge keys (how a few hundred bytes expand into gigabytes), by presence and
// before anything is decoded.
func refuseUnsafeYAML(data []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("the suite could not be read as YAML")
		}
	}()

	file, parseErr := strictyaml.ParseBytes(data, 0)
	if parseErr != nil {
		return fmt.Errorf("the suite is not YAML: %w", parseErr)
	}

	documents := 0
	for _, doc := range file.Docs {
		if doc.Body != nil {
			documents++
		}
	}
	if documents > 1 {
		return errors.New("a suite is one document; a second, after `---`, would be silently ignored, so put every case under one `cases:`")
	}

	if found := flowfile.StrictYAMLRefusals(file); len(found) > 0 {
		return fmt.Errorf("line %d, column %d: a suite is a plain table; anchors (&), aliases (*) and merge keys (<<) "+
			"are not accepted, so write each value out", found[0].Line, found[0].Column)
	}

	return nil
}

// validationError renders the schema's refusals as field and message.
func validationError(err error) error {
	invalid, ok := errors.AsType[*v1.ValidationError](err)
	if !ok {
		return errors.New("the suite does not satisfy its schema")
	}

	lines := make([]string, 0, 5)
	for _, violation := range invalid.Violations[:min(len(invalid.Violations), 5)] {
		field := violation.Field
		if field == "" {
			field = "the suite"
		}
		lines = append(lines, fmt.Sprintf("%s: %s", field, violation.Message))
	}

	return fmt.Errorf("the suite does not satisfy its schema: %s", strings.Join(lines, "; "))
}
