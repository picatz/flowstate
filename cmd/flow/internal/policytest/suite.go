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

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/picatz/flowstate/internal/strictyaml"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
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
	// shared caller rendering.
	egress principal.Caller

	// task is read by task-shape rules.
	task *v1.WorkloadIdentity
}

// subjectOf is the one place a case's identity fields become the evaluators'
// identity values. Egress, exec and task-shape rules all read the same
// principal.Caller, rendered from the one WorkloadIdentity the case describes,
// which is the case's own [v1.Principal] and nothing a case did not write.
func subjectOf(who *v1.Principal) subject {
	task := &v1.WorkloadIdentity{Principal: who}
	return subject{egress: v1.CallerOf(task), task: task}
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
	if err := readSuite(data, &doc); err != nil {
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

		if err := checkIdentity(held.GetPrincipal()); err != nil {
			return nil, fmt.Errorf("case %q: %w", held.GetName(), err)
		}

		c := Case{
			Name:    held.GetName(),
			Expect:  held.GetExpect(),
			Rule:    held.GetRule(),
			subject: subjectOf(held.GetPrincipal()),
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

// readSuite decodes the document into the schema's message, letting a case write
// `kind: human`, the spelling a trust policy and every other surface use, where
// the schema's own enum name is PRINCIPAL_KIND_HUMAN. It reads the document once
// as a generic message to respell the kinds and then into the schema, so both
// reads keep the strictness of [strictyaml.UnmarshalProto].
func readSuite(data []byte, into *v1.PolicyTestSuite) error {
	doc := &structpb.Struct{}
	if err := strictyaml.UnmarshalProto(data, doc); err != nil {
		return err
	}

	for _, held := range doc.GetFields()["cases"].GetListValue().GetValues() {
		v1.RespellPrincipalKind(held.GetStructValue().GetFields()["principal"])
	}

	encoded, err := protojson.Marshal(doc)
	if err != nil {
		return err
	}

	return protojson.Unmarshal(encoded, into)
}

// checkIdentity refuses a principal no rule could read the way its author meant,
// by the rules a worker's own identity is held to: a subject and an issuer
// travel together, because a subject is only unique within its issuer, and
// `issuer_entry` names the trust policy entry that admitted a caller, which a
// case has none of and so could only invent.
func checkIdentity(who *v1.Principal) error {
	if (who.GetSubject() == "") != (who.GetIssuer() == "") {
		return errors.New("principal names a subject or an issuer without the other; give both, because a subject is only unique within its issuer")
	}

	if who.GetIssuerEntry() != "" {
		return errors.New("principal.issuer_entry names a trust policy entry, which a case has none of")
	}

	// A claim nested deeper or holding more values than an identity may carry is
	// dropped when the identity is read, which would let a case pass as a caller
	// it did not describe. The bounds are the ones every mint is held to.
	if err := v1.AuthIdentity(&v1.WorkloadIdentity{Principal: who}).ClaimsError(); err != nil {
		return fmt.Errorf("principal.claims: %w", err)
	}

	return nil
}

// bind checks the request carries what the surface needs and nothing of another
// surface's, and prepares the egress request.
func (c *Case) bind(surface string) error {
	req := c.req

	fields := map[string][]string{
		SurfaceEgress: {"url", "method", "ip", "credentials"},
		SurfaceTask:   {"task"},
		SurfaceExec:   {"argv", "dir", "env"},
	}

	given := map[string]bool{
		"url":         req.GetUrl() != "",
		"method":      req.GetMethod() != "",
		"ip":          req.GetIp() != "",
		"credentials": req.GetCredentials(),
		"task":        req.GetTask() != "",
		"argv":        len(req.GetArgv()) > 0,
		"dir":         req.GetDir() != "",
		"env":         len(req.GetEnv()) > 0,
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

		// A worker judges an IP-literal host as the address it dials, whatever
		// a resolver would have said, so a different `ip` would certify a
		// destination the worker refuses.
		if literal, lerr := netip.ParseAddr(u.Hostname()); lerr == nil && literal.Unmap() != ip.Unmap() {
			return errors.New("request.ip differs from the IP-literal host in request.url; a worker dials the literal, so drop request.ip or make them agree")
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
