package main

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policytest"
)

const tenantEgressPolicy = `egress:
  schemes: [https]
  allow:
    - identity.namespace == "team-a" && host == "partner-a.example.com"
    - host == "api.github.com"
  deny:
    - method != "GET"
`

func policyTestJSON(t *testing.T, args ...string) (flowResult, policytest.Report) {
	t.Helper()

	res := runFlow(t, append([]string{"policy", "test"}, append(args, "-o", "json")...)...)

	var report policytest.Report
	if res.Stdout != "" {
		require.NoError(t, json.Unmarshal([]byte(res.Stdout), &report), res.Output())
	}

	return res, report
}

func TestPolicyTestEgressReportsWhichRuleDenied(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "egress.yaml", tenantEgressPolicy)
	cases := writeFile(t, "cases.yaml", `surface: egress
cases:
  - name: team-a reaches its partner
    principal: {namespace: team-a}
    request: {url: "https://partner-a.example.com/v1"}
    expect: allow
  - name: team-b does not
    principal: {namespace: team-b}
    request: {url: "https://partner-a.example.com/v1"}
    expect: deny
    rule: allow rules
  - name: nobody does not
    request: {url: "https://partner-a.example.com/v1"}
    expect: deny
  - name: a write is refused by the deny rule
    principal: {namespace: team-a}
    request: {url: "https://partner-a.example.com/v1", method: POST}
    expect: deny
    rule: 'method != "GET"'
  - name: a lowercase method is judged as written, as the worker does
    principal: {namespace: team-a}
    request: {url: "https://partner-a.example.com/v1", method: get}
    expect: deny
    rule: 'method != "GET"'
  - name: a resolved loopback address is refused
    request: {url: "https://api.github.com/", ip: "127.0.0.1"}
    expect: deny
    rule: address
`)

	res, report := policyTestJSON(t, policy, cases)
	require.Equal(t, 0, res.ExitCode, res.Output())
	require.True(t, report.Matches)
	require.Equal(t, 6, report.Total)
	require.Equal(t, 5, report.Denials)
	require.Empty(t, report.Warnings)

	byName := map[string]policytest.Result{}
	for _, r := range report.Cases {
		byName[r.Name] = r
	}

	require.Equal(t, "deny rule", byName["a write is refused by the deny rule"].Reason)
	require.Equal(t, `method != "GET"`, byName["a write is refused by the deny rule"].Rule)
	require.Equal(t, "allow rules", byName["team-b does not"].Rule)
	require.Equal(t, "address", byName["a resolved loopback address is refused"].Rule)

	text := runFlow(t, "policy", "test", policy, cases)
	require.Equal(t, 0, text.ExitCode, text.Output())
	require.Contains(t, text.Stdout, "ok    team-b does not  (deny)")
	require.Contains(t, text.Stdout, `denied: deny rule: method != "GET"`)
	require.Contains(t, text.Stdout, "6 cases (egress policy): 6 passed, 0 failed")
}

// The negative direction the verb exists for: a policy that wrongly allows
// fails the case that says it must deny.
func TestPolicyTestAPolicyThatWronglyAllowsFailsItsDenyCase(t *testing.T) {
	t.Parallel()

	// Anyone may reach the partner: the tenant rule has been loosened.
	policy := writeFile(t, "egress.yaml", `egress:
  schemes: [https]
  allow:
    - host == "partner-a.example.com"
`)
	cases := writeFile(t, "cases.yaml", `surface: egress
cases:
  - name: team-b is refused team-a's partner
    principal: {namespace: team-b}
    request: {url: "https://partner-a.example.com/v1"}
    expect: deny
`)

	res, report := policyTestJSON(t, policy, cases)
	require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	require.False(t, report.Matches)
	require.Equal(t, 1, report.Failed)
	require.Equal(t, "allow", report.Cases[0].Got)
	require.Equal(t, "expected deny, got allow", report.Cases[0].Failure)

	text := runFlow(t, "policy", "test", policy, cases)
	require.Equal(t, exitCodeFailure, text.ExitCode)
	require.Contains(t, text.Stdout, "FAIL  team-b is refused team-a's partner")
	require.Contains(t, text.Stdout, "expected deny, got allow")
}

func TestPolicyTestADenialForTheWrongRuleFails(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "egress.yaml", tenantEgressPolicy)
	cases := writeFile(t, "cases.yaml", `surface: egress
cases:
  - name: refused, but not by the rule the case names
    principal: {namespace: team-b}
    request: {url: "https://partner-a.example.com/v1"}
    expect: deny
    rule: 'method != "GET"'
`)

	res, report := policyTestJSON(t, policy, cases)
	require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	require.Equal(t, "deny", report.Cases[0].Got)
	require.Contains(t, report.Cases[0].Failure, `got "allow rules"`)
}

func TestPolicyTestARuleThatCannotBeEvaluatedDenies(t *testing.T) {
	t.Parallel()

	// identity.claims has no "team" key for this caller, so the rule errors.
	policy := writeFile(t, "task.yaml", `allow:
  - identity.claims["team"] == "release"
`)
	cases := writeFile(t, "cases.yaml", `surface: task
cases:
  - name: a caller with no team claim is refused, by the error
    request: {task: http}
    expect: deny
    rule: rule error
  - name: a caller with the claim is admitted
    principal: {claims: {team: release}}
    request: {task: http}
    expect: allow
`)

	res, report := policyTestJSON(t, policy, cases)
	require.Equal(t, 0, res.ExitCode, res.Output())
	require.Equal(t, "rule error", report.Cases[0].Reason)
}

func TestPolicyTestTaskShape(t *testing.T) {
	t.Parallel()

	res, report := policyTestJSON(t,
		"../../examples/task-shape-policy/task-policy.yaml", "../../examples/policy-test/task-cases.yaml")
	require.Equal(t, 0, res.ExitCode, res.Output())
	require.Equal(t, "task", report.Surface)
	require.Equal(t, 4, report.Total)
}

func TestPolicyTestEgressExample(t *testing.T) {
	t.Parallel()

	res, report := policyTestJSON(t,
		"../../examples/egress-policy.yaml", "../../examples/policy-test/egress-cases.yaml")
	require.Equal(t, 0, res.ExitCode, res.Output())
	require.Equal(t, 9, report.Total)
}

func TestPolicyTestExec(t *testing.T) {
	t.Parallel()

	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh is not installed")
	}

	sh, err = filepath.EvalSymlinks(sh)
	require.NoError(t, err)

	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	policy := writeFile(t, "exec.yaml", "exec:\n  executables: {sh: "+sh+"}\n  roots: ["+root+"]\n"+
		"  timeout: 30s\n  max_output_bytes: 64KiB\n  deny:\n    - identity.namespace == \"blocked\"\n")
	cases := writeFile(t, "cases.yaml", `surface: exec
cases:
  - name: a listed program runs in a root
    request: {argv: [sh, -c, "true"], dir: `+root+`}
    expect: allow
  - name: an unlisted program is refused
    request: {argv: [python3, -c, "1"], dir: `+root+`}
    expect: deny
    rule: executable
  - name: a directory outside every root is refused
    request: {argv: [sh], dir: /}
    expect: deny
    rule: dir
  - name: a blocked tenant is refused by the rule
    principal: {namespace: blocked}
    request: {argv: [sh], dir: `+root+`}
    expect: deny
    rule: 'identity.namespace == "blocked"'
`)

	res, report := policyTestJSON(t, policy, cases)
	require.Equal(t, 0, res.ExitCode, res.Output())
	require.Equal(t, 4, report.Total)
}

func TestPolicyTestWarnsWhenNoCaseExpectsDeny(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "egress.yaml", tenantEgressPolicy)
	cases := writeFile(t, "cases.yaml", `surface: egress
cases:
  - name: only ever the happy path
    request: {url: "https://api.github.com/"}
    expect: allow
`)

	res, report := policyTestJSON(t, policy, cases)
	require.Equal(t, 0, res.ExitCode, res.Output())
	require.Len(t, report.Warnings, 1)

	text := runFlow(t, "policy", "test", policy, cases)
	require.Contains(t, text.Stdout, "warning: no case expects deny")
}

func TestPolicyTestRefusesWhatCannotBeTrusted(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "egress.yaml", tenantEgressPolicy)

	for name, c := range map[string]struct{ cases, want string }{
		"a misspelled key":                      {"surface: egress\ncases:\n  - name: a\n    request: {url: \"https://api.github.com/\"}\n    expct: deny\n    expect: allow\n", "expct"},
		"no expectation":                        {"surface: egress\ncases:\n  - name: a\n    request: {url: \"https://api.github.com/\"}\n", "expect"},
		"a rule on an allow":                    {"surface: egress\ncases:\n  - name: a\n    request: {url: \"https://api.github.com/\"}\n    expect: allow\n    rule: scheme\n", "expect: deny"},
		"another surface's":                     {"surface: egress\ncases:\n  - name: a\n    request: {url: \"https://api.github.com/\", task: http}\n    expect: allow\n", "another surface"},
		"no url":                                {"surface: egress\ncases:\n  - name: a\n    request: {method: GET}\n    expect: allow\n", "request.url is required"},
		"a duplicate name":                      {"surface: egress\ncases:\n  - {name: a, request: {url: \"https://api.github.com/\"}, expect: allow}\n  - {name: a, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "used twice"},
		"an unknown surface":                    {"surface: secrets\ncases:\n  - {name: a, expect: allow}\n", "surface"},
		"no cases":                              {"surface: egress\ncases: []\n", "cases"},
		"an alias":                              {"surface: egress\nx: &a 1\ncases:\n  - {name: a, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "anchors"},
		"a second document":                     {"surface: egress\ncases:\n  - {name: a, request: {url: \"https://api.github.com/\"}, expect: allow}\n---\nsurface: egress\n", "one document"},
		"a bad ip":                              {"surface: egress\ncases:\n  - {name: a, request: {url: \"https://api.github.com/\", ip: nope}, expect: allow}\n", "request.ip"},
		"an ip that contradicts a literal host": {"surface: egress\ncases:\n  - {name: a, request: {url: \"https://127.0.0.1/\", ip: \"8.8.8.8\"}, expect: deny}\n", "IP-literal host"},
		"a url with no port":                    {"surface: egress\ncases:\n  - {name: a, request: {url: \"ftp://10.0.0.1/\"}, expect: allow}\n", "write the port"},
		"the retired identity key":              {"surface: egress\ncases:\n  - {name: a, identity: {namespace: team-a}, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "identity"},
		"a misspelled principal field":          {"surface: egress\ncases:\n  - {name: a, principal: {namespce: team-a}, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "namespce"},
		"a subject with no issuer":              {"surface: egress\ncases:\n  - {name: a, principal: {subject: a@example.com}, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "without the other"},
		"an issuer entry nothing admitted":      {"surface: egress\ncases:\n  - {name: a, principal: {issuer_entry: ci}, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "issuer_entry"},
		"a kind no policy assigns":              {"surface: egress\ncases:\n  - {name: a, principal: {kind: robot}, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "robot"},
		"a kind spelled in the enum's case":     {"surface: egress\ncases:\n  - {name: a, principal: {kind: Human}, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "Human"},
		"a surface mismatched":                  {"surface: task\ncases:\n  - {name: a, request: {url: \"https://api.github.com/\"}, expect: allow}\n", "another surface"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := runFlow(t, "policy", "test", policy, writeFile(t, "cases.yaml", c.cases))
			require.NotEqual(t, 0, res.ExitCode, res.Output())
			require.Empty(t, res.Stdout, "a refused suite must not print a verdict")
			require.Contains(t, res.Output(), c.want)
		})
	}
}

func TestPolicyTestRefusesAPolicyThatDoesNotLoad(t *testing.T) {
	t.Parallel()

	cases := writeFile(t, "cases.yaml", "surface: egress\ncases:\n  - {name: a, request: {url: \"https://api.github.com/\"}, expect: deny}\n")

	for name, policy := range map[string]string{
		"a rule that does not compile": "egress:\n  allow:\n    - host ==\n",
		"a misspelled key":             "egress:\n  alow: [\"true\"]\n",
		"a proxy, which needs DNS":     "egress:\n  proxy_from_environment: true\n  allow:\n    - host == \"api.github.com\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := runFlow(t, "policy", "test", writeFile(t, "egress.yaml", policy), cases)
			require.NotEqual(t, 0, res.ExitCode, res.Output())
			require.Empty(t, res.Stdout, "a policy that did not load must not be tested as an empty one")
		})
	}

	res := runFlow(t, "policy", "test", filepath.Join(t.TempDir(), "missing.yaml"), cases)
	require.NotEqual(t, 0, res.ExitCode)
}

func TestPolicyTestBoundsTheSuite(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "task.yaml", "allow: ['task == \"http\"']\n")

	suite := func(n int) string {
		var b strings.Builder
		b.WriteString("surface: task\ncases:\n")
		for i := range n {
			b.WriteString("  - {name: c" + strings.Repeat("x", i%7) + string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676)) +
				", request: {task: http}, expect: allow}\n")
		}
		return b.String()
	}

	res := runFlow(t, "policy", "test", policy, writeFile(t, "ok.yaml", suite(policytest.MaxCases)))
	require.Equal(t, 0, res.ExitCode, res.Output())

	res = runFlow(t, "policy", "test", policy, writeFile(t, "over.yaml", suite(policytest.MaxCases+1)))
	require.NotEqual(t, 0, res.ExitCode)
	require.Empty(t, res.Stdout)

	res = runFlow(t, "policy", "test", policy, writeFile(t, "big.yaml", suite(1)+"# "+strings.Repeat("x", policytest.MaxSuiteBytes)))
	require.NotEqual(t, 0, res.ExitCode)
	require.Contains(t, res.Output(), "limit")
}

// A case declares the whole caller a rule can read, not only the tenant: its
// kind, a list claim, the actions it was granted and who acts for it. The same
// rule is put to each declared identity, and only the one that carries every
// fact it names is admitted; a case never gains what it did not write.
func TestPolicyTestAPrincipalCarriesOnlyWhatTheCaseDeclares(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "task.yaml", `allow:
  - identity.kind == "agent" && "ops" in identity.claims.groups && "run.start" in identity.actions && !identity.delegated
`)
	cases := writeFile(t, "cases.yaml", `surface: task
cases:
  - name: an agent in ops, granted run.start, acting for itself, is admitted
    principal:
      subject: bot@example.com
      issuer: https://issuer.example.com
      kind: agent
      claims: {groups: [dev, ops]}
      actions: [run.start]
    request: {task: http}
    expect: allow
  - name: the same caller as a human is refused
    principal:
      kind: human
      claims: {groups: [ops]}
      actions: [run.start]
    request: {task: http}
    expect: deny
    rule: allow rules
  - name: no kind is no kind, not agent
    principal:
      claims: {groups: [ops]}
      actions: [run.start]
    request: {task: http}
    expect: deny
    rule: allow rules
  - name: a groups claim without ops is refused
    principal:
      kind: agent
      claims: {groups: [dev]}
      actions: [run.start]
    request: {task: http}
    expect: deny
    rule: allow rules
  - name: an agent granted no action is refused
    principal:
      kind: agent
      claims: {groups: [ops]}
    request: {task: http}
    expect: deny
    rule: allow rules
  - name: an agent acting on behalf of another is refused
    principal:
      kind: agent
      claims: {groups: [ops]}
      actions: [run.start]
      actors:
        - {issuer: "https://issuer.example.com", subject: "carol@example.com"}
    request: {task: http}
    expect: deny
    rule: allow rules
  - name: a case that declares nobody is refused
    request: {task: http}
    expect: deny
    rule: allow rules
`)

	res, report := policyTestJSON(t, policy, cases)
	require.Equal(t, 0, res.ExitCode, res.Output())
	require.Equal(t, 7, report.Passed, res.Output())
}

// The kind a case declares reaches the egress and exec evaluators through the
// same Caller, so one spelling tests every surface.
func TestPolicyTestAKindAndAListClaimReachEgress(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "egress.yaml", `egress:
  schemes: [https]
  allow:
    - identity.kind == "workload" && "payments" in identity.claims.teams && host == "partner-a.example.com"
`)
	cases := writeFile(t, "cases.yaml", `surface: egress
cases:
  - name: a payments workload reaches the partner
    principal: {kind: workload, claims: {teams: [payments]}}
    request: {url: "https://partner-a.example.com/v1"}
    expect: allow
  - name: a payments human does not
    principal: {kind: human, claims: {teams: [payments]}}
    request: {url: "https://partner-a.example.com/v1"}
    expect: deny
    rule: allow rules
`)

	res, report := policyTestJSON(t, policy, cases)
	require.Equal(t, 0, res.ExitCode, res.Output())
	require.Equal(t, 2, report.Passed, res.Output())
}

// A kind is a name: protojson would read `kind: 1` as HUMAN and keep `kind: 99`
// as an unknown kind that renders as none, so a case would stand for a caller
// it did not describe.
func TestPolicyTestRefusesANumericKind(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "egress.yaml", tenantEgressPolicy)

	for _, kind := range []string{"1", "99", "null", "[human]"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			res := runFlow(t, "policy", "test", policy, writeFile(t, "cases.yaml",
				"surface: egress\ncases:\n  - {name: a, principal: {kind: "+kind+"}, request: {url: \"https://api.github.com/\"}, expect: allow}\n"))
			require.NotEqual(t, 0, res.ExitCode, res.Output())
			require.Empty(t, res.Stdout)
		})
	}
}

// A claim over the carried-claim bounds is dropped when the identity is read,
// which would let a deny case pass for a caller it did not describe, so the
// case is refused instead.
func TestPolicyTestRefusesAnOverBoundClaim(t *testing.T) {
	t.Parallel()

	policy := writeFile(t, "egress.yaml", tenantEgressPolicy)

	deep := "x"
	for range 6 {
		deep = "[" + deep + "]"
	}

	wide := "[" + strings.Repeat("a, ", 600) + "a]"

	for name, claim := range map[string]string{"over depth": deep, "over count": wide} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := runFlow(t, "policy", "test", policy, writeFile(t, "cases.yaml",
				"surface: egress\ncases:\n  - {name: a, principal: {claims: {groups: "+claim+"}}, request: {url: \"https://api.github.com/\"}, expect: deny}\n"))
			require.NotEqual(t, 0, res.ExitCode, res.Output())
			require.Empty(t, res.Stdout, "a refused suite must not print a verdict")
			require.Contains(t, res.Output(), "principal.claims")
			require.Contains(t, res.Output(), `"groups"`)
		})
	}
}
