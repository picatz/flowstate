package netpolicy

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"
)

// The policy file is a policy surface, so its parser gets the fail-closed tests
// the auth policy's has: an unknown key is an error, a rule that does not compile
// is an error, and nothing about a bad file degrades into the default policy —
// the caller refuses to start instead.

func TestParseConfigRejectsUnknownKeys(t *testing.T) {
	t.Parallel()

	// allow_lopback is the misspelling this exists for: silently ignored, it
	// would be a file the operator believes permits loopback and does not —
	// which fails safe — or, spelled deny_ports as denyports, a file that
	// silently drops a restriction, which does not.
	_, err := ParseConfig([]byte(`
egress:
  allow_lopback: true
`))
	if err == nil {
		t.Fatal("a config with an unknown key parsed; a misspelled key must be an error, not a no-op")
	}
	if got := err.Error(); !strings.Contains(got, "allow_lopback") {
		t.Fatalf("the error does not name the unknown key: %v", err)
	}
}

func TestParseConfigRejectsUnknownTopLevelKeys(t *testing.T) {
	t.Parallel()

	_, err := ParseConfig([]byte("egres:\n  schemes: [https]\n"))
	if err == nil {
		t.Fatal("a config misspelling the egress section parsed as an empty (default) policy")
	}
}

func TestConfigRefusesAMalformedRule(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]byte(`
egress:
  deny:
    - 'port !='
`))
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}

	_, err = cfg.Policy()
	if err == nil {
		t.Fatal("a policy with an uncompilable rule built")
	}
	if got := err.Error(); !strings.Contains(got, "port !=") {
		t.Fatalf("the error does not name the rule that failed to compile: %v", err)
	}
}

func TestConfigRefusesABadNetwork(t *testing.T) {
	t.Parallel()

	for field, doc := range map[string]string{
		"allow_networks": "egress:\n  allow_networks: [\"10.0.0.0\"]\n",
		"deny_networks":  "egress:\n  deny_networks: [\"not-a-network\"]\n",
	} {
		cfg, err := ParseConfig([]byte(doc))
		if err != nil {
			t.Fatalf("%s: parsing: %v", field, err)
		}
		if _, err := cfg.Policy(); err == nil {
			t.Fatalf("%s: a network that is not a CIDR built a policy", field)
		} else if !strings.Contains(err.Error(), field) {
			t.Fatalf("%s: the error does not name the field: %v", field, err)
		}
	}
}

func TestConfigRefusesABadPort(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]byte("egress:\n  allow_ports: [70000]\n"))
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	if _, err := cfg.Policy(); err == nil {
		t.Fatal("a port past 65535 built a policy")
	}
}

func TestConfigRefusesABadTLSVersion(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]byte("egress:\n  min_tls_version: \"1.1\"\n"))
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	if _, err := cfg.Policy(); err == nil {
		t.Fatal("min_tls_version 1.1 built a policy; the floor is 1.2")
	}
}

func TestConfigRefusesRemovingABound(t *testing.T) {
	t.Parallel()

	for name, doc := range map[string]string{
		"max_response_bytes": "egress:\n  max_response_bytes: 0\n",
		"timeout":            "egress:\n  timeout: 0s\n",
	} {
		cfg, err := ParseConfig([]byte(doc))
		if err != nil {
			t.Fatalf("%s: parsing: %v", name, err)
		}
		if _, err := cfg.Policy(); err == nil {
			t.Fatalf("%s: a file removed a bound; files may only move bounds", name)
		}
	}
}

func TestConfigRefusesAnEmptySchemeList(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]byte("egress:\n  schemes: []\n"))
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	if _, err := cfg.Policy(); err == nil {
		t.Fatal("an explicitly empty scheme allowlist built a policy that can request nothing")
	}
}

// TestEmptyConfigIsTheDefaultPolicy pins the safe direction: a file that says
// nothing builds exactly what New() with no options builds, so the only thing a
// file can do is be deliberate.
func TestEmptyConfigIsTheDefaultPolicy(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]byte("egress: {}\n"))
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}

	p, err := cfg.Policy()
	if err != nil {
		t.Fatalf("building: %v", err)
	}

	if got := p.MaxResponseBytes(); got != DefaultMaxResponseBytes {
		t.Fatalf("default body cap: got %d, want %d", got, DefaultMaxResponseBytes)
	}
	if got := p.Timeout(); got != DefaultTimeout {
		t.Fatalf("default timeout: got %s, want %s", got, DefaultTimeout)
	}
}

// TestConfigFieldsReachTheOptionsTheyName is the mapping test: each field lands
// in the built policy where its option would have put it. Checked through the
// frozen config rather than by probing behavior, because what this test owns is
// the translation, and the behavior of each option is tested with the option.
func TestConfigFieldsReachTheOptionsTheyName(t *testing.T) {
	t.Parallel()

	timeout := 42 * time.Second

	cfg, err := ParseConfig([]byte(`
egress:
  schemes: [https]
  allow_loopback: true
  allow_private_networks: true
  allow_networks: [10.1.0.0/16]
  deny_networks: [10.1.2.0/24]
  allow_ports: [443, 8443]
  deny_ports: [8443]
  allow:
    - host == "example.com"
  deny:
    - method == "DELETE"
  max_redirects: 2
  max_response_bytes: 4096
  timeout: 42s
  min_tls_version: "1.3"
  proxy_from_environment: true
`))
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}

	p, err := cfg.Policy()
	if err != nil {
		t.Fatalf("building: %v", err)
	}

	c := p.cfg
	if _, ok := c.schemes["https"]; !ok || len(c.schemes) != 1 {
		t.Fatalf("schemes: got %v, want https only", c.schemes)
	}
	if !c.allowed[catLoopback] {
		t.Fatal("allow_loopback did not reach the policy")
	}
	if !c.allowed[catPrivate] || !c.allowed[catUniqueLocal] || !c.allowed[catCarrierGrade] {
		t.Fatal("allow_private_networks did not reach the policy")
	}
	if len(c.allowNetworks) != 1 || c.allowNetworks[0].String() != "10.1.0.0/16" {
		t.Fatalf("allow_networks: got %v", c.allowNetworks)
	}
	if len(c.denyNetworks) != 1 || c.denyNetworks[0].String() != "10.1.2.0/24" {
		t.Fatalf("deny_networks: got %v", c.denyNetworks)
	}
	if _, ok := c.allowPorts[443]; !ok || len(c.allowPorts) != 2 {
		t.Fatalf("allow_ports: got %v", c.allowPorts)
	}
	if _, ok := c.denyPorts[8443]; !ok {
		t.Fatalf("deny_ports: got %v", c.denyPorts)
	}
	if len(p.requestRules.allow) != 1 || len(p.requestRules.deny) != 1 {
		t.Fatalf("rules: got %d allow, %d deny in the request scope",
			len(p.requestRules.allow), len(p.requestRules.deny))
	}
	if c.maxRedirects != 2 {
		t.Fatalf("max_redirects: got %d", c.maxRedirects)
	}
	if c.maxResponseBytes != 4096 {
		t.Fatalf("max_response_bytes: got %d", c.maxResponseBytes)
	}
	if c.timeout != timeout {
		t.Fatalf("timeout: got %s", c.timeout)
	}
	if c.minTLSVersion != tls.VersionTLS13 {
		t.Fatalf("min_tls_version: got %#x", c.minTLSVersion)
	}
	if c.proxy == nil {
		t.Fatal("proxy_from_environment did not reach the policy")
	}
}

// TestConfigRefusesAnEmptyAllowlist covers the widening direction Codex caught
// in review: `allow_networks: []` read as absent would drop the option and
// permit every public network — an empty allowlist quietly meaning "no
// restriction", which is the one direction a parse must never widen. The
// schemes field already refused its empty form; these three did not.
//
// Deny-shaped fields are deliberately not here: an empty deny list and an
// absent one both deny nothing, so treating them alike loses no restriction.
func TestConfigRefusesAnEmptyAllowlist(t *testing.T) {
	t.Parallel()

	for name, doc := range map[string]string{
		"allow_networks": "egress:\n  allow_networks: []\n",
		"allow_ports":    "egress:\n  allow_ports: []\n",
		"allow":          "egress:\n  allow: []\n",
	} {
		cfg, err := ParseConfig([]byte(doc))
		if err != nil {
			t.Fatalf("%s: parsing: %v", name, err)
		}
		if _, err := cfg.Policy(); err == nil {
			t.Fatalf("%s: an explicitly empty allowlist was read as absent, silently widening the policy", name)
		}
	}

	// The distinction the refusals rest on: an empty deny list stays legal,
	// because both readings deny nothing.
	cfg, err := ParseConfig([]byte("egress:\n  deny_networks: []\n  deny_ports: []\n  deny: []\n"))
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	if _, err := cfg.Policy(); err != nil {
		t.Fatalf("an empty deny list was refused, though absent and empty deny the same nothing: %v", err)
	}
}
