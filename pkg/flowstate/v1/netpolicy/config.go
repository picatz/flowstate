package netpolicy

import (
	"crypto/tls"
	"fmt"
	"net/netip"
	"time"

	"github.com/goccy/go-yaml"
)

// Config is the file form of a policy: what an operator writes in YAML and hands
// to `flow worker --egress-policy`, mapped onto the same [Option] set a Go caller
// uses. One field per option that a deployment plausibly writes down; the options
// with no field here — root CAs, the control plane, per-phase timeouts — are
// reachable from Go, and a field can be added when a deployment needs one.
//
// A Config that says nothing builds the safe default policy [New] builds with no
// options. Every field therefore only ever *adds* to what the file expresses, and
// none of them can remove a bound: the file is the loosening surface, and a bound
// an operator wants gone should be raised, not deleted.
type Config struct {
	Egress EgressConfig `json:"egress" yaml:"egress"`
}

// EgressConfig is the `egress:` section of a policy file.
//
// The zero value is the safe default: public http and https only, everything
// internal denied, bounded in every dimension. Absent fields keep the defaults
// described on the corresponding option and constant.
type EgressConfig struct {
	// CredentialHosts names hosts allowed to receive worker-resolved secrets or
	// federated credentials. General egress permission never implies this grant.
	CredentialHosts []string `json:"credential_hosts,omitempty" yaml:"credential_hosts,omitempty"`

	// Schemes replaces the scheme allowlist. Only http and https can be named,
	// and naming neither — an explicitly empty list — is an error rather than a
	// policy that allows nothing by accident. See [WithSchemes].
	Schemes []string `json:"schemes,omitempty" yaml:"schemes,omitempty"`

	// AllowLoopback permits loopback addresses, which is where development
	// servers and sidecars listen. See [WithAllowLoopback].
	AllowLoopback bool `json:"allow_loopback,omitempty" yaml:"allow_loopback,omitempty"`

	// AllowPrivateNetworks permits private address space wholesale. Prefer
	// AllowNetworks naming the ranges actually needed. See
	// [WithAllowPrivateNetworks].
	AllowPrivateNetworks bool `json:"allow_private_networks,omitempty" yaml:"allow_private_networks,omitempty"`

	// AllowNetworks restricts connections to the given CIDR ranges, which are
	// then exempt from the category denials — except cloud metadata, which stays
	// denied. See [WithAllowNetworks].
	AllowNetworks []string `json:"allow_networks,omitempty" yaml:"allow_networks,omitempty"`

	// DenyNetworks denies the given CIDR ranges before anything else is
	// consulted. See [WithDenyNetworks].
	DenyNetworks []string `json:"deny_networks,omitempty" yaml:"deny_networks,omitempty"`

	// AllowPorts restricts requests to the given ports. Empty means any port,
	// subject to DenyPorts. See [WithAllowPorts].
	AllowPorts []int `json:"allow_ports,omitempty" yaml:"allow_ports,omitempty"`

	// DenyPorts denies the given ports, overriding AllowPorts. See
	// [WithDenyPorts].
	DenyPorts []int `json:"deny_ports,omitempty" yaml:"deny_ports,omitempty"`

	// Allow holds CEL allow rules. Configuring any turns the policy into an
	// allowlist: a request must match at least one. The attributes a rule may
	// name are documented on the package. See [WithAllowRules].
	Allow []string `json:"allow,omitempty" yaml:"allow,omitempty"`

	// Deny holds CEL deny rules. A matching rule denies the request regardless
	// of the allow rules, and a rule that fails to evaluate denies it too. See
	// [WithDenyRules].
	Deny []string `json:"deny,omitempty" yaml:"deny,omitempty"`

	// MaxRedirects sets how many redirects may be followed; zero refuses the
	// first one. Unset keeps [DefaultMaxRedirects]. See [WithMaxRedirects].
	MaxRedirects *int `json:"max_redirects,omitempty" yaml:"max_redirects,omitempty"`

	// MaxResponseBytes caps the response body, written the way sizes are said:
	// `1MiB`, `10MB`, or a bare count of bytes — see [ByteSize] for the forms.
	// It must be positive: the bound cannot be removed from a file, only moved.
	// Unset keeps [DefaultMaxResponseBytes]. See [WithMaxResponseBytes].
	MaxResponseBytes *ByteSize `json:"max_response_bytes,omitempty" yaml:"max_response_bytes,omitempty"`

	// Timeout bounds a whole request, written as a duration such as "30s". It
	// must be positive, for the same reason MaxResponseBytes must: unbounded is
	// not something a file can ask for. Unset keeps [DefaultTimeout]. See
	// [WithTimeout].
	Timeout *time.Duration `json:"timeout,omitempty" yaml:"timeout,omitempty"`

	// MinTLSVersion is the lowest TLS version accepted, written as "1.2" or
	// "1.3". Unset means 1.2, and nothing lower exists to ask for. See
	// [WithMinTLSVersion].
	MinTLSVersion string `json:"min_tls_version,omitempty" yaml:"min_tls_version,omitempty"`

	// ProxyFromEnvironment routes requests through the proxy the HTTP_PROXY,
	// HTTPS_PROXY, and NO_PROXY variables name. Off by default because a proxy
	// is the one place the address checks cannot see past — the caveat on
	// [WithProxyFromEnvironment] applies in full.
	ProxyFromEnvironment bool `json:"proxy_from_environment,omitempty" yaml:"proxy_from_environment,omitempty"`
}

// ParseConfig decodes a policy file from YAML or JSON, which is a subset of
// YAML. Unknown and duplicate fields are errors, so that a misspelled key fails
// loudly at startup instead of silently dropping a restriction — the same rule
// [github.com/picatz/flowstate/pkg/flowstate/v1/auth.ParsePolicy] applies to the
// trust policy, for the same reason.
//
// Parsing checks the document's shape. Whether the fields describe a usable
// policy is [Config.Policy]'s job, where [New] compiles and type-checks every
// CEL rule.
func ParseConfig(data []byte) (Config, error) {
	var cfg Config

	if err := yaml.UnmarshalWithOptions(data, &cfg, yaml.Strict()); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
	}

	return cfg, nil
}

// Policy builds the policy the file describes, by way of [Options] and [New].
// Every failure wraps [ErrInvalidPolicy]: a CIDR that does not parse, a port out
// of range, a TLS version that is not "1.2" or "1.3", and every CEL rule problem
// [New] reports.
func (c Config) Policy() (*Policy, error) {
	opts, err := c.Options()
	if err != nil {
		return nil, err
	}

	return New(opts...)
}

// Options translates the file into the option calls a Go caller would write, in
// a fixed order that makes the file mean the same thing however it is arranged.
// It reports the mistakes the option set cannot see because they are properties
// of the textual form: a network that is not a CIDR, a port outside 1–65535, a
// TLS version string that names nothing.
func (c Config) Options() ([]Option, error) {
	e := c.Egress

	var opts []Option

	// Distinguishing an absent list from a written-empty one: `schemes: []` is
	// an operator asking for a policy that can request nothing, which WithSchemes
	// refuses with an error saying so, where an absent key keeps the default.
	if e.Schemes != nil {
		opts = append(opts, WithSchemes(e.Schemes...))
	}

	if e.AllowLoopback {
		opts = append(opts, WithAllowLoopback())
	}
	if e.AllowPrivateNetworks {
		opts = append(opts, WithAllowPrivateNetworks())
	}

	// The empty-allowlist rule, applied to every allow-shaped field and not only
	// schemes. Each of these is nil when absent — keep the default — and non-nil
	// empty when an operator wrote `[]`, which reads as "allow none" and would
	// quietly mean "no restriction" if the empty value dropped the option: the
	// one direction a parse must never widen. A generated policy is where this
	// happens in practice — a template whose range produced nothing.
	//
	// Deny-shaped fields need no such check, because an empty deny list and an
	// absent one both deny nothing, and treating them alike loses no restriction.
	if e.AllowNetworks != nil && len(e.AllowNetworks) == 0 {
		return nil, fmt.Errorf(
			"%w: allow_networks is empty; an empty allowlist would permit every public network, "+
				"which is the default it looks like it restricts — delete the key to mean that, "+
				"or name the networks to mean the restriction", ErrInvalidPolicy)
	}
	if e.AllowPorts != nil && len(e.AllowPorts) == 0 {
		return nil, fmt.Errorf(
			"%w: allow_ports is empty; an empty allowlist would permit every port — delete the key "+
				"to mean that, or name the ports to mean the restriction", ErrInvalidPolicy)
	}
	if e.Allow != nil && len(e.Allow) == 0 {
		return nil, fmt.Errorf(
			"%w: allow is empty; an empty rule list would remove the allowlist gate entirely — delete "+
				"the key to mean that, or write the rules a request must match", ErrInvalidPolicy)
	}
	if e.CredentialHosts != nil && len(e.CredentialHosts) == 0 {
		return nil, fmt.Errorf("%w: credential_hosts is empty; name the permitted recipients or delete the key", ErrInvalidPolicy)
	}
	if len(e.CredentialHosts) > 0 {
		opts = append(opts, WithCredentialHosts(e.CredentialHosts...))
	}

	allowNets, err := parsePrefixes("allow_networks", e.AllowNetworks)
	if err != nil {
		return nil, err
	}
	if len(allowNets) > 0 {
		opts = append(opts, WithAllowNetworks(allowNets...))
	}

	denyNets, err := parsePrefixes("deny_networks", e.DenyNetworks)
	if err != nil {
		return nil, err
	}
	if len(denyNets) > 0 {
		opts = append(opts, WithDenyNetworks(denyNets...))
	}

	allowPorts, err := parsePorts("allow_ports", e.AllowPorts)
	if err != nil {
		return nil, err
	}
	if len(allowPorts) > 0 {
		opts = append(opts, WithAllowPorts(allowPorts...))
	}

	denyPorts, err := parsePorts("deny_ports", e.DenyPorts)
	if err != nil {
		return nil, err
	}
	if len(denyPorts) > 0 {
		opts = append(opts, WithDenyPorts(denyPorts...))
	}

	if len(e.Allow) > 0 {
		opts = append(opts, WithAllowRules(e.Allow...))
	}
	if len(e.Deny) > 0 {
		opts = append(opts, WithDenyRules(e.Deny...))
	}

	if e.MaxRedirects != nil {
		opts = append(opts, WithMaxRedirects(*e.MaxRedirects))
	}

	if e.MaxResponseBytes != nil {
		if *e.MaxResponseBytes <= 0 {
			return nil, fmt.Errorf(
				"%w: max_response_bytes must be positive, got %d; the body cap cannot be removed from a policy file, "+
					"only raised", ErrInvalidPolicy, *e.MaxResponseBytes)
		}
		opts = append(opts, WithMaxResponseBytes(int64(*e.MaxResponseBytes)))
	}

	if e.Timeout != nil {
		if *e.Timeout <= 0 {
			return nil, fmt.Errorf(
				"%w: timeout must be positive, got %s; the request bound cannot be removed from a policy file, "+
					"only raised", ErrInvalidPolicy, *e.Timeout)
		}
		opts = append(opts, WithTimeout(*e.Timeout))
	}

	switch e.MinTLSVersion {
	case "":
		// The default, TLS 1.2.
	case "1.2":
		opts = append(opts, WithMinTLSVersion(tls.VersionTLS12))
	case "1.3":
		opts = append(opts, WithMinTLSVersion(tls.VersionTLS13))
	default:
		return nil, fmt.Errorf(
			"%w: min_tls_version %q is not a TLS version this policy can require; write \"1.2\" or \"1.3\"",
			ErrInvalidPolicy, e.MinTLSVersion)
	}

	if e.ProxyFromEnvironment {
		opts = append(opts, WithProxyFromEnvironment())
	}

	return opts, nil
}

// parsePrefixes parses one field's CIDR list, naming the field and the value in
// the error so an operator can find the line that refused to load.
func parsePrefixes(field string, values []string) ([]netip.Prefix, error) {
	prefixes := make([]netip.Prefix, 0, len(values))
	for _, v := range values {
		p, err := netip.ParsePrefix(v)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: %s: %q is not a network in CIDR form such as \"10.0.0.0/8\": %w",
				ErrInvalidPolicy, field, v, err)
		}
		prefixes = append(prefixes, p)
	}

	return prefixes, nil
}

// parsePorts checks one field's port list fits in a port, which the YAML integer
// type cannot say on its own.
func parsePorts(field string, values []int) ([]uint16, error) {
	ports := make([]uint16, 0, len(values))
	for _, v := range values {
		if v < 1 || v > 65535 {
			return nil, fmt.Errorf("%w: %s: %d is not a port; ports are 1 through 65535",
				ErrInvalidPolicy, field, v)
		}
		ports = append(ports, uint16(v))
	}

	return ports, nil
}
