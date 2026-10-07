package auth_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/strictyaml"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// docSection returns the body of the markdown section whose heading line is
// exactly heading, up to the next heading of the same or a higher level.
func docSection(t *testing.T, doc, heading string) string {
	t.Helper()

	level := len(heading) - len(strings.TrimLeft(heading, "#"))
	var body []string
	inSection := false
	for line := range strings.Lines(doc) {
		line = strings.TrimRight(line, "\r\n")
		if inSection {
			if rest := strings.TrimLeft(line, "#"); strings.HasPrefix(line, "#") && strings.HasPrefix(rest, " ") &&
				len(line)-len(rest) <= level {
				break
			}
			body = append(body, line)
			continue
		}
		inSection = line == heading
	}
	return strings.Join(body, "\n")
}

// yamlBlocks returns the contents of every ```yaml fence in section, the same
// extraction the keyring page's drift test performs.
func yamlBlocks(section string) []string {
	var blocks []string
	for i, chunk := range slices.Collect(strings.SplitSeq(section, "```yaml\n")) {
		// The first chunk is the prose before any fence.
		if block, _, found := strings.Cut(chunk, "```"); i > 0 && found {
			blocks = append(blocks, block)
		}
	}
	return blocks
}

func deploymentDoc(t *testing.T) string {
	t.Helper()

	doc, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "docs", "DEPLOYMENT.md"))
	require.NoError(t, err)
	return string(doc)
}

// TestTheDocumentedProviderPoliciesParse holds docs/DEPLOYMENT.md's per-provider
// trust policies to the schema: each is a policy ParsePolicy (which validates)
// accepts, so a page that stops parsing fails here rather than in an operator's
// first rollout.
func TestTheDocumentedProviderPoliciesParse(t *testing.T) {
	t.Parallel()

	section := docSection(t, deploymentDoc(t), "### Trust policy per identity provider")
	blocks := yamlBlocks(section)
	require.Len(t, blocks, 7, "the provider policies were not found, so nothing was checked")

	var mapped int
	for _, block := range blocks {
		policy, err := auth.ParsePolicy([]byte(block))
		require.NoError(t, err, "docs/DEPLOYMENT.md teaches a trust policy the schema refuses:\n%s", block)
		for _, issuer := range policy.Issuers {
			if len(issuer.NamespaceMap) > 0 {
				mapped++
			}
		}
	}
	// GitHub Actions, GitLab, Kubernetes and Entra ID name their tenant by a
	// claim that is not a namespace.
	require.Equal(t, 4, mapped, "a namespace_map example is missing or was parsed away")
}

// TestTheExtractorFindsNothingWhereThereIsNothing is the negative half: a
// section that is absent, or has no yaml fence, yields no blocks, which the
// tests above refuse rather than pass over.
func TestTheExtractorFindsNothingWhereThereIsNothing(t *testing.T) {
	t.Parallel()

	doc := "## A\n\n### Other\n\n```yaml\na: b\n```\n"
	require.Empty(t, docSection(t, doc, "### Trust policy per identity provider"))
	require.Empty(t, yamlBlocks(docSection(t, doc, "### Trust policy per identity provider")))
	require.Len(t, yamlBlocks(docSection(t, doc, "### Other")), 1)
	require.Equal(t, []string{"a: b\n"}, yamlBlocks(docSection(t, doc, "### Other")))
}

// TestTheDocumentedEgressSection ties docs/DEPLOYMENT.md's identity egress
// section to the code: the fields it teaches are fields of
// netpolicy.EgressConfig, its two worked policies load, and the loopback
// rehearsal is refused when the section that admits it is removed — the same
// answer for Policy.Validate-admitted loopback http as the fetch gives (#1694).
func TestTheDocumentedEgressSection(t *testing.T) {
	t.Parallel()

	section := docSection(t, deploymentDoc(t), "### Identity egress: where the trust policy may fetch keys from")

	tags := map[string]bool{}
	for field := range reflect.TypeFor[netpolicy.EgressConfig]().Fields() {
		tags[strings.Split(field.Tag.Get("yaml"), ",")[0]] = true
	}
	for _, field := range []string{"schemes", "allow_loopback", "allow_private_networks", "allow_networks"} {
		require.True(t, tags[field], "%s is no longer an egress: field", field)
		require.Contains(t, section, "`"+field, "the section no longer documents %s", field)
	}

	blocks := yamlBlocks(section)
	require.Len(t, blocks, 3, "the egress policies were not found, so nothing was checked")

	// Find the worked examples by what they admit.
	var private, loopback string
	for _, block := range blocks {
		policy, err := auth.ParsePolicy([]byte(block))
		if err != nil {
			continue
		}
		switch {
		case policy.Egress != nil && policy.Egress.AllowPrivateNetworks:
			private = block
		case policy.Egress != nil && policy.Egress.AllowLoopback:
			loopback = block
		}
	}
	require.NotEmpty(t, private, "the in-cluster worked example does not load")
	require.NotEmpty(t, loopback, "the loopback worked example does not load")

	withoutSection, _, found := strings.Cut(loopback, "\negress:")
	require.True(t, found)
	_, err := auth.ParsePolicy([]byte(withoutSection))
	require.ErrorContains(t, err, "configure the trust policy's egress: section")
	require.ErrorContains(t, err, "`schemes: [http, https]`")
}

// TestTheDocumentedTenantPolicyParses ties docs/DEPLOYMENT.md's per-tenant issuer
// section to the schema: its policy is one ParsePolicy accepts, the tenants it
// lists each have the issuer URL the section teaches, and a name it does not list
// has none.
func TestTheDocumentedTenantPolicyParses(t *testing.T) {
	t.Parallel()

	section := docSection(t, deploymentDoc(t), "### Per-tenant issuers")
	blocks := yamlBlocks(section)
	require.Len(t, blocks, 1, "the per-tenant policy was not found, so nothing was checked")

	// The block is the `federation:` section of a trust policy, which a whole
	// policy would surround with the issuers it authenticates callers by; the
	// section is what the page teaches, so it is what is held to the schema.
	var policy struct {
		Federation auth.FederationPolicy `yaml:"federation"`
	}
	require.NoError(t, strictyaml.UnmarshalStrict([]byte(blocks[0]), &policy),
		"docs/DEPLOYMENT.md teaches a federation policy the schema refuses:\n%s", blocks[0])
	require.NoError(t, policy.Federation.Validate(), blocks[0])
	require.Equal(t, []string{"acme", "globex"}, policy.Federation.Tenants)

	for _, tenant := range policy.Federation.Tenants {
		issuer, err := policy.Federation.TenantIssuerURL(tenant)
		require.NoError(t, err)
		require.Equal(t, policy.Federation.Issuer+"/tenants/"+tenant, issuer)
		require.Contains(t, section, "https://HOST/tenants/NAMESPACE", "the section no longer teaches the issuer URL's shape")
	}

	_, err := policy.Federation.TenantIssuerURL("initech")
	require.ErrorIs(t, err, auth.ErrUnknownTenant)
}
