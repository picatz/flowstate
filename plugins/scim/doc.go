// Command flowstate-plugin-scim reads and deactivates identity-provider users
// over SCIM 2.0.
//
// # Why SCIM and not a provider
//
// A plugin named for Okta would be a plugin named for one company's API, and
// the next deployment runs Entra ID. SCIM is the standard those products
// already serve - RFC 7643 defines the user and group resources, RFC 7644 the
// protocol over them - so one plugin covers Okta, Entra ID, Keycloak, Google
// Workspace, JumpCloud and anything else that implements the specification.
// Where providers differ, this plugin follows the RFC and says so rather than
// special-casing a vendor.
//
// # What it is for
//
// The compliance access review, which is the recurring workload every audited
// organization runs and the one examples/enterprise-access-review has had to
// simulate with an `http:` call to a fake IAM endpoint: read the accounts in
// scope, gather the evidence, put it in front of a reviewer, and turn off what
// the reviewer refuses - durably, across the days that review takes.
//
// # Three tasks, and the write that is missing on purpose
//
// scim.user_get and scim.user_list read. scim.user_deactivate is the only
// write, and it is deactivation rather than deletion: providers treat DELETE as
// irreversible, and an organization's auditor wants the account that was turned
// off on a date, not an account that is gone. Creating and updating users is
// provisioning rather than review, needs the whole resource schema rather than
// one attribute, and is a different design; it is not half-added here.
//
// # Filters, and why a user name is not one
//
// A SCIM filter is an expression language the provider evaluates. scim.user_get
// takes a user_name and builds the filter itself, escaping the value per RFC
// 7644 section 3.4.2.2, so a name holding a quote cannot become filter syntax -
// the same reason plugins/sql binds every parameter rather than interpolating
// SQL text. scim.user_list does take a filter, because a review's scope is a
// filter and there is no typed surface that covers what organizations actually
// ask for; it is bounded, checked for shape, and evaluated by the provider
// under the calling credential, so what it can reach is what that credential
// can reach.
//
// # Writes, retries, and the one conditional
//
// Deactivation is idempotent - replacing active with false twice leaves the
// same account in the same state - so a transport failure is retryable rather
// than an unknown outcome, which is the opposite of what plugins/slack's
// non-idempotent post has to do. Where a provider sends an ETag, passing it
// back as expected_version makes the write a compare-and-swap: a user modified
// since the reviewer read them fails as a conflict a workflow can dispatch on,
// rather than overwriting a change nobody saw.
package main
