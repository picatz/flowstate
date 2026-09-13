# flowstate-plugin-scim

Identity-provider tasks over SCIM 2.0: `scim.user_get` and `scim.user_list`
(the read half an access review gathers evidence with) and
`scim.user_deactivate` (the write half, conditional on what the reviewer
actually saw).

Built on `net/http` and the SDK's governed client against
[RFC 7643](https://www.rfc-editor.org/rfc/rfc7643) (the user and group
resources) and [RFC 7644](https://www.rfc-editor.org/rfc/rfc7644) (the
protocol) - no provider SDK, no subprocess, and no dependency beyond what the
plugin SDK already brings.

An example that runs all three lives at
[`examples/plugins/scim`](../../examples/plugins/scim); read that first if you
want to see it work rather than read about it.

## Why SCIM rather than a provider

A plugin named `okta` would be a plugin named for one company's API, and the
next deployment runs Entra ID. SCIM is the standard those products already
serve, so one plugin covers Okta, Entra ID, Keycloak, Google Workspace,
JumpCloud and anything else implementing the specification. Where providers
differ, this plugin follows the RFC and says so rather than special-casing a
vendor.

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-scim ./plugins/scim
```

## Tasks

| Task | Reads/Writes | Idempotent | Needs a credential |
| --- | --- | --- | --- |
| `scim.user_get` | reads | yes | always |
| `scim.user_list` | reads | yes | always |
| `scim.user_deactivate` | writes | yes (replacing `active` with false twice leaves one account in one state) | always |

Because the write is idempotent, a lost connection is `Unavailable` - retryable
- rather than `OutcomeUnknown`. `plugins/slack` has to take the opposite
posture for `post`, and a task here that *created* a user would too, which is
one more reason there is not one.

## Authentication

`token` is declared in `secret_inputs` and `required_secret_inputs`, so a
Flowfile writes it as a whole secret reference - `${secret('env:SCIM_TOKEN')}` -
and a literal is refused before the specification can enter durable history. The
host resolves it under the caller's namespace and scrubs it from errors and
outputs.

`base_url` is an ordinary input, because a deployment legitimately reviews more
than one tenant, and it must be HTTPS: a directory credential is not sent in
cleartext. Which providers are reachable at all is the deployment's egress
policy, applied on the dial path -
[`examples/plugins/scim/egress-policy.yaml`](../../examples/plugins/scim/egress-policy.yaml)
is what an operator writes to narrow it to one host.

## Filters, and the injection boundary

A SCIM filter is an expression language the provider evaluates, so a value
interpolated into one is code. This plugin draws the line the way
`plugins/sql` draws it between a bound parameter and SQL text:

- **`scim.user_get` takes a `user_name`, never a filter.** It builds
  `userName eq "…"` itself and escapes the value per RFC 7644 §3.4.2.2, so a
  name holding a quote cannot end the literal and change which users the
  expression selects. A name holding a control character is refused outright -
  a filter literal is a JSON string, which cannot carry one.
- **`scim.user_list` does take a `filter`**, because a review's scope *is* a
  filter and no typed surface covers what organizations actually ask for. It is
  bounded at 1 KiB and refused if it carries control characters. What the filter
  can reach is bounded by the credential, not by that check: this plugin does
  not parse the expression, and the provider evaluates it under the token the
  step was given.

## Bounds

| What | Limit |
| --- | --- |
| one user resource | 1 MiB |
| a page of users | 4 MiB |
| `count` | 50 by default, 500 maximum |
| a filter | 1 KiB |
| a resolved credential | 8 KiB |
| `base_url` | 512 B |
| an id or user name | 256 B |
| groups carried per user | 64, each name 256 B |

A `count` over the ceiling is **refused, never lowered**, and a provider that
ignores `count` and returns more is truncated to what the step asked for - the
limit is a bound on what enters durable history, not a hint to a cooperative
provider.

## Design decisions

**Deactivate, never delete.** Providers treat `DELETE` as irreversible, and an
auditor wants the account that was turned off on a date rather than an absence.
There is no `scim.user_delete` and adding one is a decision, not an omission.

**A read before the write.** `scim.user_deactivate` reads the user first, so
`already_inactive` is honest and a review that runs twice makes one change and
one no-op rather than two writes an auditor has to reconcile.

**`expected_version` makes the write a compare-and-swap.** Pass the `version`
(the provider's ETag) from the read the reviewer's evidence came from, and a
user modified since then fails as [`sdk.Conflict`](../../pkg/flowstate/v1/plugin/sdk)
- a classification a Flowfile can `dispatch:` on - rather than overwriting a
change nobody saw. Providers that send no ETag leave only the unconditional
write available, and the input is optional for that reason.

**A missing `active` reads as active.** RFC 7643 makes the attribute optional.
Reading an omitted one as `false` would tell a review that every account at such
a provider is already deactivated.

**Two users for one `user_name` is a refusal.** RFC 7643 makes `userName`
unique; a provider returning two is describing a directory this plugin cannot
safely choose from, and choosing anyway is how a review deactivates the wrong
account.

**A provider that accepts the write and reports the user still active fails.**
An audit record saying an account was turned off, when it is on, is worse than
an error.

## What was left undone, and why

- **Create and update (provisioning).** That is the joiner half of
  joiner-mover-leaver, needs the whole resource schema rather than one
  attribute, and has an unknown-outcome posture this plugin's tasks do not: it
  is a design, not a fourth task added quietly.
- **Groups as a resource.** `scim.user_get` reports the group names a user
  carries, which is what a review reads. Managing group membership is the same
  provisioning design as above.
- **Cursor-based pagination (RFC 8977).** The `startIndex`/`count` pagination
  RFC 7644 defines is what providers implement today; the cursor extension is
  added when a provider a deployment uses requires it.
