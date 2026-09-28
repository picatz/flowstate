# flowstate-plugin-oidc

No tasks. One secret scheme: `${secret('oidc:<provider>')}` resolves to an
access token minted for that call, through the OAuth 2.0 client credentials
grant (RFC 6749 §4.4), against a token endpoint an operator configured.

It is the canonical **secret provider** plugin in this tree, and the exchange
itself is [`pkg/flowstate/v1/auth`](../../pkg/flowstate/v1/auth)'s — the same
exchanger the engine's outbound federation uses — so there is one
implementation of "ask an authorization server for a token" here.

An example lives at [`examples/plugins/oidc`](../../examples/plugins/oidc).

## Why a secret provider and not a task

A task returns outputs, and outputs are durable history. A task that returned an
access token would write a bearer credential into every run's record, where it
outlives its usefulness and reaches everyone who can read a run. That is the
reason [#1344](https://github.com/picatz/flowstate/issues/1344)'s audit refused
generic signing, and it applies to minting just as squarely.

A reference is the other shape. `${secret('oidc:billing-api')}` is a reference
in the Flowfile *and* in history; the host resolves it worker-side, at the
moment the step runs, under the caller's own namespace, and hands the value to
the task that needs it. The credential exists in one process for one call.

`flow plugins` will show this plugin offering **no tasks**, and the reachability
test asserts that — minting belongs behind the secret boundary, not in front of
it.

## Building

```console
$ go -C plugins/oidc build -o /path/to/plugins/flowstate-plugin-oidc .
```

Run it from the repository root. The plugin is its own Go module, so `go -C`
builds it in its own directory, and the `-o` path must be absolute: a relative
one resolves against `plugins/oidc`.

## Configuring

```console
$ flow worker --plugin-dir /path/to/plugins \
    --temporal-deployment-name flowstate --build-id "$(git rev-parse --short HEAD)" \
    --plugin-env oidc=FLOWSTATE_OIDC_PROVIDERS=/etc/flowstate/oidc-providers.yaml \
    --auth-policy /etc/flowstate/auth-policy.yaml
```

A plugin inherits nothing of the worker's environment, so the providers file
is named with `--plugin-env`. `--auth-policy` is required, and needs a
`secrets:` section: this plugin registers the `oidc:` scheme, a worker holding a
secret provider with no access policy refuses to start, and that section is
what decides which workloads may resolve which `oidc:` reference.

```yaml
providers:
  billing-api:                                   # the name a reference uses
    token_url: https://idp.example.com/oauth2/token
    client_id: flowstate-worker
    client_secret_file: /etc/flowstate/secrets/billing-client
    scopes: [invoices.read]
    max_lifetime: 1h
    namespaces: [billing]                        # optional tenant scoping
```

The client secret is a **path, not a value**: a configuration document an
operator diffs in review should not be one they have to redact, and the secret
is read when it is needed rather than held for the life of the process.

The file is checked when the plugin starts:

- a provider name is what a reference writes, so it is lower-case letters,
  digits and interior hyphens, at most 64 characters;
- `token_url` is HTTPS, and `client_id` and `client_secret_file` are
  required, the file an absolute path (read at each resolution, at most
  8 KiB);
- `max_lifetime` defaults to 1h, with a ceiling of 12h;
- `timeout` bounds one exchange, default 20s, ceiling 30s;
- a scope holds no whitespace, and the file itself is at most 1 MiB.

With no file, or one that fails these checks, this plugin mints nothing and
says so: a running worker logs a warning naming the reason at each health
check. `flow plugins` does not report health.

## Lifetimes

Every token comes back with the lifetime the authorization server reported, less
a refresh margin, and that travels to the host as `SecretResponse.ExpiresIn`.
**Nothing caches the credential**: not this plugin, since a second cache would
be a second answer about when a token stops being usable, and not the worker
either, which wires no cache in front of a plugin's secret provider today, so
every resolution mints a fresh token. `max_lifetime` is not a cap on a
lifetime: an authorization server that reports an `expires_in` longer than it,
or none at all, is refused (`PermissionDenied`) rather than shortened.

## Classification

An authorization server that **refused this client** is permanent: the same
secret sent again is refused again, so it is `PermissionDenied` and no retry
budget is spent on it. One that **could not be reached** is `Unavailable` and
retryable. A destination the deployment's egress policy denies is
`PermissionDenied` too — a policy decision is not a transient condition, which
is a distinction this plugin's work
[fixed in the engine's own exchanger](../../pkg/flowstate/v1/auth/exchange.go)
rather than papering over here.

## Tenancy

A provider may list `namespaces`, compared against the namespace the host
established for the calling workload, never one the workload declared. The
host's own secret access policy decides whether a workload may ask for a
reference at all; this decides whether the operator granted *this tenant* that
provider.

## What was left undone, and why

- **Discovery.** A provider names its token endpoint. Discovery is a second
  fetch whose only purpose is to find a URL an operator already knows.
- **Authorization code, device code, refresh tokens.** Flows with a human at a
  browser. A durable workload is not one.
- **Token exchange (RFC 8693).** The engine implements it for workload identity
  federation, where the assertion is the *worker's own* identity — reaching that
  from a plugin would mean handing a plugin the issuer's signing key, which is
  server-side authority. Making federation reachable from a Flowfile is real,
  separate work (and is where `aws`/`gcp` plugins would start).
- **`private_key_jwt` client authentication.** The exchanger supports assertion
  -based client auth already; wiring it here needs a key-custody story for the
  plugin's own signing key, which is the same question token exchange raises.
