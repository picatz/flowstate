package docsgen

import (
	"fmt"
	"strings"
)

// environmentVariable is one variable this build reads.
type environmentVariable struct {
	// name is the variable, or the family's spelling where family is set.
	name string

	// value is what happens when it is unset.
	value string

	// purpose is what setting it does.
	purpose string

	// read is where the process reads it — the files calling os.Getenv or
	// os.LookupEnv for this name, so a reader who doubts the sentence can go and
	// check it.
	//
	// Read sites, not declaration sites, and the distinction is not pedantic:
	// the five FLOWSTATE_PLUGIN_* entries pointed at the package declaring their
	// names, which reads none of them, so anyone who followed the column found
	// nothing and had no way to tell whether the document or their reading was
	// wrong. TestEveryDocumentedReadLocationIsWhereItIsRead now compares this
	// against the tree in both directions.
	read string

	// family marks an entry that stands for a *set* of variables rather than one
	// name: `FLOWSTATE_SECRET_<NAME>`, the OTLP exporter's own configuration. The
	// drift test does not require a literal for these, and the mirror probe skips
	// them, because there is no single name to set.
	family bool
}

// documentedEnvironmentVariables is the one place this repository says which
// variables it reads.
//
// Hand-kept, because there is no registration point to derive it from: a variable
// is read wherever it is needed — a flag default here, a condition there, a size
// ceiling in the server — and inventing a registry every read site had to call
// would be a fifth thing to forget rather than a first thing to derive.
//
// What makes it honest is [TestEveryEnvironmentReadIsDocumented], which parses
// every non-test file under cmd/ and pkg/, collects the variable names they
// mention and every os.Getenv/os.LookupEnv call site, and fails on one this list
// does not carry — and fails the other way too, on an entry nothing reads any
// more. This table drifting is therefore a red test rather than a wrong document,
// which is the whole reason it is allowed to be written by hand.
func (g *Generator) documentedEnvironmentVariables() []environmentVariable {
	return []environmentVariable{
		{
			name:    "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
			value:   "unset",
			purpose: "Set by GitHub Actions inside a job granted `id-token: write`; the request token the `github-actions` credential source presents to the runner's own OIDC token endpoint. Never configured by an operator.",
			read:    "pkg/flowstate/v1/credentialsource/github_actions.go",
		},
		{
			name:    "ACTIONS_ID_TOKEN_REQUEST_URL",
			value:   "unset",
			purpose: "Set by GitHub Actions inside a job granted `id-token: write`; the runner's OIDC token endpoint the `github-actions` credential source asks for a token. Never configured by an operator.",
			read:    "pkg/flowstate/v1/credentialsource/github_actions.go",
		},
		{
			name:    "CI_JOB_JWT_V2",
			value:   "unset",
			purpose: "Set by GitLab before 17.0, which removed it. Never read for its value: the `gitlab` credential source only checks whether it is present, so a job still relying on it is told that it was removed and to declare an `id_tokens:` token instead, rather than being told its ID token is missing.",
			read:    "pkg/flowstate/v1/credentialsource/gitlab.go",
		},
		{
			name:    "FLOWSTATE_ADDRESS",
			value:   g.src.DefaultAddress,
			purpose: "Address the API server listens on, and that the client commands connect to.",
			read:    "cmd/flow/client.go, cmd/flow/main.go, cmd/flow/mcp.go, cmd/flow/serverdev.go",
		},
		{
			name:    "FLOWSTATE_ALLOW_LOOPBACK_EGRESS",
			value:   "unset",
			purpose: "Permit the `http` task to reach loopback addresses. Ignored while an `--egress-policy` file is in force: a policy that wants loopback says `allow_loopback: true`.",
			read:    "pkg/flowstate/v1/eval_task_http_def.go, cmd/flow/serverdev.go",
		},
		{
			name:    "FLOWSTATE_AUDIENCE",
			value:   "unset",
			purpose: "Default for `--audience`: the relying party a credential is addressed to. Required by `--credential-source=github-actions`, which mints a token for it. Checked against the token's own `aud` claim by `gitlab` and `terraform-cloud`, whose platforms bound the audience at job or workspace configuration and cannot be asked for another; a mismatch is refused with the setting to change. Ignored by `file` and `env`.",
			read:    "cmd/flow/client.go",
		},
		{
			name:    "FLOWSTATE_AUTH_POLICY",
			value:   "unset",
			purpose: "Default for `--auth-policy`: on `flow server` and `flow mcp serve` the trust policy naming which issuers and claims to accept; on `flow worker`, `flow run local` and `flow mcp` the same file's secrets rules, authorizing worker-side resolution.",
			read:    "cmd/flow/main.go, cmd/flow/mcp.go, cmd/flow/mcpserve.go, cmd/flow/serverdev.go, cmd/flow/taskrun.go",
		},
		{
			name:    "FLOWSTATE_BACKGROUND",
			value:   "unset",
			purpose: "Declare the terminal background (`dark`/`light`) instead of querying for it. Also the way out of the four-second wait on a terminal that never answers the query.",
			read:    "cmd/flow/internal/ui/ui.go",
		},
		{
			name:    "FLOWSTATE_BUILD_ID",
			value:   "unset",
			purpose: "Default for `--build-id`: this worker binary's version identifier, unique per build. Required alongside the deployment name.",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_CREDENTIAL_SOURCE",
			value:   "unset",
			purpose: "Default for `--credential-source`: acquire a credential from a named `pkg/flowstate/v1/credentialsource.Source` (`github-actions`, `gitlab`, `terraform-cloud`, `file`, `env`) instead of the `--token-file`/`FLOWSTATE_TOKEN` default. An unknown or unusable source is an error, never anonymous.",
			read:    "cmd/flow/client.go",
		},
		{
			name:    "FLOWSTATE_DEPLOYMENT_NAME",
			value:   "unset",
			purpose: "Default for `--deployment-name`: the Worker Deployment this worker belongs to. A worker refuses to start without both halves of a version unless `--allow-unversioned-interpreter` accepts the risk.",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_DST_SCHEDULES",
			value:   "24",
			purpose: "How many seeded schedules the deterministic simulation tier explores per case (`pkg/flowstate/v1/dst`). Read by that test harness rather than by any command; the weekly deep tier raises it. Capped, because a schedule is a whole workflow run and the cost is linear.",
			read:    "pkg/flowstate/v1/dst/dst.go",
		},
		{
			name:    "FLOWSTATE_DST_SEED",
			value:   "unset",
			purpose: "Replay exactly one schedule in the deterministic simulation tier, replacing the search. The number a diverging run prints, which is the whole of what reproduces its interleaving.",
			read:    "pkg/flowstate/v1/dst/dst.go",
		},
		{
			name:    "FLOWSTATE_DST_SEED0",
			value:   "1",
			purpose: "The first seed of the deterministic simulation tier's search, which walks upward from it. Moving it explores a different part of the schedule space; fixed by default so a defect is not intermittent.",
			read:    "pkg/flowstate/v1/dst/dst.go",
		},
		{
			name:    "FLOWSTATE_EGRESS_POLICY",
			value:   "unset",
			purpose: "Default for `--egress-policy`: a YAML policy governing built-in HTTP, first-party SQL PostgreSQL connections, and every plugin the worker launches. When set it replaces the built-in policy entirely rather than merging with it.",
			read:    "cmd/flow/egress.go",
		},
		{
			name:    "FLOWSTATE_EGRESS_POLICY_B64",
			value:   "unset",
			purpose: "Internal grant from the plugin host to every plugin it launches: an immutable base64 encoding of the exact `--egress-policy` bytes the host already parsed, at most 64 KiB before encoding. It is a per-launch snapshot, so a policy file edited afterwards reaches the plugins the worker starts next rather than the ones already running. Operators configure the flag or `FLOWSTATE_EGRESS_POLICY`, not this variable directly; a plugin that asks the SDK for the policy or an HTTP client is refused when the grant is absent, rather than getting an ungoverned one. Set-but-empty is a grant whose policy document is empty, which is what an empty `--egress-policy` file configures; only an unset variable means nothing was granted.",
			read:    "pkg/flowstate/v1/plugin/sdk/egress.go",
		},
		{
			name:    "FLOWSTATE_TASK_POLICY",
			value:   "unset",
			purpose: "Default for `--task-policy`: a YAML task-shape policy (#187) governing which identities may dispatch which tasks. When set it replaces the built-in policy (no restriction) entirely rather than merging with it.",
			read:    "cmd/flow/taskpolicy.go",
		},
		{
			name:    "FLOWSTATE_ID_TOKEN",
			value:   "unset",
			purpose: "The OIDC ID token a GitLab CI job's `id_tokens:` keyword mints, and the variable name `--credential-source=gitlab` reads unless told otherwise. GitLab lets the job author name the key, so this is Flowstate's convention rather than the platform's; a job using another name passes it as `credentialsource.Config.EnvVar`. Set by GitLab, never by an operator.",
			read:    "pkg/flowstate/v1/credentialsource/gitlab.go",
		},
		{
			name:    "FLOWSTATE_IDENTITY_KEY",
			value:   "unset",
			purpose: "Default for `--identity-key`: the PKCS#8 PEM key Flowstate signs its own short-lived assertions with, required when the trust policy configures federation. It names one key, since a rotation names the keys in order and a list in an environment variable would need a separator; `--identity-key` on the command line replaces this default rather than adding to it.",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_INSECURE_PLAINTEXT_TOKEN",
			value:   "false",
			purpose: "Set to `true` to permit sending a bearer token over plain HTTP to somewhere that is not loopback. A refusal by default, because a token on the wire in the clear belongs to whatever is between here and there.",
			read:    "cmd/flow/credentials.go",
		},
		{
			name:    "FLOWSTATE_INTERNAL_ADDRESS",
			value:   "unset",
			purpose: "Default for `--internal-listen` on `flow server` and `flow worker`: a private socket of that process's own, carrying health and pprof. Unset (the default) means no internal listener at all; set it to a loopback address such as `127.0.0.1:9090` to turn it on. Refused unless it is loopback: it serves pprof, whose profiles carry the process's memory (resolved secret values among them), and it has no authentication or TLS configuration of its own.",
			read:    "cmd/flow/internallistener.go",
		},
		{
			name:    "FLOWSTATE_MAX_STEPS_PER_RUN",
			value:   "unset",
			purpose: "Server-side ceiling on the steps one run may submit. An unparseable or non-positive value is ignored rather than lowering the bound.",
			read:    "pkg/flowstate/v1/server/server.go",
		},
		{
			name:    "FLOWSTATE_PLUGIN_DIR",
			value:   "unset",
			purpose: "Default for `--plugin-dir`: directories to discover plugins in, separated the way `$PATH` is, the form an image bakes in rather than repeating on every command line.",
			read:    "cmd/flow/plugins.go",
		},
		{
			name:    "FLOWSTATE_PLUGIN_HOST_FD",
			value:   "unset",
			purpose: "Handshake: the descriptor a plugin watches to learn its host has gone. Set by the host on the child process; never configured by an operator.",
			read:    "pkg/flowstate/v1/plugin/sdk/sdk.go",
		},
		{
			name:    "FLOWSTATE_PLUGIN_MAGIC_COOKIE",
			value:   "unset",
			purpose: "Handshake: refuses to serve a plugin protocol to a process that did not mean to launch one. Set by the host on the child process.",
			read:    "pkg/flowstate/v1/plugin/sdk/sdk.go",
		},
		{
			name:    "FLOWSTATE_PLUGIN_PINS",
			value:   "unset",
			purpose: "Default for `--plugin-pins`: a YAML file mapping plugin names to the digest the binary answering to each must have, merged with any --plugin-pin (#1010). Unset means no pins file; a deployment with neither this nor --plugin-pin configures no digest pins, and every plugin name launches exactly as it always has.",
			read:    "cmd/flow/plugins.go",
		},
		{
			name:    "FLOWSTATE_PLUGIN_PROTOCOL_VERSIONS",
			value:   "unset",
			purpose: "Handshake: the protocol versions the host offers. Set by the host on the child process.",
			read:    "pkg/flowstate/v1/plugin/sdk/sdk.go",
		},
		{
			name:    "FLOWSTATE_PLUGIN_SOCKET",
			value:   "unset",
			purpose: "Handshake: the socket path a plugin serves on. Set by the host on the child process.",
			read:    "pkg/flowstate/v1/plugin/sdk/sdk.go",
		},
		{
			name:    "FLOWSTATE_PLUGIN_TOKEN",
			value:   "unset",
			purpose: "Retired: it carried the per-launch token up to plugin protocol version 3, and nothing sets or reads it now (#1336). The name stays reserved, so a plugin still does not see it if a deployment sets it.",
			read:    "pkg/flowstate/v1/plugin/launch.go",
		},
		{
			name:    "FLOWSTATE_PLUGIN_TOKEN_FD",
			value:   "unset",
			purpose: "Handshake: the descriptor carrying the per-launch token a plugin authenticates its host with. Set by the host on the child process; never configured by an operator. The token itself is never in the environment, because /proc/<pid>/environ would then expose it for the plugin's whole life (#1336).",
			read:    "pkg/flowstate/v1/plugin/sdk/sdk.go",
		},
		{
			name:    "FLOWSTATE_PROTECTED_RESOURCE",
			value:   "unset",
			purpose: "Default for `--protected-resource` on `flow server`: the canonical resource URI (RFC 8707 section 2) this deployment's MCP surface identifies as. Given together with one or more `--authorization-server`, this deployment serves RFC 9728 protected resource metadata and every 401 challenge names it. Unset: the route does not exist, and every challenge reads exactly as it does without this slice.",
			read:    "cmd/flow/protectedresource.go",
		},
		{
			name:    "FLOWSTATE_RPC_RESOURCE",
			value:   "unset",
			purpose: "Default for `--rpc-resource` on `flow server`: the canonical Connect RPC resource URI required in every bearer token's `aud` claim. Required whenever `--auth-policy` trusts a `kind: oidc` issuer, unless the migration-only `--allow-issuer-wide-audiences` flag explicitly restores the older issuer-wide behavior; a policy of nothing but `kind: mtls` entries mints no token to bind and needs neither flag. Distinct from the remote MCP protected resource and from any future HTTP surface.",
			read:    "cmd/flow/rpcresource.go",
		},
		{
			name:    "FLOWSTATE_SECRET_COMMAND",
			value:   "unset",
			purpose: "Default for `--secret-command`: the argv of the command that resolves `command:` secrets, `$PATH`-list-separated (the executable first). `{{name}}` and, with `--secret-command-namespaced`, `{{namespace}}` are substituted literally into one argument, never through a shell.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_DIR",
			value:   "unset",
			purpose: "Default for `--secret-dir`: the directory `file:` secrets are read from.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_ENV_ALLOW",
			value:   "unset",
			purpose: "Default for `--secret-env`: comma-separated names this process may resolve as `env:` secrets.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_KEYCHAIN",
			value:   "unset",
			purpose: "Default for `--secret-keychain`: set to resolve `keychain:` secrets from the macOS keychain. Refused at startup, with a message naming the platform, on any OS other than macOS.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_KEYCHAIN_SERVICE",
			value:   "unset",
			purpose: "Default for `--secret-keychain-service`: the keychain service name entries are stored under, in place of the built-in default.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_OP",
			value:   "unset",
			purpose: "Default for `--secret-op`: set to resolve `op:` secrets through the 1Password CLI. Refused at startup when the `op` CLI is not on `PATH`.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_OP_VAULT",
			value:   "unset",
			purpose: "Default for `--secret-op-vault`: the 1Password vault read when a run has no namespace, in place of the built-in default.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_ADDR",
			value:   "unset",
			purpose: "Default for `--secret-vault-addr`: the address of the Vault or OpenBao instance `vault:` secrets are read from. Unset, no vault provider is registered.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_CA_FILE",
			value:   "unset",
			purpose: "Default for `--secret-vault-ca-file`: a PEM CA bundle to verify the vault's certificate against, instead of the system roots.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_KUBERNETES_MOUNT",
			value:   "unset",
			purpose: "Default for `--secret-vault-kubernetes-mount`: where the Kubernetes auth method is mounted, in place of the built-in default.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_KUBERNETES_ROLE",
			value:   "unset",
			purpose: "Default for `--secret-vault-kubernetes-role`: the Vault role to authenticate as via the Kubernetes auth method, using this pod's projected service account token. Exactly one of this or a token must be configured.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_MOUNT",
			value:   "unset",
			purpose: "Default for `--secret-vault-mount`: where the KV v2 engine is mounted, in place of the built-in default.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_NAMESPACE",
			value:   "unset",
			purpose: "Default for `--secret-vault-namespace`: the Vault Enterprise or OpenBao namespace header. This is the vault's own namespace, not the tenant namespace a run authenticates with.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_PATH_PREFIX",
			value:   "unset",
			purpose: "Default for `--secret-vault-path-prefix`: a path prefix inside the mount, above the namespace segment.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_TOKEN",
			value:   "unset",
			purpose: "A static Vault client token, read directly when `--secret-vault-token-file` (and `$FLOWSTATE_SECRET_VAULT_TOKEN_FILE`) is unset. For a development vault or a test; a long-running worker should prefer the file form or Kubernetes auth, since this one cannot be rotated without a restart.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_VAULT_TOKEN_FILE",
			value:   "unset",
			purpose: "Default for `--secret-vault-token-file`: a file holding a static Vault client token, re-read on every login so a rotated token is picked up without a restart.",
			read:    "cmd/flow/secrets.go",
		},
		{
			name:    "FLOWSTATE_SECRET_<NAME>",
			value:   "unset",
			purpose: "The value of an `env:` secret. Read only for a name the allowlist carries, and only inside the activity that applies it: the reference is what travels, never the value.",
			read:    "pkg/flowstate/v1/secrets/env.go",
			family:  true,
		},
		{
			name:    "FLOWSTATE_SYMBOLS",
			value:   "unset",
			purpose: "Override symbol selection (`unicode`/`ascii`) when terminal detection guesses wrong.",
			read:    "cmd/flow/internal/ui/ui.go",
		},
		{
			name:    "FLOWSTATE_TLS_ACME_ACCEPT_TOS",
			value:   "unset",
			purpose: "Default for `--tls-acme-accept-tos` on `flow server`: set (to anything) to agree to the ACME CA's subscriber agreement. Required to turn ACME automatic-certificate issuance on; not defaulted, because agreeing to a third party's terms on an operator's behalf is not this process's decision to make quietly.",
			read:    "cmd/flow/acme.go",
		},
		{
			name:    "FLOWSTATE_TLS_ACME_CACHE",
			value:   "unset",
			purpose: "Default for `--tls-acme-cache` on `flow server`: the directory holding the ACME account key and issued certificates. Required when ACME is configured — an in-memory-only cache re-issues on every restart, which burns the CA's rate limit. Created with mode 0700 if missing, and refused if it exists but is readable or writable by anyone but its owner.",
			read:    "cmd/flow/acme.go",
		},
		{
			name:    "FLOWSTATE_TLS_ACME_DIRECTORY",
			value:   "unset",
			purpose: "Default for `--tls-acme-directory` on `flow server`: the ACME directory URL to request certificates from. Unset means Let's Encrypt's production directory; point this at a staging or private directory (Pebble, an enterprise ACME server) for anything other than a real production certificate.",
			read:    "cmd/flow/acme.go",
		},
		{
			name:    "FLOWSTATE_TLS_ACME_EMAIL",
			value:   "unset",
			purpose: "Default for `--tls-acme-email` on `flow server`: a contact email the ACME CA may use to warn about a problem with an issued certificate. Optional.",
			read:    "cmd/flow/acme.go",
		},
		{
			name:    "FLOWSTATE_TLS_ACME_HOSTS",
			value:   "unset",
			purpose: "Default for `--tls-acme-hosts` on `flow server`: comma-separated public DNS host(s) to obtain a certificate for automatically via ACME's TLS-ALPN-01 challenge. Required to turn ACME on, and the whole of what a certificate may be obtained for — refused empty rather than defaulting to issuing for whatever SNI a caller sends. Mutually exclusive with the explicit certificate flags and `--tls-terminated-upstream`, and refused together with `--internal-listen`.",
			read:    "cmd/flow/acme.go",
		},
		{
			name:    "FLOWSTATE_TLS_CA_FILE",
			value:   "unset",
			purpose: "Default for `--tls-ca-file` on the client commands: a PEM CA bundle to verify the server's certificate against, in place of the system roots. Unset trusts the system roots.",
			read:    "cmd/flow/clientcert.go",
		},
		{
			name:    "FLOWSTATE_TLS_CERT_FILE",
			value:   "unset",
			purpose: "Default for `--tls-cert-file` on `flow server`: a PEM certificate (or chain) for the public listener. Unset serves plain HTTP, refused unless the listen address is loopback. Must be given with `FLOWSTATE_TLS_KEY_FILE`.",
			read:    "cmd/flow/tls.go",
		},
		{
			name:    "FLOWSTATE_TLS_CLIENT_AUTH",
			value:   "off",
			purpose: "Default for `--tls-client-auth` on `flow server`: `off` or `require`. `require` makes the public listener refuse a handshake with no client certificate, or one that does not chain to a `kind: mtls` issuer entry's `client_ca_file` in `--auth-policy` — there is no separate CA flag. Only these two values are ever offered.",
			read:    "cmd/flow/mtls.go",
		},
		{
			name:    "FLOWSTATE_TLS_CLIENT_AUTH_IDENTITY",
			value:   "unset",
			purpose: "Default for `--tls-client-auth-identity` on `flow server`: set (to anything) to also authenticate the caller from a verified client certificate, through the `kind: mtls` trust policy entry that admitted it. Requires `FLOWSTATE_TLS_CLIENT_AUTH=require`; without it a required certificate is a connection-level fence only and a caller still needs a bearer token.",
			read:    "cmd/flow/mtls.go",
		},
		{
			name:    "FLOWSTATE_TLS_CLIENT_CERT_FILE",
			value:   "unset",
			purpose: "Default for `--tls-client-cert-file` on the client commands: a PEM client certificate to present to a server started with `--tls-client-auth require`. Must be given with `FLOWSTATE_TLS_CLIENT_KEY_FILE`. Unset presents no certificate, which such a server refuses at the handshake.",
			read:    "cmd/flow/clientcert.go",
		},
		{
			name:    "FLOWSTATE_TLS_CLIENT_KEY_FILE",
			value:   "unset",
			purpose: "Default for `--tls-client-key-file` on the client commands: the PEM private key matching `FLOWSTATE_TLS_CLIENT_CERT_FILE`.",
			read:    "cmd/flow/clientcert.go",
		},
		{
			name:    "FLOWSTATE_TLS_KEY_FILE",
			value:   "unset",
			purpose: "Default for `--tls-key-file` on `flow server`: the PEM private key matching `FLOWSTATE_TLS_CERT_FILE`.",
			read:    "cmd/flow/tls.go",
		},
		{
			name:    "FLOWSTATE_TLS_MIN_VERSION",
			value:   "1.2",
			purpose: "Default for `--tls-min-version` on `flow server`: the minimum TLS protocol version to accept, `1.2` or `1.3`. Nothing below 1.2 is offered.",
			read:    "cmd/flow/tls.go",
		},
		{
			name:    "FLOWSTATE_TLS_TERMINATED_UPSTREAM",
			value:   "unset",
			purpose: "Default for `--tls-terminated-upstream` on `flow server`: set (to anything) to permit the public listener to serve plain HTTP on a non-loopback address with no certificate configured. A refusal by default; set this only when something in front of this process — a reverse proxy, a Kubernetes Ingress, a load balancer, a container's published-port binding — already terminates TLS or bounds who can reach this address. Never a substitute for a certificate when nothing actually stands in front of this process.",
			read:    "cmd/flow/tls.go",
		},
		{
			name:    "FLOWSTATE_TOKEN",
			value:   "unset",
			purpose: "Bearer token the client authenticates with, used when no token file is set.",
			read:    "cmd/flow/credentials.go",
		},
		{
			name:    "FLOWSTATE_TOKEN_FILE",
			value:   "unset",
			purpose: "Default for `--token-file`: a file holding the bearer token, re-read per request so a rotated token is picked up without a restart.",
			read:    "cmd/flow/client.go",
		},
		{
			name:    "FLOWSTATE_VERBOSE_LOGGING",
			value:   "false",
			purpose: "Default for `--verbose`. Read as a condition rather than as a string, so it does not appear as a flag default in the CLI reference.",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_WORKER_IDENTITY",
			value:   "unset",
			purpose: "Default for `--identity`: how this worker identifies itself to Temporal, shown in Event History and a Task Queue's poller list (#752). Unset builds one from `--deployment-name`/`--build-id`, `--tenant` if set, and this process's hostname — more specific than the SDK's own `pid@hostname` default, but a platform-native identifier (a Kubernetes pod name, an ECS task id) is worth setting explicitly.",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_WORKER_MAX_ACTIVITIES_PER_SECOND",
			value:   "0",
			purpose: "Default for `--max-activities-per-second` on `flow worker`: maximum rate, per second, at which this worker process starts activity tasks. `0` takes the Temporal SDK's own default (effectively unlimited). Enforced locally, per worker process — see `FLOWSTATE_WORKER_TASK_QUEUE_ACTIVITIES_PER_SECOND` for the server-enforced, per-queue limit. A negative value refuses to start (#783).",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_WORKER_MAX_CONCURRENT_ACTIVITIES",
			value:   "0",
			purpose: "Default for `--max-concurrent-activities` on `flow worker`: maximum number of activity tasks executing at once in this process. `0` takes the Temporal SDK's own default (1000). Raising this trades worker CPU/memory for throughput on a single replica rather than scaling out — see docs/DEPLOYMENT.md's capacity section. A negative value refuses to start (#783).",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_WORKER_MAX_CONCURRENT_WORKFLOW_TASKS",
			value:   "0",
			purpose: "Default for `--max-concurrent-workflow-tasks` on `flow worker`: maximum number of workflow tasks executing at once in this process. `0` takes the Temporal SDK's own default (1000). The value `1` refuses to start: the Temporal SDK panics on it, because a worker with a single workflow-task slot never polls its regular queue (#783).",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_WORKER_TASK_QUEUE_ACTIVITIES_PER_SECOND",
			value:   "0",
			purpose: "Default for `--task-queue-activities-per-second` on `flow worker`: maximum rate, per second, at which the Temporal server dispatches activity tasks from this worker's task queue, shared across every worker polling that queue (last-writer-wins if they disagree). `0` takes the Temporal SDK's own default (effectively unlimited); setting it disables eager activity execution for this worker. A negative value refuses to start (#783).",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_WORKER_STICKY_CACHE_SIZE",
			value:   "0",
			purpose: "Default for `--sticky-cache-size` on `flow worker`: maximum number of workflow executions kept in this process's sticky cache. Unlike the other four `FLOWSTATE_WORKER_*` capacity variables, `0` does NOT take the Temporal SDK's own default (10000) by being passed through — `worker.SetStickyWorkflowCacheSize` assigns its argument unconditionally, so `0` reaching it would configure a zero-entry cache and force full history replay on every workflow task. `0` (or unset) is implemented by not calling the setter at all — see docs/DEPLOYMENT.md's capacity section and workerCapacity's doc comment in cmd/flow/main.go (#921).",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "FLOWSTATE_WORKER_STOP_TIMEOUT",
			value:   "2m0s",
			purpose: "Default for `--worker-stop-timeout` on `flow worker`: how long a shutdown (SIGINT or SIGTERM) waits for in-flight activities and workflow tasks to finish before the worker exits regardless. Parsed with v1.ParseDuration, the same grammar the DSL itself accepts (Go's duration syntax plus days); an unparsable value refuses to start rather than silently keep the default. Keep it under whatever grace period the deployment shape actually gives the process — see docs/DEPLOYMENT.md.",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "GITLAB_CI",
			value:   "unset",
			purpose: "Set to `true` inside every GitLab CI job. Read by the `gitlab` credential source only to tell \"this is not a GitLab job\" apart from \"it is, and the job declares no ID token\" — two mistakes with different fixes. Never configured by an operator.",
			read:    "pkg/flowstate/v1/credentialsource/gitlab.go",
		},
		{
			name:    "OTEL_EXPORTER_OTLP_ENDPOINT",
			value:   "unset",
			purpose: "The fallback OTLP endpoint for every enabled signal. A signal-specific endpoint overrides it; its signal's `OTEL_*_EXPORTER=none` disables that signal even when this is set.",
			read:    "cmd/flow/telemetry.go, cmd/flow/serverdev.go",
		},
		{
			name:    "OTEL_TRACES_EXPORTER",
			value:   "unset",
			purpose: "Select the trace exporter (`otlp`) or disable trace export (`none`), matched case-insensitively. When unset, a general or trace-specific OTLP endpoint enables it. Propagation remains available for enabled metrics or correlated logs even when trace export is disabled.",
			read:    "cmd/flow/telemetry.go",
		},
		{
			name:    "OTEL_METRICS_EXPORTER",
			value:   "unset",
			purpose: "Select the metrics exporter (`otlp`) or disable metrics (`none`), matched case-insensitively. When unset, a general or metrics-specific OTLP endpoint enables it.",
			read:    "cmd/flow/telemetry.go",
		},
		{
			name:    "OTEL_LOGS_EXPORTER",
			value:   "unset",
			purpose: "Select the log exporter (`otlp`) or disable log export (`none`), matched case-insensitively. When unset, a general or logs-specific OTLP endpoint enables it; stderr logging is never disabled.",
			read:    "cmd/flow/telemetry.go",
		},
		{
			name:    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
			value:   "unset",
			purpose: "The same, for a deployment sending logs somewhere different. It enables logs on its own, unless `OTEL_LOGS_EXPORTER=none` disables that signal. Logs are exported through the OTLP log exporter beside stderr, never instead of it, so a collector is a destination gained, not exchanged.",
			read:    "cmd/flow/telemetry.go, cmd/flow/serverdev.go",
		},
		{
			name:    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
			value:   "unset",
			purpose: "The same, for a deployment sending metrics somewhere different. It enables metrics on its own, unless `OTEL_METRICS_EXPORTER=none` disables that signal.",
			read:    "cmd/flow/telemetry.go, cmd/flow/serverdev.go",
		},
		{
			name:    "OTEL_EXPORTER_OTLP_*",
			value:   "—",
			purpose: "Headers, protocol, timeouts and the rest: read by the OTLP exporters themselves rather than re-spelled here, so anything else OTLP-speaking is configured the same way.",
			read:    "go.opentelemetry.io/otel exporters",
			family:  true,
		},
		{
			name:  "OTEL_TRACES_SAMPLER",
			value: "parentbased_always_on",
			purpose: "Select the head sampler (`always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, " +
				"`parentbased_always_off`, `parentbased_traceidratio`, and the jaeger_remote and xray variants the " +
				"SDK also accepts), matched per the OTel spec. `flow` never calls `sdktrace.WithSampler`, so the " +
				"tracer provider is built with none configured and the SDK reads this variable itself. Set it on " +
				"the worker: sampling is a head decision made where the root span starts, and every child span " +
				"follows the parent's decision through `parentbased_*` regardless of what this variable says on " +
				"the process that created the child. This shapes what gets exported, not the span, metrics, or " +
				"logs pipeline itself — a tail-based decision (keep every trace touching an error, regardless of " +
				"its head sampling) belongs in a collector, not here.",
			read: "go.opentelemetry.io/otel/sdk/trace (consulted only when WithSampler is absent)",
		},
		{
			name:  "OTEL_TRACES_SAMPLER_ARG",
			value: "unset",
			purpose: "The argument the selected sampler takes: a ratio in `[0,1]` for `traceidratio` and " +
				"`parentbased_traceidratio`, a remote-sampler endpoint for `jaeger_remote`, ignored by every " +
				"other sampler. Meaningless without `OTEL_TRACES_SAMPLER` naming a sampler that reads it.",
			read: "go.opentelemetry.io/otel/sdk/trace (consulted only when WithSampler is absent)",
		},
		{
			name:  "FLOWSTATE_TASK_QUEUE_PREFIX",
			value: "unset",
			purpose: "Default for `--task-queue-prefix` on both `flow server` and `flow worker`: route each " +
				"tenant's runs to a task queue named `<prefix>_<namespace>` instead of the single shared " +
				"queue. Unset routes nothing, which is the zero-configuration behaviour. It has to be the " +
				"same value on the server and on every worker, because a worker that spelled it differently " +
				"would poll a queue nothing submits to, which is why one variable is the convenient way to " +
				"set it. A worker also needs `--tenant` to say which of those queues is its own.",
			read: "cmd/flow/main.go",
		},
		{
			name:  "TEMPORAL_ADDRESS",
			value: "unset",
			purpose: "Temporal's own environment configuration, honoured by every command that dials a " +
				"cluster: `flow server` and `flow worker` resolve it through the SDK, and " +
				"`--temporal-address` overrides it (`--address` on those two commands is refused, and " +
				"says so — picatz/flowstate#580). `flow server dev` is the exception, and refuses to " +
				"start while it is set: " +
				"that command starts a Temporal of its own, so a variable naming somebody else's cluster " +
				"would be silently unused while its operator believed their runs were landing there.",
			read: "cmd/flow/serverdev.go, go.temporal.io/sdk envconfig",
		},
		{
			name:  "TEMPORAL_PROFILE",
			value: "unset",
			purpose: "Selects a profile from the same `temporal.toml` the `temporal` CLI reads, which is " +
				"how one binary moves between a laptop, a self-hosted cluster and Cloud without a scheme " +
				"invented here. Refused by `flow server dev` for the same reason as `TEMPORAL_ADDRESS`.",
			read: "cmd/flow/serverdev.go, go.temporal.io/sdk envconfig",
		},
		{
			name:  "TEMPORAL_CONFIG_FILE",
			value: "unset",
			purpose: "Path to the TOML configuration file the profile is read from, honoured by the SDK's " +
				"environment configuration wherever a cluster is dialed. Refused by `flow server dev` for " +
				"the same reason as `TEMPORAL_ADDRESS`: an explicit file pointing at another cluster would " +
				"be the same silent misrouting through a different spelling.",
			read: "cmd/flow/serverdev.go, go.temporal.io/sdk envconfig",
		},
		{
			name:    "TEMPORAL_TASK_QUEUE",
			value:   "flowstate-run-task-queue",
			purpose: "Default for `--task-queue`: the queue workers serve and workflows are routed to.",
			read:    "cmd/flow/main.go",
		},
		{
			name:    "TFC_RUN_ID",
			value:   "unset",
			purpose: "Set by HCP Terraform in every run. Read by the `terraform-cloud` credential source only to tell \"this is not an HCP Terraform run\" apart from \"it is, and the workspace set no workload identity audience\". Never configured by an operator.",
			read:    "pkg/flowstate/v1/credentialsource/terraform_cloud.go",
		},
		{
			name:    "TFC_WORKLOAD_IDENTITY_AUDIENCE[_<TAG>]",
			value:   "unset",
			purpose: "The workspace variable an operator sets to make HCP Terraform mint a workload identity token for a run, naming the relying party — the Flowstate server. The tagged form mints a second token for another relying party. Set on the workspace, not in the run's environment, so nothing here reads it; it is named in the `terraform-cloud` source's diagnostics because it is the setting missing when the token is.",
			read:    "pkg/flowstate/v1/credentialsource/terraform_cloud.go",
			family:  true,
		},
		{
			name:    "TFC_WORKLOAD_IDENTITY_TOKEN[_<TAG>]",
			value:   "unset",
			purpose: "The workload identity token HCP Terraform gives a run whose workspace set the audience variable above, and what `--credential-source=terraform-cloud` presents. The tagged form carries the token for a tagged audience. Set by HCP Terraform, never by an operator.",
			read:    "pkg/flowstate/v1/credentialsource/terraform_cloud.go",
			family:  true,
		},
	}
}

// renderEnvVarReference documents every variable this build reads.
//
// The table VISION names as the proven drifter — it shipped ten variables short
// — so it is the one with the most machinery behind it. The prose is written by
// hand and the *set* is not: see [documentedEnvironmentVariables].
func (g *Generator) renderEnvVarReference() string {
	var b strings.Builder

	b.WriteString(generatedNotice + "\n\n")
	b.WriteString("# Environment variable reference\n\n")
	b.WriteString("Every variable this build reads. There is no single registration point to derive\n")
	b.WriteString("this from (a variable is read where it is needed), so the prose is written by\n")
	b.WriteString("hand in `cmd/flow/internal/docsgen/envvars.go` and the *set* is enforced: a test\n")
	b.WriteString("parses every non-test file under `cmd/` and `pkg/`, collects the variable names\n")
	b.WriteString("and the `os.Getenv`/`os.LookupEnv` call sites, and fails on a read this table\n")
	b.WriteString("does not carry or an entry nothing reads any more.\n\n")
	b.WriteString("Where a variable is the default of a flag, the flag wins when both are given.\n")
	b.WriteString("[cli.md](cli.md) says which flag each one feeds.\n\n")

	b.WriteString("| Variable | Default | Purpose | Read in |\n|---|---|---|---|\n")
	for _, variable := range g.documentedEnvironmentVariables() {
		fmt.Fprintf(&b, "| `%s` | %s | %s | `%s` |\n",
			cell(variable.name), orDash(codeOrEmpty(variable.value)), cell(variable.purpose), cell(variable.read))
	}

	return b.String()
}
