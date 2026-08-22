// Package credentialsource turns an environment into a credential.
//
// # The hole this closes
//
// Flowstate's server verifies bearer tokens from a trust policy — OIDC, workload
// identity federation, per-tenant namespaces, the lot — and until this package
// existed nothing in the shipped tree acquired one. The only code that turned an
// ambient workload identity (a GitHub Actions job's OIDC token endpoint, a
// Kubernetes projected service account file, a plain environment variable) into a
// token presentable to a Flowstate server was a test helper
// (auth/realtoken_test.go's requestCIToken). Every real deployment therefore hand
// wrote a dozen lines of curl against $ACTIONS_ID_TOKEN_REQUEST_URL, piped the
// result through jq into a file, and hoped nobody logged it.
//
// A [Source] is the fix: a named, pluggable thing a client asks for a token,
// which knows how to reach one ambient identity and re-mints it before it expires.
// [flowstate/v1/auth.Broker] already has this shape on the outbound side — mint,
// cache, refresh shortly before expiry — and this package is the same idea
// pointed inward, at acquiring the credential a caller presents to Flowstate
// itself rather than one Flowstate presents onward.
//
// # Sources
//
// [SourceGitHubActions] asks the GitHub Actions runner's OIDC token endpoint
// (ACTIONS_ID_TOKEN_REQUEST_URL / ACTIONS_ID_TOKEN_REQUEST_TOKEN, present only in
// a job granted `id-token: write`) for a token addressed to a given audience, and
// re-mints it once the cached one is within its refresh margin of expiring — read
// from the token's own "exp" claim, unverified, purely to schedule the next mint.
// Nothing here decides whether the token is trustworthy; that is the server's
// OIDCVerifier's job, against its own trust policy, when the token arrives.
//
// [SourceGitLab] and [SourceTerraformCloud] are the other shape a CI platform's
// OIDC integration takes: there is no endpoint to ask. GitLab mints an ID token
// before the job starts, for the audience the job's `id_tokens:` keyword names,
// and puts it in an environment variable the job author named; HCP Terraform
// does the same for a run whose workspace sets TFC_WORKLOAD_IDENTITY_AUDIENCE.
// Neither can be asked for a second token addressed anywhere else, so those
// Sources read, check and refuse rather than mint: an expired token, a token
// that is not a JWT, or a token addressed to a relying party other than the one
// the caller named is a refusal carrying the job-configuration key to change,
// instead of an "unauthenticated" from the server that could equally mean a
// wrong trust policy. The audience comparison, like the expiry, is an
// unverified decode used for scheduling and diagnostics only — never for trust.
//
// [SourceFile] and [SourceEnv] are the plain case: a token already sitting in a
// file or an environment variable, read fresh on every call rather than cached.
// The file form matters most in practice, because it is the shape a rotating
// credential actually arrives in — Kubernetes rewrites a projected service
// account token in place, and reading it fresh on every call is what makes that
// keep working without this package or its caller ever needing to notice.
//
// # Fail closed
//
// A [Source] obtained by name through [Resolve] is a source a caller explicitly
// asked for, so [Source.Token] returning no error and an empty token is not a
// spelling this package uses anywhere: every Source here returns a usable token
// or a wrapped [ErrSourceUnusable], never a silent fall-back to anonymous. An
// unknown or not-yet-implemented name is a construction-time [ErrUnknownSource].
// Whether "nobody asked for a credential at all" is itself acceptable — the
// anonymous case — is a policy the caller owns; this package never decides it.
//
// # Secrets never leave through the front door
//
// [Token] holds its bearer value in an [flowstate/v1/auth.Material], the same
// closure-backed type every other secret-carrying value in this codebase uses,
// so it renders as "[redacted]" under every fmt verb, through any container, at
// any depth, and drops the value entirely when serialized. See
// [flowstate/v1/auth.Material] for why a closure and not a field is what makes
// that hold through reflection.
package credentialsource
