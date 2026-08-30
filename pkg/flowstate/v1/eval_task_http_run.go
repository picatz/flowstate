package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/interpreter"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// idempotentMethods are the methods RFC 9110 defines as idempotent: repeating one
// has the same effect on the server as making it once.
//
// POST and PATCH are absent deliberately. They are the methods for which a repeat
// is a second effect.
var idempotentMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodHead:    true,
	http.MethodPut:     true,
	http.MethodDelete:  true,
	http.MethodOptions: true,
	http.MethodTrace:   true,
}

// retriableTransportFailure reports whether a request that failed without a
// response can be attempted again.
//
// A transport failure is not one situation but two, and they differ in the only way
// that matters. If the connection was never established, the request cannot have
// taken effect, and retrying it is safe whatever the method. If the request was
// written and then the transport failed — a timeout awaiting the response, a reset
// mid-flight — the outcome is *unknown*: the server may have completed the work and
// failed only to tell us. Retrying that is not retrying a failure, it is performing
// the operation a second time.
//
// For an idempotent method that is fine by definition. For a POST or a PATCH it is
// how one charge becomes two, so an unknown outcome is reported as permanent and the
// author decides what to do about it. That matches what ErrorKind.Retryable already
// documents as the intent — "retrying a POST that already took effect is worse than
// surfacing a failure that might have resolved on its own" — which the transport
// path previously did not honor, because it classified by transport-versus-policy and
// never looked at the method.
func retriableTransportFailure(method string, err error) bool {
	if idempotentMethods[strings.ToUpper(method)] {
		return true
	}

	// A dial failure means no bytes reached the server: connection refused, no
	// route, DNS failure. Nothing can have happened, so this stays retriable even
	// for a non-idempotent method — which keeps the common "the server is not up
	// yet" case working.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}

	return false
}

// firstHeaderValues flattens response headers to one value per name, which is
// the shape the schema declares and the shape a workflow author expects when
// writing ${steps.<id>.headers['Content-Type']}.
//
// Repeated headers keep their first value. Expressions needing every value of a
// repeated header can reach them through the outputs input, where headers are
// exposed as lists.
func firstHeaderValues(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for name, values := range h {
		if len(values) > 0 {
			out[name] = values[0]
		}
	}
	return out
}

// httpResponseEnv returns the CEL environment the http task's own expressions —
// `outputs:` and `expect:` — are evaluated in.
//
// The profile's environment plus the response root, and the profile part is a fix
// rather than a flourish. This used to be `cel.NewEnv(response, jsonLibrary())`: the
// json library and nothing else. So `${vars.greeting.upperAscii()}` worked in a
// `vars:` binding, in an `if:`, in `items:`, in `wait_until:` and in every other task
// input, and failed inside `outputs:` — after the request had already been made,
// since that is when the expression runs.
//
// That is the exact defect `libs:` was retired to end. A step could once name its own
// extension libraries and nothing else in the file could, so one step spoke a richer
// dialect than the rest; the profile replaced it with a single membership. This was
// the same split from the other side — one position speaking a *poorer* dialect than
// the rest — and it survived the retirement because it is not spelled anywhere in the
// grammar. One dialect means these two positions as well.
//
// Found by a type checker disagreeing with the runtime: the validator judged these
// expressions against the profile, which was right, against a runtime that was not.
//
// # The example above needs both halves
//
// `${vars.greeting.upperAscii()}` names a function and a variable, and fixing the
// environment fixed only the function. The *activation* is where the variable comes
// from, and both callers built one by hand over the step outputs alone — so the same
// expression kept failing here, now saying `no such key: greeting` about a variable
// declared at the top of the file. Worse than the unbound name it replaced: an empty
// `vars` root answers by design, so that `vars.missing` reads as a missing key rather
// than as the namespace not existing, and with nothing in it every var read that way.
//
// One dialect is a claim about names as much as functions, and a position gets both
// from the scope or it is still a dialect of its own. See [Scope.Activation].
//
// # The profile is the run's, not the build's
//
// Taken from the scope rather than from [CurrentProfile], because this evaluates on
// whichever worker picked the activity up, and that worker's build may know a
// different set of libraries than the one that compiled the spec. The whole point of
// recording a profile is that the run's vocabulary travels with it — resolving it
// here from a package constant would be the bug invariant 10 exists to prevent, in
// the one place the scope is already carrying the answer.
func httpResponseEnv(profile string) (*cel.Env, error) {
	libs, err := ProfileLibraries(profile)
	if err != nil {
		return nil, err
	}

	base, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil, err
	}

	// One variable, and everything about the response hangs from it.
	//
	// These names are *system-chosen* and injected into an author's namespace,
	// which is the shape the signal-payload fix already rooted under `payload.*`
	// — for two reasons that apply identically here. The set will grow: a future
	// `duration_ms` written bare would capture a binding somebody already had.
	// And the collision is representable today: `as: body` on a loop enclosing an
	// http step whose `expect:` says `body` reads the response, not the item, and
	// nothing in the file says so.
	//
	// Dynamically typed because a root answered whole cannot be given a field-wise
	// type here without restating the response's shape in a second place.
	env, err := base.Extend(cel.Variable(ResponseRoot, cel.DynType))
	if err != nil {
		return nil, fmt.Errorf("create HTTP outputs CEL environment: %w", err)
	}

	return env, nil
}

// revealSecret returns the [revealFunc] the http task resolves references
// through: authorized and resolved from the activity's own execution context, and
// registered with the scrubber so a peer reflecting the value back cannot put it
// into an output.
//
// The context is captured rather than threaded through every converter, and that is
// deliberate. What travels is a function, so nothing between here and the header or
// the encoded body holds a resolved value in a field, a map, or a struct — the
// arrangement that survives `%+v` on whatever happens to hold it.
func revealSecret(ctx context.Context, scrubber *secrets.Scrubber) revealFunc {
	return func(ref *SecretRef) (string, error) {
		secret, err := ResolveSecret(ctx, ref)
		if err != nil {
			return "", &secretResolutionError{err: fmt.Errorf(
				"resolving reference %s: %w", secretRefText(ref), err)}
		}
		scrubber.Add(secret)

		return secret.Reveal(), nil
	}
}

// httpInputError classifies a failure to build the request from its inputs.
//
// A reference the policy refused is a denial and will be refused again; a
// malformed input is the file's mistake. Both used to be reported as invalid
// input, which made a policy decision look like a typo and — worse — made it
// retryable in exactly the cases retrying cannot help.
func httpInputError(err error) error {
	var resolution *secretResolutionError
	if errors.As(err, &resolution) {
		kind := ErrorKindPolicyDenied
		if secrets.Retryable(err) {
			kind = ErrorKindUpstream
		}

		return NewTaskError("http", kind, err)
	}

	return NewTaskError("http", ErrorKindInvalidInput, err)
}

// httpPreparedInputs is the request-independent front of the http task: the
// deferred and structure-held inputs split out, the rest populated into the
// schema message, the `outputs` specification normalized into the field or the
// whole-map expression, and the schema's constraints enforced. One function
// with two callers — [taskFuncHTTP], which then performs the request, and
// [httpStubResponseFn], which is handed the response by a test harness — so
// the two paths cannot disagree about what the inputs mean (#925).
func httpPreparedInputs(ctx context.Context, input map[string]*Value, scope *Scope) (taskInputs *Task_HTTP_Inputs, outputsExpr *expr.ParsedExpr, expectSpec, headersSpec *Value, err error) {
	taskInputs = &Task_HTTP_Inputs{
		Method: proto.String(http.MethodGet),
	}

	// `outputs` and `expect` are evaluated against the response rather than
	// against earlier steps, so they are held back from population: resolving
	// them here would fail on `status_code` and `body`, which do not exist yet.
	//
	// `headers` is held back for a different reason and only when it is written
	// as a structure. The schema's field is map<string, string> and a reference
	// is not a string, so a structure carrying one has nowhere to be put down —
	// and putting the *resolved* value there would be worse than the error it
	// avoids, since a proto struct field is exactly what `fmt` reflects into.
	// It stays a Value until [httpRequestHeaders] hands it to the request.
	var outputsSpec *Value
	inputForPopulate := input
	if _, hasOutputs := input["outputs"]; hasOutputs || input["expect"] != nil ||
		input["headers"].GetStructure() != nil {
		outputsSpec, expectSpec = input["outputs"], input["expect"]
		if input["headers"].GetStructure() != nil {
			headersSpec = input["headers"]
		}
		inputForPopulate = make(map[string]*Value, len(input))
		for k, v := range input {
			if k == "outputs" || k == "expect" {
				continue
			}
			if k == "headers" && headersSpec != nil {
				continue
			}
			inputForPopulate[k] = v
		}
	}

	if err := populateProtoMessageFromValueMap(ctx, inputForPopulate, taskInputs, scope); err != nil {
		return nil, nil, nil, nil, NewTaskError("http", ErrorKindInvalidInput, err)
	}

	if outputsSpec != nil {
		switch kind := outputsSpec.GetKind().(type) {
		case *Value_Literal:
			converted, err := literalToValueMap(kind.Literal)
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("invalid outputs literal: %w", err)
			}
			if len(converted) > 0 {
				taskInputs.Outputs = converted
			}
		case *Value_Structure_:
			// The mapping form, compiled entry by entry so its names survive
			// into the specification. Each entry lands in the same field a
			// literal map lands in and is evaluated by the same loop below —
			// the shape the schema always had for this input, now reachable
			// from a spelling that keeps its keys.
			mapped := kind.Structure.GetMap()
			if mapped == nil {
				return nil, nil, nil, nil, fmt.Errorf("outputs must be a mapping of names to values, not a list")
			}
			taskInputs.Outputs = mapped.GetEntries()
		case *Value_Expr:
			outputsExpr = kind.Expr
		case *Value_Error_:
			return nil, nil, nil, nil, fmt.Errorf("invalid outputs specification: %s", kind.Error.GetMessage())
		default:
			return nil, nil, nil, nil, fmt.Errorf("unsupported outputs specification kind: %T", kind)
		}
	}

	// Enforce the constraints the schema declares (a valid URI, a known
	// method). This is real validation: the previously generated validator
	// was produced by a plugin that reads a different option set than the
	// schema uses, so every check in it was a no-op.
	if err := Validate(taskInputs); err != nil {
		return nil, nil, nil, nil, NewTaskError("http", ErrorKindInvalidInput, err)
	}

	return taskInputs, outputsExpr, expectSpec, headersSpec, nil
}

// refuseCleartextCredential refuses a request that would carry a bearer
// secret or a JIT federation credential to a non-loopback http:// destination.
//
// Mirrors secrets/vault/vault.go's parseAddress guard against sending Vault's
// own client token in cleartext (see vault.go:751-766, "would send the client
// token in cleartext"): same idiom, and the identical loopback exception —
// applied here where the http task is about to do the analogous thing with a
// workflow-resolved credential rather than with Vault's own token.
//
// It runs on taskInputs and the request's already-built URL, before either
// [ResolveSecret] or [AuthorizeCredential] is ever called, so a refused
// request never reaches a secret backend and never triggers a live IdP
// exchange. That ordering is the point: had this run after resolution, the
// secret would already have left the reference behind, been read from
// wherever it lives, and be sitting in memory for a request this function is
// about to say should never have been made.
//
// Deliberately not a `flow validate` diagnostic. Whether a deployment
// terminates TLS in front of the worker — a sidecar, a service mesh — is a
// property of the deployment, not of the workflow file, and a validator runs
// in an author's editor with no way to know it. See CLAUDE.md, "report what
// is a property of the file, and stay silent about what a deployment
// decides."
//
// Out of scope, deliberately: the plugin credential path (plugin/task.go)
// resolves its own secret inputs and egresses on its own policy. This
// function has no visibility into a plugin's request and does not cover it —
// see #963.
func refuseCleartextCredential(taskInputs *Task_HTTP_Inputs, headersSpec *Value, reqURL *url.URL) error {
	if !taskCarriesCredential(taskInputs, headersSpec) {
		// Neither carries a resolved credential, so there is nothing this
		// request could leak by going out in the clear.
		return nil
	}
	if reqURL.Scheme != "http" {
		// https, or any other scheme the schema's own validation already
		// restricts to http/https — either way, not cleartext.
		return nil
	}
	if isLoopbackHost(reqURL.Hostname()) {
		// The one case plaintext http leaks nothing to the network: the
		// request never leaves the machine, the same reasoning vault.go
		// applies to a Vault Agent sidecar.
		return nil
	}

	return NewTaskError("http", ErrorKindPolicyDenied, fmt.Errorf(
		"%s would send a credential in cleartext; use https, or http only for "+
			"a loopback address such as a sidecar terminating TLS",
		reqURL.Redacted()))
}

// taskCarriesCredential reports whether taskInputs will attach a
// worker-resolved credential to the request: a bearer secret reference, a JIT
// federation target, or a reference nested in its headers or structured body.
//
// It is the single spelling of "this request carries a credential", shared by
// the cleartext refusal above (#963 half one) and the `credentials` fact this
// task marks on the egress-policy context below (#963 half two, see
// [netpolicy.ContextWithCredentials]) — so the two halves cannot drift apart
// on what counts as a credential.
func taskCarriesCredential(taskInputs *Task_HTTP_Inputs, headersSpec *Value) bool {
	if taskInputs.GetBearer() != nil || taskInputs.GetCredential() != "" ||
		ValueHoldsSecretRef(headersSpec) || ValueHoldsSecretRef(taskInputs.GetJson()) {
		return true
	}
	for _, value := range taskInputs.GetForm() {
		if ValueHoldsSecretRef(value) {
			return true
		}
	}
	return false
}

// isLoopbackHost reports whether host names the local machine — literally
// "localhost", or an address that parses and is its own loopback range.
//
// Matches secrets/vault/vault.go's isLoopback exactly: no DNS resolution, so a
// hostname that merely *resolves* to a loopback address today (and might
// resolve anywhere tomorrow) is not exempt. Only what the file itself spells
// out as loopback is trusted as loopback.
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}

	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}

	return ip.IsLoopback()
}

func taskFuncHTTP(policy *netpolicy.Policy) TaskFunc {
	return func(ctx context.Context, input map[string]*Value, scope *Scope) (*Node_Outputs, error) {
		taskInputs, outputsExpr, expectSpec, headersSpec, err := httpPreparedInputs(ctx, input, scope)
		if err != nil {
			return nil, err
		}

		requestURL, err := applyQuery(taskInputs.GetUrl(), taskInputs.GetQuery())
		if err != nil {
			return nil, httpInputError(err)
		}

		// Carry the run's attested identity into the egress policy, so a rule can
		// scope this request by tenant (#240). It is rendered from the one
		// WorkloadIdentity the scope carries — the same source the secret-access and
		// task-shape policies read — rather than derived a second way, which is what
		// keeps the three surfaces agreeing about who is calling. A local run carries
		// the identity its starter rehearsed as (`flow run local --as-namespace`,
		// through [NewContextWithRehearsalIdentity]), so an identity-scoped rule
		// answers here the way it answers in production — which is what a rehearsal
		// under `--egress-policy` is for. A run whose starter named none renders as
		// the empty identity, which an identity-scoped allow rule declines to match:
		// the fail-closed reading, and the same one the task-shape surface gives.
		if id := scope.GetIdentity(); id != nil {
			ctx = netpolicy.ContextWithIdentity(ctx, netpolicy.Identity{
				Subject:   id.GetSubject(),
				Issuer:    id.GetIssuer(),
				Namespace: id.GetNamespace(),
				Claims:    id.GetClaims(),
			})
		}

		// Mark whether this request carries a worker-resolved credential, so an
		// egress rule naming `credentials` (#963) sees the same fact the
		// cleartext refusal below already keys on — [taskCarriesCredential] is
		// the one detector both read. The mark rides the context the same way
		// identity does, and is known here because it comes from taskInputs
		// directly, before any secret has been resolved.
		credentialed := taskCarriesCredential(taskInputs, headersSpec)
		ctx = netpolicy.ContextWithCredentials(ctx, credentialed)

		// Build a bodyless request for checks that must happen before any nested
		// reference is resolved. The request body is immaterial to egress policy.
		httpReq, err := http.NewRequestWithContext(ctx, taskInputs.GetMethod(), requestURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP request: %w", err)
		}

		// Preflight the credentials-scoped egress rule before either secret path
		// is read. policy.Client().Do below applies the same request-scoped
		// rules when it actually dials, but that happens after ResolveSecret and
		// AuthorizeCredential run — this call is what keeps a rule denying
		// "credentials to this host" from ever reaching the secret backend or
		// performing a live IdP exchange for a request that will not be sent
		// (see the design comment on #963). Only worth the extra check when a
		// credential is in play; an uncredentialed request gets the identical
		// check for free when it is actually sent.
		if credentialed {
			if err := policy.CheckURL(httpReq.Context(), httpReq.Method, httpReq.URL); err != nil {
				return nil, NewTaskError("http", ErrorKindPolicyDenied, err)
			}
		}

		// Refused here, before anything below reads a secret. The destination is
		// fully known — httpReq.URL — and so is whether this request carries a
		// credential — taskInputs says so directly — so the refusal costs no
		// secret backend call and no live IdP exchange for a request that will
		// never be sent. See [refuseCleartextCredential].
		if err := refuseCleartextCredential(taskInputs, headersSpec, httpReq.URL); err != nil {
			return nil, err
		}

		// The scrubber exists before the first reference is resolved, because every
		// value resolved from here on is registered with it: a peer that reflects a
		// header or a body back is the path that turns a request credential into a
		// durable output.
		scrubber := secrets.NewScrubber()
		reveal := revealSecret(ctx, scrubber)

		bodyText, contentType, err := httpRequestBody(taskInputs, reveal)
		if err != nil {
			return nil, httpInputError(err)
		}
		if bodyText != "" || taskInputs.Body != nil {
			httpReq.Body = io.NopCloser(strings.NewReader(bodyText))
			httpReq.ContentLength = int64(len(bodyText))
			httpReq.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(bodyText)), nil
			}
		}

		// Apply request headers if provided.
		if taskInputs.Headers != nil {
			for k, v := range taskInputs.Headers {
				httpReq.Header.Add(k, v)
			}
		}

		// And the ones written as a structure, whose references are resolved one
		// entry at a time, straight onto the request. Applied before the bearer and
		// credential blocks below so that an `Authorization` written here is the
		// header they refuse to overwrite — two ways of setting one header have to
		// meet whichever order an author writes them in.
		if headersSpec != nil {
			header, err := httpRequestHeaders(headersSpec, reveal)
			if err != nil {
				return nil, httpInputError(err)
			}
			for name, values := range header {
				for _, value := range values {
					httpReq.Header.Add(name, value)
				}
			}
		}

		// Resolve the reference only here, after the task has crossed into its
		// execution context. The workflow and the activity payload continue to hold
		// only the reference; the revealed value exists for the lifetime of this
		// request and is never added to an output.
		if taskInputs.GetBearer() != nil && taskInputs.GetCredential() != "" {
			return nil, NewTaskError("http", ErrorKindInvalidInput,
				fmt.Errorf("bearer and credential are mutually exclusive; use a static secret reference or a JIT federation target, not both"))
		}
		if bearer := taskInputs.GetBearer(); bearer != nil {
			ref := bearer.GetSecretRef()
			if ref == nil {
				return nil, NewTaskError("http", ErrorKindInvalidInput,
					fmt.Errorf("bearer must be a secret reference, such as env:API_TOKEN"))
			}
			if httpReq.Header.Get("Authorization") != "" {
				return nil, NewTaskError("http", ErrorKindInvalidInput,
					fmt.Errorf("bearer and an Authorization header cannot both be set"))
			}
			secret, err := ResolveSecret(ctx, ref)
			if err != nil {
				kind := ErrorKindPolicyDenied
				if secrets.Retryable(err) {
					kind = ErrorKindUpstream
				}
				return nil, NewTaskError("http", kind, fmt.Errorf("resolving bearer reference %s: %w", secretRefText(ref), err))
			}
			scrubber.Add(secret)
			httpReq.Header.Set("Authorization", "Bearer "+secret.Reveal())
		}
		if target := taskInputs.GetCredential(); target != "" {
			if httpReq.Header.Get("Authorization") != "" {
				return nil, NewTaskError("http", ErrorKindInvalidInput,
					fmt.Errorf("credential and an Authorization header cannot both be set"))
			}
			if err := AuthorizeCredential(ctx, httpReq, target); err != nil {
				kind := ErrorKindPolicyDenied
				if auth.Retryable(err) {
					kind = ErrorKindUpstream
				}
				return nil, NewTaskError("http", kind,
					fmt.Errorf("authorizing federation target %q: %w", target, err))
			}
			// The broker applies material directly to the request. Register both the
			// complete header and its bearer value so a peer reflecting either form
			// cannot put the credential into outputs or history.
			authorization := httpReq.Header.Get("Authorization")
			scrubber.AddValue(authorization)
			if token, found := strings.CutPrefix(authorization, "Bearer "); found {
				scrubber.AddValue(token)
			}
		}

		// A structured body implies the header describing it, but only when the author
		// did not say: someone sending a JSON variant like application/ld+json has
		// been more specific than we can be, and overwriting that would be wrong.
		if contentType != "" && httpReq.Header.Get("Content-Type") == "" {
			httpReq.Header.Set("Content-Type", contentType)
		}

		// Built, authorized, and about to leave the worker. A step that sits here is
		// waiting on somebody else, and until this said so a long request was
		// indistinguishable from a wedged worker.
		ReportProgress(ctx, PhaseRequesting)

		httpResp, err := policy.Client().Do(httpReq)
		if err != nil {
			// Read the typed rate refusal off the error the transport returned,
			// before scrubbing, because [secrets.Scrubber.ScrubError]
			// deliberately breaks errors.As — a typed error can hold the
			// unredacted URL in an exported field, so it exposes nothing but
			// errors.Is. plugin/task.go makes the same move for the same
			// reason. Only the delay and the host are taken from it; the
			// message reported below is built from the scrubbed error.
			var limited *netpolicy.RateLimitedError
			rateLimited := errors.As(err, &limited)

			err = scrubber.ScrubError(err)
			// A policy denial is deliberate and will happen again; a connection
			// reset, DNS failure, or timeout may succeed later. Distinguishing
			// them is what stops a denied request from being retried.
			if errors.Is(err, netpolicy.ErrDenied) {
				return nil, NewTaskError("http", ErrorKindPolicyDenied, err)
			}

			// The policy's own per-host rate bound (#912 phase two). Checked
			// before the two classifications below, because it is neither of
			// the things they decide: the request was permitted and was never
			// sent, so it is not a denial and its outcome is not unknown — a
			// bucket that refuses cannot have reached the peer. Classifying it
			// as UpstreamUnknown on a POST would make a request nobody made
			// look like one that might have taken effect.
			//
			// RateLimited is retryable and carries the bucket's own wait, so
			// this rides the machinery a 429's Retry-After already rides
			// (#1180): both drivers read RetryAfter off the error and schedule
			// the next attempt from it. Nothing blocks in the activity.
			if rateLimited {
				rateErr := NewTaskError("http", ErrorKindRateLimited, fmt.Errorf(
					"%s %s was held back by this worker's own rate limit for %s of %g requests per second per process: %w",
					taskInputs.GetMethod(), taskInputs.GetUrl(), limited.Host, limited.RequestsPerSecond, err))
				rateErr.RetryAfter = limited.RetryAfter
				return nil, rateErr
			}

			if !taskInputs.GetRetryOnUnknownOutcome() && !retriableTransportFailure(taskInputs.GetMethod(), err) {
				return nil, NewTaskError("http", ErrorKindUpstreamUnknown, fmt.Errorf(
					"%s %s failed with no response, so whether it took effect is unknown: %w",
					taskInputs.GetMethod(), taskInputs.GetUrl(), err))
			}

			return nil, NewTaskError("http", ErrorKindUpstream, err)
		}
		defer httpResp.Body.Close()

		// The body is read before success is decided, because `expect` is an
		// expression over the response and the interesting cases are about its
		// content: a 200 carrying an error, or a 404 that means "not yet". Reading it
		// first costs nothing — the policy bounds the read either way.
		//
		// The policy bounds the read, so no endpoint a workflow names can decide
		// how much memory the worker allocates.
		// The peer answered; what is left is however long it takes to say the rest.
		// Separate from the phase above because the two fail differently: stuck
		// requesting is a peer that has said nothing, stuck reading is a peer that
		// answered and then stopped talking.
		ReportProgress(ctx, PhaseReadingResponse)

		respBody, err := policy.ReadResponseBody(httpResp)
		if err != nil {
			err = scrubber.ScrubError(err)
			var tooLarge *netpolicy.BodyTooLargeError
			if errors.As(err, &tooLarge) {
				return nil, NewTaskError("http", ErrorKindLimitExceeded, fmt.Errorf(
					"response body from %s is too large: %w; use the outputs input to select only the fields this step needs",
					taskInputs.GetUrl(), err))
			}
			// The status said the request succeeded and only reading the reply
			// failed. For an idempotent method another attempt is free; for a POST
			// or a PATCH it would perform the operation a second time, and this
			// time we know the first one completed rather than merely suspecting
			// it. So the outcome is reported rather than retried.
			//
			// This path matters more than it looks. A failure after the headers is
			// the normal way a chunked or event-stream response breaks, so it stops
			// being an edge case as soon as a response is anything but one buffered
			// body.
			if !taskInputs.GetRetryOnUnknownOutcome() && !idempotentMethods[strings.ToUpper(taskInputs.GetMethod())] {
				return nil, NewTaskError("http", ErrorKindUpstreamUnknown, fmt.Errorf(
					"%s %s returned %d and then the response could not be read, so it took effect but its result is lost: %w",
					taskInputs.GetMethod(), taskInputs.GetUrl(), httpResp.StatusCode, err))
			}

			return nil, NewTaskError("http", ErrorKindUpstream,
				fmt.Errorf("failed to read HTTP response body: %w", err))
		}

		// A peer may reflect the Authorization header in its response body or
		// headers. Those values are about to become task outputs and workflow
		// history, so scrub them before parsing, expectation evaluation, logging or
		// output shaping can observe them. Scrubbing only transport errors protects
		// the exceptional path and leaves the successful echo path wide open.
		respBody = []byte(scrubber.Scrub(string(respBody)))
		for name, values := range httpResp.Header {
			for i := range values {
				values[i] = scrubber.Scrub(values[i])
			}
			httpResp.Header[name] = values
		}

		return httpAnswerFromResponse(ctx, taskInputs, outputsExpr, expectSpec, httpResp, respBody, scope)
	}
}

// httpAnswerFromResponse is the response-dependent tail of the http task:
// optional JSON parsing under the element bound, `expect:` deciding success,
// and the step's own `outputs:` shaping — everything the task does once it
// holds a status, headers, and a body, wherever they came from. One function
// with two callers — [taskFuncHTTP] after a real request, and
// [httpStubResponseFn] over a response a test declared — which is what makes a
// stubbed response exercise the same shaping expressions a live one does
// instead of bypassing them (#925).
//
// The caller owns everything transport: the request, the bounded body read,
// and the credential scrub, which must happen before this observes the bytes.
func httpAnswerFromResponse(ctx context.Context, taskInputs *Task_HTTP_Inputs, outputsExpr *expr.ParsedExpr, expectSpec *Value, httpResp *http.Response, respBody []byte, scope *Scope) (*Node_Outputs, error) {

	// Parsing is opt-in, so a body that is not JSON is a real error here rather
	// than a silently empty value: a step that asked for JSON and got HTML has a
	// problem worth naming.
	var parsedJSON *expr.Value
	if taskInputs.GetParseJson() {
		var err error
		parsedJSON, err = parseJSONResponse(respBody)
		if err != nil {
			return nil, NewTaskError("http", ErrorKindInvalidInput, fmt.Errorf(
				"%s %s: %w", taskInputs.GetMethod(), taskInputs.GetUrl(), err))
		}

		// Bounded here, before anything below gets a chance to walk it: both
		// `expect:` (httpExpectationMet, below) and `outputs:` (the
		// evaluation further down) run a CEL expression directly against
		// `response.json` *inside this function*, so
		// [checkTaskOutputElementBound] at [Task.EvalInScope] — which only
		// sees what this function returns — is too late to stop a
		// comprehension over an oversized response from ever running. The
		// byte cap on the body (see [netpolicy.Policy.ReadResponseBody])
		// bounds bytes, not elements: a body well under it can still carry
		// tens of thousands of small elements, exactly the asymmetry #204
		// measured for the input side and #224 review found still open
		// here for a task's own evaluation of its result.
		if err := checkHTTPResponseElementBound(taskInputs.GetUrl(), parsedJSON); err != nil {
			return nil, err
		}
	}

	respVars := httpResponseVars(httpResp, respBody, parsedJSON)

	if err := httpExpectationMet(ctx, taskInputs, expectSpec, httpResp, respVars, scope); err != nil {
		return nil, err
	}

	// Default outputs mirror the response so a workflow can reference
	// ${steps.<id>.status_code}, ${steps.<id>.body}, and ${steps.<id>.headers} without
	// declaring an outputs expression.
	defaultOuts := &Task_HTTP_Outputs{
		StatusCode: int32(httpResp.StatusCode),
		Body:       string(respBody),
		Headers:    firstHeaderValues(httpResp.Header),
		Json:       parsedJSON,
	}

	// If typed outputs provided (either as explicit map entries or via a CEL map expression),
	// evaluate them using the response variables and return only those named values.
	if len(taskInputs.Outputs) > 0 || outputsExpr != nil {
		env, err := httpResponseEnv(scope.GetProfile())
		if err != nil {
			return nil, err
		}
		varAct, err := interpreter.NewActivation(respVars)
		if err != nil {
			return nil, fmt.Errorf("failed to create activation: %w", err)
		}
		// The scope's own activation, which is what carries `vars.<name>` and a
		// loop's iterator into here — and which sets Ctx, Eval and Profile, so a
		// stored expression resolved while shaping these outputs stays
		// cancellable, cost-bounded and pinned to the run's vocabulary. The
		// hand-built one this replaces set the first two and dropped the rest.
		//
		// The response is the child, so it is asked first; see the same note in
		// [httpExpectSatisfied].
		act := interpreter.NewHierarchicalActivation(scope.Activation(ctx), varAct)

		if outputsExpr != nil {
			// Through the shared evaluator, which is what applies the cost
			// limit and makes the evaluation cancellable. Building a program
			// here by hand did neither: an author's `outputs:` expression is
			// the expression they most directly control, and it was the one
			// place in the engine that ran unbounded.
			//
			// Every failure below is classified, and that is not tidiness. An
			// unwrapped error classifies as [ErrorKindInternal], which is
			// *retryable* — so a typo in `outputs:` re-ran the whole attempt,
			// and the whole attempt begins by sending the request again. A POST
			// that had already succeeded was sent five times for a mistake no
			// number of attempts could fix. [ErrorKind.Retryable] states the
			// rule this was breaking: "Retrying a POST that already took effect
			// is worse than surfacing a failure that might have resolved on its
			// own." `expect:`, one file over, has classified all along.
			out, err := DefaultEvaluator().EvalParsed(ctx, env, outputsExpr, act)
			if err != nil {
				return nil, NewTaskError("http", ErrorKindExpression,
					fmt.Errorf("evaluating outputs: %w", err))
			}
			pv, err := cel.RefValueToValue(out)
			if err != nil {
				return nil, NewTaskError("http", ErrorKindExpression,
					fmt.Errorf("converting the result of outputs: %w", err))
			}
			mv, ok := pv.GetKind().(*expr.Value_MapValue)
			if !ok {
				return nil, NewTaskError("http", ErrorKindExpression,
					fmt.Errorf("outputs must evaluate to a map of names to values, got %s",
						out.Type().TypeName()))
			}
			outputs := &Node_Outputs{NamedValues: make(map[string]*Value, len(mv.MapValue.Entries))}
			for _, e := range mv.MapValue.Entries {
				outputs.NamedValues[e.Key.GetStringValue()] = &Value{
					Kind: &Value_Literal{Literal: e.Value},
				}
			}
			return outputs, nil
		}

		outputs := &Node_Outputs{NamedValues: map[string]*Value{}}
		for name, v := range taskInputs.Outputs {
			switch k := v.GetKind().(type) {
			case *Value_Expr:
				// Same path as the whole-block form above, for the same reason.
				out, err := DefaultEvaluator().EvalParsed(ctx, env, k.Expr, act)
				if err != nil {
					return nil, NewTaskError("http", ErrorKindExpression,
						fmt.Errorf("evaluating output %q: %w", name, err))
				}
				pv, err := cel.RefValueToValue(out)
				if err != nil {
					return nil, NewTaskError("http", ErrorKindExpression,
						fmt.Errorf("converting output %q: %w", name, err))
				}
				outputs.NamedValues[name] = &Value{Kind: &Value_Literal{Literal: pv}}
			case *Value_Literal:
				outputs.NamedValues[name] = &Value{Kind: &Value_Literal{Literal: k.Literal}}
			default:
				return nil, NewTaskError("http", ErrorKindInvalidInput,
					fmt.Errorf("output %q is neither an expression nor a literal (%T)", name, v.GetKind()))
			}
		}
		return outputs, nil
	}

	return nodeOutputsFromProtoMessage(defaultOuts)
}
