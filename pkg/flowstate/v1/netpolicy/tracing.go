package netpolicy

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope these spans are attributed to, spelled
// the way [engine.startTaskSpan] spells its own: the package's import path.
const tracerName = "github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"

// tracingRoundTripper opens the CLIENT span covering one outbound request and
// injects W3C trace context onto it.
//
// # Why it lives here rather than in the http task
//
// This is the same argument the response byte cap already makes one file over: a
// bound — or in this case an instrumentation — that every caller has to remember
// to apply is one the next caller will not have. The http task's request leaves
// through a [Policy]'s client, and so does every other caller that egresses
// under a policy — `plugins/git` and `plugins/vcs` each build one — so wrapping
// the policy's transport instruments all of them once instead of instrumenting
// the task and leaving the next caller uninstrumented. It also puts the span at
// the egress boundary it describes: the span's lifetime is the round trip the
// policy governs, including the denial it may answer with instead.
//
// It wraps *above* the policy's own [roundTripper] so a refused request still
// produces a span. A denial that left no trace would be the one outcome an
// operator most wants to see and the only one invisible.
//
// # What it may say, which is much less than it knows
//
// A span is exported to a collector, indexed, and read by people and systems
// with no relationship to the run that produced it — the same reasoning
// `engine`'s task span is built on, and stricter than workflow history, because
// a collector is not tenant-scoped at all.
//
// So the URL never becomes an attribute in any form: not `url.full`, not
// `url.query`, not `url.path`. The schema already states why for the query, at
// [flowstatev1.HTTPTaskDef]'s comment on why `query` is excluded from
// NestedSecretInputs — "a query string is written to access logs, kept in
// browser history, and forwarded in a Referer header" — and a span attribute is
// another such destination. The path is left out for the same reason wearing
// different clothes: a webhook URL of the shape
// https://hooks.example.com/services/T000/B000/<token> carries its credential
// there. And userinfo (https://user:password@host) is never reached, because
// what is recorded is [url.URL.Hostname], which does not contain it.
//
// What is left is the shape of the call and not its content: the method, the
// scheme, the host and port dialed, and the status returned. That is enough to
// find the span, and there is no spelling of a secret that fits in it.
//
// Errors are recorded as a fixed classification, never with
// [trace.Span.RecordError] and never with the error's own text — the rule
// `plugin/telemetry.go` and `engine/activities.go` both already state, and it
// binds harder here: a [DenyError]'s Detail names the target it refused, which
// is a URL.
type tracingRoundTripper struct {
	next http.RoundTripper
}

// RoundTrip implements [http.RoundTripper].
func (rt *tracingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	name, method := spanNameAndMethod(req.Method)

	attrs := []attribute.KeyValue{method}
	if req.URL != nil {
		if scheme := strings.ToLower(req.URL.Scheme); scheme != "" {
			attrs = append(attrs, semconv.URLScheme(scheme))
		}
		if host := ruleHost(req.URL); host != "" {
			attrs = append(attrs, semconv.ServerAddress(host))
		}
		if port, ok := spanPort(req.URL); ok {
			attrs = append(attrs, semconv.ServerPort(port))
		}
	}

	// The provider is read per request rather than captured when the client is
	// built, because a [Policy] outlives the process's telemetry setup: a client
	// built before the provider was installed would hold the no-op tracer for
	// good. With no provider installed this is the no-op tracer, the span is not
	// recording, its context is invalid, and the injection below writes nothing —
	// which is how "no telemetry configured" stays literally silent.
	ctx, span := otel.GetTracerProvider().Tracer(tracerName).Start(req.Context(), name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...))

	// A round tripper must not modify the request it is given, so the header the
	// propagator writes goes onto a copy — the same rule the policy's own round
	// tripper follows when it attaches dial attributes.
	req = req.Clone(ctx)

	// W3C trace context only, and deliberately not the global propagator.
	//
	// The globally registered propagator is a composite that also carries
	// baggage, and baggage is caller-controlled key/value data. The peer here is
	// whatever host a workflow named, so forwarding baggage would hand an
	// arbitrary third party whatever any part of this process put in it. The
	// plugin boundary makes the narrower version of this same call — it filters
	// baggage down to two bounded members even for a plugin the worker launched
	// itself (see plugin/telemetry.go's propagationInterceptor) — and an external
	// peer gets less trust than that, not more.
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := rt.next.RoundTrip(req)
	if err != nil {
		// The type, not the message. A transport error can quote a URL, and a
		// [DenyError] certainly does.
		span.SetAttributes(semconv.ErrorTypeKey.String(fmt.Sprintf("%T", err)))
		span.SetStatus(codes.Error, "request failed")
		span.End()

		return nil, err
	}

	span.SetAttributes(semconv.HTTPResponseStatusCode(resp.StatusCode))

	classified := false
	if resp.StatusCode >= 400 {
		// Semantic conventions make a 4xx or 5xx an error for a client span: the
		// caller asked for something and did not get it. The description is the
		// status and nothing else, because the reason belongs to a body whose
		// text this must not repeat.
		span.SetAttributes(semconv.ErrorTypeKey.String(strconv.Itoa(resp.StatusCode)))
		span.SetStatus(codes.Error, "request failed")
		classified = true
	}

	if resp.Body == nil {
		span.End()

		return resp, nil
	}

	// The span outlives RoundTrip, because RoundTrip returns when the *headers*
	// arrive and a response is not over then. Ending here would report a
	// four-minute streamed download as a two-millisecond request, and would call
	// a 200 whose body dies mid-read a success — which is the outcome the http
	// task itself reports as a failure. So the body carries the span and ends it
	// when the response is actually over.
	//
	// The cost, named: a caller that never closes the body never ends the span.
	// That caller is already leaking a connection, which is the louder failure of
	// the two, and the alternative — ending at the headers — is wrong for every
	// well-behaved caller rather than for the broken one.
	resp.Body = &tracedBody{ReadCloser: resp.Body, span: span, classified: classified}

	return resp, nil
}

// tracedBody ends the request's span when the response body is finished with,
// whether that is by being read to the end, by failing, or by being closed.
//
// Whichever happens first wins: a body read to EOF and then closed ends its span
// once, at the EOF.
type tracedBody struct {
	io.ReadCloser

	span trace.Span

	// classified records whether an error.type has already been written for this
	// span, so a body that fails after a 4xx does not overwrite the status that
	// explains it.
	classified bool

	once sync.Once
}

// Read implements [io.Reader], ending the span at the end of the body or at the
// failure that stops it.
func (b *tracedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)

	switch {
	case err == nil:
	case errors.Is(err, io.EOF):
		b.finish(nil)
	default:
		b.finish(err)
	}

	return n, err
}

// Close implements [io.Closer]. A body closed before it is exhausted — the
// ordinary shape of a caller that has read enough — ends the span there.
func (b *tracedBody) Close() error {
	err := b.ReadCloser.Close()
	b.finish(nil)

	return err
}

// finish ends the span exactly once, recording a read failure as a
// classification and never as its text: a [BodyTooLargeError] names a limit, and
// a transport error at this point can quote the URL the body was coming from.
func (b *tracedBody) finish(err error) {
	b.once.Do(func() {
		if err != nil {
			if !b.classified {
				b.span.SetAttributes(semconv.ErrorTypeKey.String(fmt.Sprintf("%T", err)))
			}
			b.span.SetStatus(codes.Error, "response body failed")
		}

		b.span.End()
	})
}

// spanNameAndMethod returns the span's name and its http.request.method
// attribute for a request method.
//
// Semantic conventions name a client span after the method alone when there is
// no low-cardinality URL template to add, and require a method the
// instrumentation does not know to be reported as `_OTHER`.
//
// # The match is case-sensitive, which is not fussiness
//
// The conventions say it outright — "HTTP method names are case-sensitive and
// `http.request.method` attribute value MUST match a known HTTP method name
// exactly" — and this repository can actually produce the mismatch: the schema
// accepts a method in any case (`pattern: '^(?i)(GET|POST|PUT|PATCH|DELETE)$'`,
// task.proto) and the http task passes the author's spelling to
// [http.NewRequestWithContext] unchanged, so a step written `method: get` puts
// `get` on the wire. A span reading `GET` would then describe an operation
// nobody performed, and a server that treats the token strictly answers it
// differently.
//
// The conventions do allow canonicalizing — but only for instrumentations of
// frameworks that consider methods case-insensitive, and only if
// `http.request.method_original` is also set. Neither applies: net/http sends
// what it is given, and the original spelling is a caller-chosen string, which
// this file does not turn into an attribute.
//
// The one conversion left is the empty method, and it is not a conversion:
// net/http sends GET for it, so GET is what happened.
func spanNameAndMethod(method string) (string, attribute.KeyValue) {
	switch method {
	case "":
		return http.MethodGet, semconv.HTTPRequestMethodGet
	case http.MethodGet:
		return http.MethodGet, semconv.HTTPRequestMethodGet
	case http.MethodHead:
		return http.MethodHead, semconv.HTTPRequestMethodHead
	case http.MethodPost:
		return http.MethodPost, semconv.HTTPRequestMethodPost
	case http.MethodPut:
		return http.MethodPut, semconv.HTTPRequestMethodPut
	case http.MethodPatch:
		return http.MethodPatch, semconv.HTTPRequestMethodPatch
	case http.MethodDelete:
		return http.MethodDelete, semconv.HTTPRequestMethodDelete
	case http.MethodConnect:
		return http.MethodConnect, semconv.HTTPRequestMethodConnect
	case http.MethodOptions:
		return http.MethodOptions, semconv.HTTPRequestMethodOptions
	case http.MethodTrace:
		return http.MethodTrace, semconv.HTTPRequestMethodTrace
	default:
		return "HTTP", semconv.HTTPRequestMethodOther
	}
}

// spanPort returns the port the request will actually reach, which is the one
// written in the URL or the default for its scheme.
func spanPort(u *url.URL) (int, bool) {
	if written := u.Port(); written != "" {
		port, err := strconv.Atoi(written)
		if err != nil {
			return 0, false
		}

		return port, true
	}

	switch strings.ToLower(u.Scheme) {
	case "http":
		return 80, true
	case "https":
		return 443, true
	default:
		return 0, false
	}
}
