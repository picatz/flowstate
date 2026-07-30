package flowstatev1

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/interpreter"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Content types the task sets for the structured bodies it can build. They are set
// rather than left to the author because a body and the header describing it are one
// decision, and splitting them is how a JSON body gets sent as form data.
const (
	contentTypeJSON = "application/json"
	contentTypeForm = "application/x-www-form-urlencoded"
)

// maxRetryAfter bounds how long a server may ask us to wait.
//
// Retry-After is the server telling us when to come back, and honoring it is both
// more polite and better for us than guessing. But it is also a value an outside
// party chooses, and an unbounded one would let a server pin a step open for as long
// as it liked. Past this the header is ignored and the ordinary backoff applies.
const maxRetryAfter = 5 * time.Minute

// httpRequestBody builds the request body from whichever of body, json, and form was
// given, returning the content type that describes it.
//
// The three are mutually exclusive. Accepting more than one would make the meaning of
// a request depend on which field the implementation happened to read first, so it is
// an error rather than a precedence rule nobody would remember.
func httpRequestBody(inputs *Task_HTTP_Inputs) (body string, contentType string, err error) {
	given := make([]string, 0, 3)
	if inputs.Body != nil {
		given = append(given, "body")
	}
	if inputs.GetJson() != nil {
		given = append(given, "json")
	}
	if len(inputs.GetForm()) > 0 {
		given = append(given, "form")
	}

	if len(given) > 1 {
		slices.Sort(given)
		return "", "", fmt.Errorf(
			"%s are mutually exclusive; a request has one body, so pick the one that describes it",
			strings.Join(given, " and "))
	}

	switch {
	case inputs.Body != nil:
		// A raw body carries no implied content type: the author is spelling out the
		// bytes, so they spell out the header too if it matters.
		return inputs.GetBody(), "", nil

	case inputs.GetJson() != nil:
		encoded, err := jsonRequestBody(inputs.GetJson())
		if err != nil {
			return "", "", err
		}
		return encoded, contentTypeJSON, nil

	case len(inputs.GetForm()) > 0:
		encoded, err := formRequestBody(inputs.GetForm())
		if err != nil {
			return "", "", err
		}
		return encoded, contentTypeForm, nil

	default:
		return "", "", nil
	}
}

// jsonRequestBody serializes a structured body to JSON.
func jsonRequestBody(value *Value) (string, error) {
	native, err := valueToNative(value)
	if err != nil {
		return "", fmt.Errorf("json body: %w", err)
	}

	encoded, err := json.Marshal(native)
	if err != nil {
		return "", fmt.Errorf("json body could not be serialized: %w", err)
	}

	return string(encoded), nil
}

// formRequestBody url-encodes a form body.
func formRequestBody(form map[string]*Value) (string, error) {
	values, err := urlValues(form, "form")
	if err != nil {
		return "", err
	}

	return values.Encode(), nil
}

// applyQuery returns rawURL with the given parameters added to its query string.
//
// Building a query string by hand is where escaping bugs live, so the escaping
// happens here once. Parameters already present in the URL are kept: a base URL with
// a fixed parameter and a step adding one more is a reasonable thing to write.
func applyQuery(rawURL string, query map[string]*Value) (string, error) {
	if len(query) == 0 {
		return rawURL, nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("url %q could not be parsed to add query parameters: %w", rawURL, err)
	}

	added, err := urlValues(query, "query")
	if err != nil {
		return "", err
	}

	existing := parsed.Query()
	for name, values := range added {
		for _, value := range values {
			existing.Add(name, value)
		}
	}

	parsed.RawQuery = existing.Encode()

	return parsed.String(), nil
}

// urlValues renders a map of values as url.Values, in sorted key order so that an
// encoded query or form body is the same on every run — which matters because a
// workflow's inputs are recorded and compared.
func urlValues(m map[string]*Value, what string) (url.Values, error) {
	values := make(url.Values, len(m))

	for _, name := range slices.Sorted(maps.Keys(m)) {
		if name == "" {
			return nil, fmt.Errorf("%s has an entry with an empty name", what)
		}

		rendered, err := valueToQueryString(m[name])
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", what, name, err)
		}

		values[name] = rendered
	}

	return values, nil
}

// valueToQueryString renders one query or form entry.
//
// A list becomes a repeated parameter, which is how every server that accepts more
// than one value for a name expects to receive them. A nested structure has no
// agreed encoding, so it is refused rather than guessed at.
func valueToQueryString(v *Value) ([]string, error) {
	if ref := v.GetSecretRef(); ref != nil {
		// The author's intent is clear and only the placement is wrong, so this says
		// where to put it instead of refusing anonymously. A query string is written
		// to access logs, kept in browser history, and sent onward in a Referer
		// header on redirect — a secret in one is a secret published.
		return nil, fmt.Errorf(
			"a secret reference cannot go in a query parameter, because query strings are recorded in "+
				"access logs, browser history, and Referer headers; put it in a header instead (%s)",
			secretRefText(ref))
	}

	literal := v.GetLiteral()
	if literal == nil {
		return nil, fmt.Errorf("must be a literal value or an expression producing one, got %T", v.GetKind())
	}

	if list := literal.GetListValue(); list != nil {
		rendered := make([]string, 0, len(list.GetValues()))
		for i, element := range list.GetValues() {
			text, err := scalarText(element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			rendered = append(rendered, text)
		}
		return rendered, nil
	}

	text, err := scalarText(literal)
	if err != nil {
		return nil, err
	}

	return []string{text}, nil
}

// scalarText renders a scalar CEL value as the text a query string or form body
// carries.
func scalarText(v *expr.Value) (string, error) {
	switch kind := v.GetKind().(type) {
	case *expr.Value_StringValue:
		return kind.StringValue, nil
	case *expr.Value_Int64Value:
		return strconv.FormatInt(kind.Int64Value, 10), nil
	case *expr.Value_Uint64Value:
		return strconv.FormatUint(kind.Uint64Value, 10), nil
	case *expr.Value_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64), nil
	case *expr.Value_BoolValue:
		return strconv.FormatBool(kind.BoolValue), nil
	case *expr.Value_BytesValue:
		return string(kind.BytesValue), nil
	case *expr.Value_NullValue:
		return "", nil
	default:
		return "", fmt.Errorf(
			"a %T has no agreed encoding in a query string or form body; send it as a json body instead",
			kind)
	}
}

// valueToNative converts a value to the Go representation encoding/json accepts.
func valueToNative(v *Value) (any, error) {
	if ref := v.GetSecretRef(); ref != nil {
		// Unlike a query parameter, a JSON body is a legitimate place for a
		// credential — but resolving one here would mean the workflow had already
		// evaluated it, which is what puts a secret in history. Whether a body may
		// carry a reference is a decision for the schema, not for this converter.
		return nil, fmt.Errorf(
			"a secret reference cannot be placed inside a json body yet (%s); "+
				"send it in a header, where a task input accepts a reference",
			secretRefText(ref))
	}

	literal := v.GetLiteral()
	if literal == nil {
		return nil, fmt.Errorf("must be a literal value or an expression producing one, got %T", v.GetKind())
	}

	return literalToNative(literal)
}

// literalToNative converts a CEL literal to its Go equivalent, recursively.
func literalToNative(v *expr.Value) (any, error) {
	switch kind := v.GetKind().(type) {
	case *expr.Value_NullValue:
		return nil, nil
	case *expr.Value_StringValue:
		return kind.StringValue, nil
	case *expr.Value_Int64Value:
		return kind.Int64Value, nil
	case *expr.Value_Uint64Value:
		return kind.Uint64Value, nil
	case *expr.Value_DoubleValue:
		return kind.DoubleValue, nil
	case *expr.Value_BoolValue:
		return kind.BoolValue, nil
	case *expr.Value_BytesValue:
		return kind.BytesValue, nil

	case *expr.Value_ListValue:
		list := make([]any, 0, len(kind.ListValue.GetValues()))
		for i, element := range kind.ListValue.GetValues() {
			native, err := literalToNative(element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			list = append(list, native)
		}
		return list, nil

	case *expr.Value_MapValue:
		object := make(map[string]any, len(kind.MapValue.GetEntries()))
		for _, entry := range kind.MapValue.GetEntries() {
			name := entry.GetKey().GetStringValue()
			if name == "" {
				return nil, fmt.Errorf("json object keys must be non-empty strings")
			}
			native, err := literalToNative(entry.GetValue())
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", name, err)
			}
			object[name] = native
		}
		return object, nil

	default:
		return nil, fmt.Errorf("a %T cannot be represented as json", kind)
	}
}

// parseJSONResponse parses a response body into a CEL value for the json output.
//
// Parsing happens only when the author asked for it, so a malformed body is a real
// error rather than something to shrug at: a step that asked for JSON and got HTML
// has a problem worth reporting, and an empty value would hide it behind whatever
// expression read it next.
func parseJSONResponse(body []byte) (*expr.Value, error) {
	var native any
	if err := json.Unmarshal(body, &native); err != nil {
		return nil, fmt.Errorf("response body is not valid json: %w", err)
	}

	value, err := cel.RefValueToValue(types.DefaultTypeAdapter.NativeToValue(native))
	if err != nil {
		return nil, fmt.Errorf("parsed json could not be represented as a value: %w", err)
	}

	return value, nil
}

// retryAfter returns how long a response asked us to wait before trying again.
//
// It reads both forms RFC 9110 defines: a delay in seconds, and an HTTP date. A date
// in the past means "now", not a negative wait. Anything unparsable, or longer than
// [maxRetryAfter], is ignored so the ordinary backoff applies — a server does not get
// to hold a step open indefinitely by sending a large number.
func retryAfter(header string, now time.Time) (time.Duration, bool) {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0, false
	}

	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil {
		return clampRetryAfter(time.Duration(seconds) * time.Second)
	}

	if when, err := http.ParseTime(header); err == nil {
		return clampRetryAfter(when.Sub(now))
	}

	return 0, false
}

// clampRetryAfter bounds a requested delay, treating a past date as no wait.
func clampRetryAfter(d time.Duration) (time.Duration, bool) {
	switch {
	case d > maxRetryAfter:
		return 0, false
	case d < 0:
		return 0, true
	default:
		return d, true
	}
}

// secretRefText renders a secret reference for a diagnostic. It names the reference,
// never a value, which is the whole point of a reference.
func secretRefText(ref *SecretRef) string {
	return ref.GetScheme() + ":" + ref.GetName()
}

// httpResponseVars builds the variables an `outputs` or `expect` expression is
// evaluated against.
//
// One construction serves both, so an author who can read a field in `expect` can
// read the same field in `outputs`. Headers are exposed as lists here — every value
// of a repeated header — while the default outputs flatten them to one value each,
// because an expression asking about a header usually wants all of it and a workflow
// referencing ${steps.<id>.headers['X']} wants a string.
func httpResponseVars(resp *http.Response, body []byte, parsedJSON *expr.Value) map[string]any {
	fields := map[string]any{
		"status_code": int64(resp.StatusCode),
		"body":        string(body),
	}

	if len(resp.Header) > 0 {
		headers := make(map[string][]string, len(resp.Header))
		maps.Copy(headers, resp.Header)
		fields["headers"] = headers
	}

	// Absent rather than null when parsing was not asked for, so an expression
	// reading `response.json` without setting parse_json fails saying the name is
	// unknown instead of quietly evaluating against nothing.
	if parsedJSON != nil {
		native, err := literalToNative(parsedJSON)
		if err == nil {
			fields["json"] = native
		}
	}

	// Rooted, so exactly one name enters the author's namespace however many fields a
	// response grows. See the comment on the environment these are declared in.
	return map[string]any{ResponseRoot: fields}
}

// httpExpectationMet decides whether a response counts as success, returning a
// classified error when it does not.
//
// With no `expect` expression the rule is the default one and is deliberately
// unchanged: 2xx succeeds, 4xx is the endpoint rejecting this request and will reject
// it again, 5xx may be transient. An `expect` expression replaces that judgement
// entirely, because an author writing one is telling us something the status alone
// cannot express — a 404 that means "not there yet, and that is fine", or a 200
// carrying an error in the body.
func httpExpectationMet(
	ctx context.Context,
	inputs *Task_HTTP_Inputs,
	expectSpec *Value,
	resp *http.Response,
	vars map[string]any,
	scope *Scope,
) error {
	if expectSpec != nil {
		return httpExpectSatisfied(ctx, inputs, expectSpec, resp, vars, scope)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	kind := ErrorKindUpstream
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		kind = ErrorKindInvalidInput
	}

	err := NewTaskError("http", kind, fmt.Errorf(
		"%s %s returned status %d", inputs.GetMethod(), inputs.GetUrl(), resp.StatusCode))

	// On a 429 or a 503 the server has told us when to come back. Ignoring that is
	// both rude and worse for us than guessing, so it is carried on the error for the
	// substrate to schedule rather than slept off inside the activity, which would
	// hold a worker slot for the duration.
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		if delay, ok := retryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			err.RetryAfter = delay
		}
	}

	return err
}

// httpExpectSatisfied evaluates an `expect` expression over the response.
//
// A failed expectation is permanent. The author has described what success looks
// like, and a response that does not match it is this endpoint answering this request
// in a way they said is wrong — repeating the request will not change their mind. An
// expression that needs a retry says so by accepting the retryable status instead.
func httpExpectSatisfied(
	ctx context.Context,
	inputs *Task_HTTP_Inputs,
	expectSpec *Value,
	resp *http.Response,
	vars map[string]any,
	scope *Scope,
) error {
	parsed := expectSpec.GetExpr()
	if parsed == nil {
		// A literal expect is a mistake worth naming: `expect: true` would accept
		// every response, including the ones the default rule exists to catch.
		return NewTaskError("http", ErrorKindInvalidInput, fmt.Errorf(
			"expect must be an expression over the response, such as ${response.status_code == 200 || response.status_code == 404}"))
	}

	env, err := httpResponseEnv(scope.GetProfile())
	if err != nil {
		return NewTaskError("http", ErrorKindInternal, err)
	}

	activation, err := interpreter.NewActivation(vars)
	if err != nil {
		return NewTaskError("http", ErrorKindInternal, fmt.Errorf("failed to create activation: %w", err))
	}

	out, err := DefaultEvaluator().EvalParsed(ctx, env, parsed,
		interpreter.NewHierarchicalActivation(
			&StepsOutputActivation{Prev: scope.StepOutputs(), Ctx: ctx, Eval: DefaultEvaluator()},
			activation))
	if err != nil {
		return NewTaskError("http", ErrorKindExpression, fmt.Errorf("evaluating expect: %w", err))
	}

	met, ok := out.Value().(bool)
	if !ok {
		// Guessing at truthiness would mean an expression returning a status code
		// silently accepted every response.
		return NewTaskError("http", ErrorKindInvalidInput, fmt.Errorf(
			"expect must evaluate to a boolean, got %s", out.Type().TypeName()))
	}

	if met {
		return nil
	}

	return NewTaskError("http", ErrorKindInvalidInput, fmt.Errorf(
		"%s %s returned status %d, which the step's expect expression does not accept",
		inputs.GetMethod(), inputs.GetUrl(), resp.StatusCode))
}
