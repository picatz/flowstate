package flowstatev1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// httpStubResponseFn is the http task's [TaskDef.StubResponseFn]: answer an
// invocation from a response a test declared, running everything the task
// does once it holds a status, headers, and a body — `expect:` deciding
// success, JSON parsing under the element bound, and the step's own
// `outputs:` shaping — through the same two halves the live path runs
// ([httpPreparedInputs], [httpAnswerFromResponse]). A stubbed response
// therefore exercises the same expressions a live one does, which is the
// point: `returns:` supplies post-shaping outputs and leaves the mapping —
// the exact place a path typo lives — dead code in every green suite (#925).
//
// Everything transport stays absent, deliberately: no request, no egress
// policy, no redirect checks, no credential scrub — a declared response
// carries no secret a peer could have reflected. Those are properties of a
// deployment's network, which `flow test` never touches (#155).
func httpStubResponseFn(ctx context.Context, input map[string]*Value, scope *Scope, response map[string]*Value) (*Node_Outputs, error) {
	taskInputs, outputsExpr, expectSpec, _, err := httpPreparedInputs(ctx, input, scope)
	if err != nil {
		return nil, err
	}

	statusCode, header, body, err := decodeHTTPStubResponse(response)
	if err != nil {
		return nil, NewTaskError("http", ErrorKindInvalidInput, err)
	}

	return httpAnswerFromResponse(ctx, taskInputs, outputsExpr, expectSpec,
		&http.Response{StatusCode: statusCode, Header: header}, body, scope)
}

// decodeHTTPStubResponse reads the three fields a declared http response may
// carry, refusing a name it does not define — a misspelled `staus_code:`
// silently defaulting to 200 would be the silent-nothing failure CLAUDE.md's
// "diagnostics are a feature" forbids.
//
//   - `status_code`: an integer; 200 when omitted, the ordinary happy path.
//   - `body`: a string, verbatim — or a map or list, encoded as its JSON,
//     which is the shape a JSON API answers with and what `parse_json: true`
//     then reads back through `response.json`.
//   - `headers`: a map of string to string, one value per name.
func decodeHTTPStubResponse(response map[string]*Value) (statusCode int, header http.Header, body []byte, err error) {
	statusCode = http.StatusOK

	for name, value := range response {
		native, convErr := LiteralToGo(value.GetLiteral())
		if convErr != nil {
			return 0, nil, nil, fmt.Errorf("response %q: %w", name, convErr)
		}

		switch name {
		case "status_code":
			code, ok := native.(int64)
			if !ok {
				return 0, nil, nil, fmt.Errorf("response status_code must be an integer, got %T", native)
			}
			// The HTTP wire range, before any narrowing conversion: Go's own
			// response parser accepts exactly three digits, so anything else
			// is a response no transport could have produced — and the
			// unchecked int64 would otherwise wrap through the int32 the
			// default outputs carry, so a stub declaring 4294967496 would
			// read back as a 200 and could satisfy a success expectation
			// (Codex, #982).
			if code < 100 || code > 999 {
				return 0, nil, nil, fmt.Errorf(
					"response status_code must be a three-digit HTTP status (100-999), got %d — "+
						"a value no HTTP response could carry", code)
			}
			statusCode = int(code)
		case "body":
			switch b := native.(type) {
			case string:
				body = []byte(b)
			case map[string]any, []any:
				encoded, encErr := json.Marshal(b)
				if encErr != nil {
					return 0, nil, nil, fmt.Errorf("response body: encoding the structured value as JSON: %w", encErr)
				}
				body = encoded
			default:
				return 0, nil, nil, fmt.Errorf(
					"response body must be a string, or a map or list to encode as JSON, got %T", native)
			}
		case "headers":
			entries, ok := native.(map[string]any)
			if !ok {
				return 0, nil, nil, fmt.Errorf("response headers must be a map of names to string values, got %T", native)
			}
			header = make(http.Header, len(entries))
			for headerName, headerValue := range entries {
				text, ok := headerValue.(string)
				if !ok {
					return 0, nil, nil, fmt.Errorf("response header %q must be a string, got %T", headerName, headerValue)
				}
				header.Set(headerName, text)
			}
		default:
			known := []string{"body", "headers", "status_code"}
			sort.Strings(known)
			return 0, nil, nil, fmt.Errorf(
				"the http task's response: takes %s; %q is not one of them",
				strings.Join(known, ", "), name)
		}
	}

	return statusCode, header, body, nil
}
