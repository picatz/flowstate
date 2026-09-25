package main

import (
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// maxFilterBytes bounds a filter expression. RFC 7644 section 3.4.2.2 sets no
// length, and a provider's parser is the thing on the other end of this value.
const maxFilterBytes = 1024

// escapeFilterValue renders a value as a SCIM string literal.
//
// This is the same rule plugins/sql applies to a query parameter and for the
// same reason: the value is data, and a value that can end its own quoting is
// not data any more. RFC 7644 section 3.4.2.2 defines a filter's string literal
// as a JSON string, so a quote and a backslash are what have to be escaped, and
// a control character - which a JSON string may not carry raw - is refused
// rather than encoded, because a user name holding one is not a user name.
func escapeFilterValue(value string) (string, error) {
	if strings.ContainsFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return "", sdk.InvalidInput("the value holds a control character, which a SCIM filter literal cannot carry")
	}

	replaced := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value)
	return `"` + replaced + `"`, nil
}

// userNameFilter builds the exact-match filter scim.user_get uses.
//
// The filter is built here rather than taken as an input so that a user name is
// never filter syntax: an attacker-chosen name containing a quote would
// otherwise change which users the expression selects.
func userNameFilter(userName string) (string, error) {
	literal, err := escapeFilterValue(userName)
	if err != nil {
		return "", err
	}
	return "userName eq " + literal, nil
}

// checkFilter bounds and shape-checks a filter a workflow wrote.
//
// It is deliberately not a parser. The provider evaluates the expression and is
// the authority on its grammar; what this does is keep a value that is not a
// filter at all - a newline-injected header, a megabyte of text - from being
// sent as one. What the filter can reach is bounded by the credential, not by
// this check, and the README says so.
func checkFilter(filter string) (string, error) {
	if filter == "" {
		return "", nil
	}
	if len(filter) > maxFilterBytes {
		return "", sdk.InvalidInput("filter is %d bytes, over the %d-byte limit", len(filter), maxFilterBytes)
	}
	if strings.ContainsFunc(filter, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return "", sdk.InvalidInput("filter holds a control character")
	}
	return filter, nil
}
