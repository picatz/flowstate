package main

import (
	"regexp"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// maxUsernameBytes bounds the account name that travels beside a credential.
const maxUsernameBytes = 256

// platformPattern is os/arch with an optional variant, the three fields a
// descriptor's platform object carries that a caller can reasonably state.
var platformPattern = regexp.MustCompile(`^[a-z0-9]+(\.[a-z0-9]+)*/[a-z0-9]+(\.[a-z0-9]+)*(/[a-zA-Z0-9]+)?$`)

// platform is a requested platform, already split.
type platform struct {
	os      string
	arch    string
	variant string
}

// requested reports whether the caller asked for one at all.
func (p platform) requested() bool { return p.os != "" }

// String renders it the way it was written.
func (p platform) String() string {
	if p.variant != "" {
		return p.os + "/" + p.arch + "/" + p.variant
	}
	return p.os + "/" + p.arch
}

// matches reports whether a descriptor's platform satisfies this request. A
// request with no variant matches any variant; a request with one matches only
// that variant, because a caller who wrote arm64/v8 meant it.
func (p platform) matches(os, arch, variant string) bool {
	if p.os != os || p.arch != arch {
		return false
	}
	return p.variant == "" || p.variant == variant
}

// parsePlatform reads os/arch[/variant], or nothing.
func parsePlatform(raw string) (platform, error) {
	if raw == "" {
		return platform{}, nil
	}
	if !platformPattern.MatchString(raw) {
		return platform{}, sdk.InvalidInput(
			"platform %q is not os/arch or os/arch/variant, such as linux/amd64 or linux/arm64/v8",
			truncate(raw, 64))
	}

	parts := strings.Split(raw, "/")
	parsed := platform{os: parts[0], arch: parts[1]}
	if len(parts) == 3 {
		parsed.variant = parts[2]
	}
	return parsed, nil
}

// credentialsFrom reads the account and the resolved secret this call acts as.
//
// The password arrives as a value because the host resolved it: a task naming
// an input in RequiredSecretInputs never receives a reference, and a literal is
// refused before the call is made, so a registry credential cannot enter
// durable history by being written into a Flowfile.
func credentialsFrom(username string, password *flowstatev1.Value) (credentials, error) {
	if len(username) > maxUsernameBytes {
		return credentials{}, sdk.InvalidInput("username is %d bytes, over the %d-byte limit", len(username), maxUsernameBytes)
	}

	secret, err := secretString(password)
	if err != nil {
		return credentials{}, err
	}
	if secret == "" && username != "" {
		return credentials{}, sdk.InvalidInput(
			"username is set and password is not; an anonymous read names neither, and an authenticated one names both")
	}
	if secret != "" && username == "" {
		// Some registries take a token as the password against a fixed
		// username, and every one of them documents which - so the refusal
		// names the fix rather than inventing a default account.
		return credentials{}, sdk.InvalidInput(
			"password is set and username is not; registries that take a token as the password document the account " +
				"name to send with it, such as \"oauth2accesstoken\" or the token's own owner")
	}

	return credentials{username: username, password: secret}, nil
}

// secretString reads a resolved secret input, refusing anything that is not one.
func secretString(value *flowstatev1.Value) (string, error) {
	if value == nil {
		return "", nil
	}

	switch kind := value.GetKind().(type) {
	case nil:
		return "", nil
	case *flowstatev1.Value_Literal:
		text, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok {
			// A resolved-but-absent secret arrives as a null literal, which is
			// how "this optional credential was not supplied" reaches a task.
			if _, isNull := kind.Literal.GetKind().(*expr.Value_NullValue); isNull {
				return "", nil
			}
			return "", sdk.InvalidInput("password must resolve to a string")
		}
		if len(text.StringValue) > maxCredentialBytes {
			return "", sdk.InvalidInput("password resolves to %d bytes, over the %d-byte limit", len(text.StringValue), maxCredentialBytes)
		}
		return text.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed(
			"password reached this plugin as an unresolved secret reference; the host resolves required secret inputs before a plugin runs")
	default:
		return "", sdk.InvalidInput("password must resolve to a string")
	}
}
