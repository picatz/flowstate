package flowstatev1

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/picatz/flowstate/internal/textbound"
)

// An InputClaim is every claim a task's input message sets on one of its own
// top-level fields with the `flowstate.v1.input` option.
//
// It is the one reading of that option: [SecretInputClaims] and
// [LiteralInputClaims] project it, and a claim added to the option adds a field
// here and a case in [InputClaims], not another walker over the descriptor.
type InputClaim struct {
	// Name is the field's name.
	Name string

	// Secret is the field's secret claim, [Secret_SECRET_UNSPECIFIED] for none. A
	// credential claim implies [Secret_SECRET_REQUIRED], so it is that here.
	Secret Secret

	// Credential is the name of the plugin credential the field receives, from
	// the `credential` claim; empty for none. See [CheckPluginCredentials] for
	// the check that it names a declared one.
	Credential string

	// Literal lists the fields at or under this one that claim to be literals,
	// as dotted paths relative to the message (this field's own name first), in
	// declaration order. See [LiteralInputClaims].
	Literal []string
}

// InputClaims reads the claims a task's input message declares, one
// [InputClaim] per top-level field that sets any, in declaration order.
//
// It fails closed: a claim on a field of the wrong shape, a number the host
// does not know, or a literal claim beside a secret claim (a literal is text the
// author typed and a secret is never that) is an error rather than a claim read
// charitably. A whole-value or required secret claim needs a
// `flowstate.v1.Value` field, which holds a reference unresolved, or a string
// field, which a plugin task receives the resolved value in; a nested secret
// claim needs a Value or a map of them (a map of strings is accepted, since that
// is how a descriptor documents a mapping the task decodes as a structure). A
// literal claim is valid on a string at any depth: see [LiteralInputClaims].
//
// A credential claim implies SECRET_REQUIRED and so has the whole-value shape
// rule; it is an error beside SECRET_WHOLE_VALUE or SECRET_NESTED, which say the
// same thing less strictly, beside a literal claim, and with a name that is not
// a [ValidCredentialName].
func InputClaims(md protoreflect.MessageDescriptor) ([]InputClaim, error) {
	if md == nil {
		return nil, nil
	}

	var claims []InputClaim
	visits := 0 // shared by every field: the budget is the message's
	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		name := string(fd.Name())

		secret := Secret_SECRET_UNSPECIFIED
		credential := ""
		if input, _ := proto.GetExtension(fd.Options(), E_Input).(*InputOptions); input != nil {
			secret = input.GetSecret()
			credential = input.GetCredential()
		}

		if credential != "" {
			if !ValidCredentialName(credential) {
				return nil, fmt.Errorf("input %q claims credential %q, which is not a name matching ^[a-z][a-z0-9_]{0,31}$",
					name, textbound.Truncate(credential, 64))
			}
			if secret != Secret_SECRET_UNSPECIFIED && secret != Secret_SECRET_REQUIRED {
				return nil, fmt.Errorf("input %q claims credential %q and also %s; a credential is always SECRET_REQUIRED",
					name, credential, secret)
			}
			secret = Secret_SECRET_REQUIRED
		}

		switch secret {
		case Secret_SECRET_UNSPECIFIED:
		case Secret_SECRET_WHOLE_VALUE, Secret_SECRET_REQUIRED:
			if !isValueField(fd) && !isStringField(fd) {
				return nil, fmt.Errorf("input %q declares %s but is neither a flowstate.v1.Value nor a string", name, secret)
			}
		case Secret_SECRET_NESTED:
			if !isValueField(fd) && !isValueMapField(fd) {
				return nil, fmt.Errorf("input %q declares %s but is neither a flowstate.v1.Value nor a map of them", name, secret)
			}
		default:
			return nil, fmt.Errorf("input %q declares unknown secret claim %d", name, int32(secret))
		}

		var literal []string
		if err := collectFieldLiteralClaims(fd, "", map[protoreflect.FullName]bool{md.FullName(): true}, 0, &visits, &literal); err != nil {
			return nil, err
		}
		if len(literal) > 0 && credential != "" {
			return nil, fmt.Errorf("input %q claims credential %q and also a literal claim, which cannot both hold", name, credential)
		}
		if len(literal) > 0 && secret != Secret_SECRET_UNSPECIFIED {
			return nil, fmt.Errorf("input %q declares %s and also a literal claim, which cannot both hold", name, secret)
		}

		if secret != Secret_SECRET_UNSPECIFIED || len(literal) > 0 {
			claims = append(claims, InputClaim{Name: name, Secret: secret, Credential: credential, Literal: literal})
		}
	}

	return claims, nil
}

// SecretInputClaims reads the secret claims a task's input message declares on
// its own fields with the `flowstate.v1.input` option, as the three name sets
// [TaskDef] carries: whole-value inputs, the subset of those that are required,
// and inputs a secret reference may sit nested inside.
//
// It is [InputClaims] projected: only the message's top-level fields are read,
// in declaration order, and each list is sorted and deduplicated so it digests
// the same however it was declared. SECRET_REQUIRED implies SECRET_WHOLE_VALUE,
// so a required name is in both whole and required. A field with no claim is in
// none.
func SecretInputClaims(md protoreflect.MessageDescriptor) (whole, required, nested []string, err error) {
	claims, err := InputClaims(md)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, c := range claims {
		switch c.Secret {
		case Secret_SECRET_WHOLE_VALUE:
			whole = append(whole, c.Name)
		case Secret_SECRET_REQUIRED:
			whole = append(whole, c.Name)
			required = append(required, c.Name)
		case Secret_SECRET_NESTED:
			nested = append(nested, c.Name)
		}
	}

	return canonicalStrings(whole), canonicalStrings(required), canonicalStrings(nested), nil
}

const valueFullName protoreflect.FullName = "flowstate.v1.Value"

// isValueField reports whether fd is a single flowstate.v1.Value.
func isValueField(fd protoreflect.FieldDescriptor) bool {
	return !fd.IsList() && !fd.IsMap() && fd.Kind() == protoreflect.MessageKind && fd.Message().FullName() == valueFullName
}

// isValueMapField reports whether fd is a map whose values are Values or
// strings.
func isValueMapField(fd protoreflect.FieldDescriptor) bool {
	if !fd.IsMap() {
		return false
	}
	v := fd.MapValue()
	return v.Kind() == protoreflect.StringKind || (v.Kind() == protoreflect.MessageKind && v.Message().FullName() == valueFullName)
}

// isStringField reports whether fd is a single string.
func isStringField(fd protoreflect.FieldDescriptor) bool {
	return !fd.IsList() && !fd.IsMap() && fd.Kind() == protoreflect.StringKind
}
