package flowstatev1

import (
	"fmt"
	"regexp"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/picatz/flowstate/internal/textbound"
)

// MaxPluginCredentials bounds the credentials one plugin declares. It matches
// the max_items rule on PluginManifest.credentials and
// PluginDescription.credentials, which a message read off the wire is held to by
// [Validate] but a descriptor built in process is not.
const MaxPluginCredentials = 8

// maxCredentialDescription matches the max_len rule on
// [CredentialDeclaration].description.
const maxCredentialDescription = 256

// credentialName is the spelling of a credential name, the pattern on
// [CredentialDeclaration].name. It is stated here too because the same name
// arrives as a string in a field option, which protovalidate does not see.
var credentialName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// ValidCredentialName reports whether name may name a plugin credential, either
// in a [CredentialDeclaration] or in the `credential` input claim.
func ValidCredentialName(name string) bool {
	return len(name) <= 32 && credentialName.MatchString(name)
}

// CredentialInputs maps each input that claims a plugin credential to the
// credential's name, from claims read by [InputClaims]. It is nil when none does.
func CredentialInputs(claims []InputClaim) map[string]string {
	var out map[string]string
	for _, c := range claims {
		if c.Credential == "" {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[c.Name] = c.Credential
	}

	return out
}

// CheckPluginCredentials applies the credential lattice to one plugin: its
// declarations are bounded, well-named and unique, every input in tasks that
// claims a credential names a declared one, and every declaration is named by
// at least one input.
//
// Each direction is a refusal because the other would be a silent loss. A claim
// naming nothing declared would read as protected and not be; a declaration no
// input names would tell an operator and a catalog the plugin takes a credential
// it never asks for. tasks is each task's input message, nil for a task with
// none. The work is bounded by the inputs themselves: one top-level read of each
// message and at most [MaxPluginCredentials] declarations.
//
// Declaring a credential grants nothing: the host still resolves only a secret
// reference an author wrote.
func CheckPluginCredentials(declarations []*CredentialDeclaration, tasks []protoreflect.MessageDescriptor) error {
	if len(declarations) > MaxPluginCredentials {
		return fmt.Errorf("declares %d credentials, more than the %d a plugin may", len(declarations), MaxPluginCredentials)
	}

	used := make(map[string]bool, len(declarations))
	for _, d := range declarations {
		name := d.GetName()
		if !ValidCredentialName(name) {
			return fmt.Errorf("credential %q is not a name matching %s", textbound.Truncate(name, 64), credentialName)
		}
		if len(d.GetDescription()) > maxCredentialDescription {
			return fmt.Errorf("credential %q has a description of %d bytes, more than %d", name, len(d.GetDescription()), maxCredentialDescription)
		}
		if _, dup := used[name]; dup {
			return fmt.Errorf("credential %q is declared twice", name)
		}
		used[name] = false
	}

	for _, md := range tasks {
		claims, err := InputClaims(md)
		if err != nil {
			return err
		}
		for _, c := range claims {
			if c.Credential == "" {
				continue
			}
			if _, ok := used[c.Credential]; !ok {
				return fmt.Errorf("input %q of %s claims credential %q, which the plugin does not declare",
					c.Name, md.FullName(), textbound.Truncate(c.Credential, 64))
			}
			used[c.Credential] = true
		}
	}

	for _, d := range declarations {
		if !used[d.GetName()] {
			return fmt.Errorf("credential %q is declared but no task input claims it", d.GetName())
		}
	}

	return nil
}
