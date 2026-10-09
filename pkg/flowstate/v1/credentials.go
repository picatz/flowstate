package flowstatev1

import (
	"fmt"
	"regexp"
	"slices"
	"unicode/utf8"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/picatz/flowstate/internal/textbound"
)

// MaxPluginCredentials bounds the credentials one plugin declares. It matches
// the max_items rule on PluginManifest.credentials and
// PluginDescription.credentials, which a message read off the wire is held to by
// [Validate] but a descriptor built in process is not.
const MaxPluginCredentials = 8

// maxCredentialDescription, in characters (protovalidate counts code points), matches the max_len rule on
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
		if utf8.RuneCountInString(d.GetDescription()) > maxCredentialDescription {
			return fmt.Errorf("credential %q has a description of %d characters, more than %d", name, utf8.RuneCountInString(d.GetDescription()), maxCredentialDescription)
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

// CredentialFederated reports whether def's plugin declares the named credential
// federated, as def carries it ([TaskDef.FederatedCredentials]).
func CredentialFederated(def TaskDef, credential string) bool {
	return slices.Contains(def.FederatedCredentials, credential)
}

// FederatedCredentialsOf lists, from claims read by [InputClaims], the
// credentials the task claims that declarations mark federated, sorted. It is
// what a host sets [TaskDef.FederatedCredentials] from, with the plugin's
// declarations in hand.
func FederatedCredentialsOf(claims []InputClaim, declarations []*CredentialDeclaration) []string {
	var out []string
	for _, c := range claims {
		if c.Credential == "" || slices.Contains(out, c.Credential) {
			continue
		}
		for _, d := range declarations {
			if d.GetName() == c.Credential && d.GetFederated() {
				out = append(out, c.Credential)

				break
			}
		}
	}
	slices.Sort(out)

	return out
}

// CredentialReferenceMatches reports whether value is the reference the
// credential's declaration allows: a federated credential is bound by a whole
// `${credential()}` reference and never a stored secret, and any other by a whole
// `${secret()}` reference and never a credential reference. A literal, an
// expression or a nested reference matches neither. One rule for the compiler,
// admission and dispatch, so the three cannot disagree about which spelling a
// credential takes.
func CredentialReferenceMatches(federated bool, value *Value) bool {
	if federated {
		return value.GetCredentialRef() != nil
	}

	return value.GetSecretRef() != nil
}

// CredentialReferenceMessage is the sentence every refusal of a credential input
// written with the wrong kind of reference says, from the compiler, admission and
// dispatch alike. It names the credential and what it takes, and never the
// reference written.
func CredentialReferenceMessage(taskName, input, credential string, federated bool) string {
	if federated {
		return fmt.Sprintf(
			"task %q input %q receives the plugin's federated credential %q, which takes a whole credential reference such as ${credential('target')}, never a stored secret",
			taskName, input, credential)
	}

	return fmt.Sprintf(
		"task %q input %q receives the plugin's credential %q, which is not federated and takes a whole secret reference such as ${secret('env:NAME')}, never a credential reference",
		taskName, input, credential)
}
