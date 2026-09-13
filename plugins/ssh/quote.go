package main

import (
	"maps"
	"slices"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// An SSH exec request carries a command *line*, not an argv: the far side hands
// the string to the account's shell. That is the protocol, not a choice this
// plugin makes, and it is why the operator's argv is assembled here rather than
// sent as a list.
//
// Everything is quoted - the operator's own arguments as well as a workflow's
// parameters. Quoting only what a workflow filled would make the guarantee
// depend on remembering which half a string came from, and an operator's argv
// holding a space would silently become two arguments.

// quoteArgument renders one argument so a POSIX shell reads it as exactly one
// literal word.
//
// Single quotes suspend every expansion a shell has - variables, globs,
// substitution, redirection, separators - and the only character they cannot
// carry is a single quote itself, which is closed, escaped, and reopened. That
// is the whole rule, and it is the same one Go's own os/exec avoids needing by
// not having a shell at all.
func quoteArgument(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// buildCommandLine fills an operator's argv with a call's parameters and
// renders the command line to send.
//
// A parameter is checked against its grant's pattern before it is substituted
// and quoted after: either alone would be enough for the cases anyone thinks
// of, and the point of having both is the case nobody thought of.
func buildCommandLine(command commandGrant, parameters map[string]string) (string, error) {
	for name := range parameters {
		if _, declared := command.Parameters[name]; !declared {
			return "", sdk.InvalidInput(
				"this command declares no parameter %q; it takes %s", truncate(name, 64), declaredParameters(command))
		}
	}

	filled := make([]string, 0, len(command.Argv))
	for _, argument := range command.Argv {
		rendered, err := fillPlaceholders(command, argument, parameters)
		if err != nil {
			return "", err
		}
		filled = append(filled, quoteArgument(rendered))
	}
	return strings.Join(filled, " "), nil
}

// fillPlaceholders substitutes one argv element's placeholders.
func fillPlaceholders(command commandGrant, argument string, parameters map[string]string) (string, error) {
	var refusal error

	rendered := placeholderPattern.ReplaceAllStringFunc(argument, func(match string) string {
		name := placeholderPattern.FindStringSubmatch(match)[1]

		value, supplied := parameters[name]
		if !supplied {
			// Missing rather than empty: an unsupplied parameter that rendered
			// as "" would run the command with an argument the author never
			// wrote, which is how `systemctl restart ''` happens.
			if refusal == nil {
				refusal = sdk.InvalidInput("parameter %q is required by this command and was not supplied", name)
			}
			return ""
		}

		grant := command.Parameters[name]
		limit := grant.MaxBytes
		if limit == 0 {
			limit = 256
		}
		if len(value) > limit {
			if refusal == nil {
				refusal = sdk.InvalidInput("parameter %q is %d bytes, over this command's limit of %d", name, len(value), limit)
			}
			return ""
		}
		if grant.compiled == nil || !grant.compiled.MatchString(value) {
			// The value is not echoed. It failed the operator's pattern, which
			// makes it exactly the kind of value not to interpolate into a
			// message that lands in durable history.
			if refusal == nil {
				refusal = sdk.InvalidInput("parameter %q does not match the pattern this command requires of it", name)
			}
			return ""
		}
		return value
	})

	if refusal != nil {
		return "", refusal
	}
	return rendered, nil
}

// declaredParameters renders what a command does take, so a refusal tells the
// author what to write instead.
func declaredParameters(command commandGrant) string {
	if len(command.Parameters) == 0 {
		return "no parameters"
	}

	// Sorted, so a refusal's wording does not depend on map iteration.
	return strings.Join(slices.Sorted(maps.Keys(command.Parameters)), ", ")
}
