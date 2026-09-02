#!/usr/bin/env bash
set -euo pipefail

# Claude's remote container puts the base image's Go installation first on
# PATH. Persist the toolchain named by go.mod for every Bash command in this
# session, so a bare gofmt agrees with the Makefile and CI.
if [[ -z "${CLAUDE_ENV_FILE:-}" ]]; then
	exit 0
fi

go_version="$(awk '$1 == "go" { print $2; exit }' "${CLAUDE_PROJECT_DIR}/go.mod")"
goroot="$(GOTOOLCHAIN="go${go_version}" go env GOROOT)"
printf 'export PATH=%q:$PATH\n' "${goroot}/bin" >> "${CLAUDE_ENV_FILE}"
