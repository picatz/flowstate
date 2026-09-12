#!/usr/bin/env bash
set -euo pipefail

# Claude's remote container puts the base image's Go installation first on
# PATH. Persist the toolchain named by go.mod for every Bash command in this
# session, so a bare gofmt agrees with the Makefile and CI, and compile the
# guard hooks once so no tool call pays for `go run`.
project_dir="${CLAUDE_PROJECT_DIR:-}"
if [[ -z "${project_dir}" || ! -d "${project_dir}" ]]; then
	printf 'CLAUDE_PROJECT_DIR does not name a checkout directory.\n' >&2
	exit 2
fi

CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/build-hooks.sh"

if [[ -z "${CLAUDE_ENV_FILE:-}" ]]; then
	exit 0
fi

go_version="$(awk '$1 == "go" { print $2; exit }' "${project_dir}/go.mod")"
goroot="$(GOTOOLCHAIN="go${go_version}" go env GOROOT)"
printf 'export PATH=%q:$PATH\n' "${goroot}/bin" >> "${CLAUDE_ENV_FILE}"
