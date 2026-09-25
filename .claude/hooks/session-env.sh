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

# The toolchain pin is written first, and unconditionally. It is the older of
# this script's two jobs and the one the session cannot recover on its own: a
# build that fails because the tree does not compile is an ordinary state
# between two edits, and letting it abort this script would leave every Bash
# command in the session resolving `go` and `gofmt` from the base image, which
# is exactly the drift AGENTS.md warns about, for as long as the session lives.
go_version="$(awk '$1 == "go" { print $2; exit }' "${project_dir}/go.mod")"
goroot="$(GOTOOLCHAIN="go${go_version}" go env GOROOT)"
if [[ -n "${CLAUDE_ENV_FILE:-}" ]]; then
	printf 'export PATH=%q:$PATH\n' "${goroot}/bin" >> "${CLAUDE_ENV_FILE}"
fi

# Then the hooks, reporting their own failure. Prebuilding here is an
# optimization: the launcher rebuilds on demand, so a session that starts on a
# tree mid-refactor is usable and repairs itself on the next tool call.
build_status=0
CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/build-hooks.sh" || build_status=$?
exit "${build_status}"
