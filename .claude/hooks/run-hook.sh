#!/usr/bin/env bash
set -euo pipefail

project_dir="${CLAUDE_PROJECT_DIR:-}"
if [[ -z "${project_dir}" || ! -d "${project_dir}" ]]; then
	printf 'CLAUDE_PROJECT_DIR does not name a checkout directory; restart Claude Code before using tools.\n' >&2
	exit 2
fi

name="${1:-}"
case "${name}" in
	genguard | gofmtcheck | pidguard | mergeguard) ;;
	*)
		printf 'unknown Flowstate Claude hook %q\n' "${name}" >&2
		exit 2
		;;
esac

hook_dir="${project_dir}/.claude/hooks/.bin"

# Reports whether the published generation is complete and was compiled from
# the sources present now. Anything it cannot establish is not current.
hooks_are_current() {
	[[ -f "${hook_dir}/.ready" && -x "${hook_dir}/${name}" && -s "${hook_dir}/.source-dirs" ]] || return 1
	local source_id
	# Read nothing from stdin: the guard downstream is still waiting for the
	# tool-call payload Claude Code piped in, and a check that consumed it
	# would hand the guard an empty decision to make.
	source_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${hook_dir}/.source-dirs" < /dev/null)" || return 1
	[[ -f "${hook_dir}/.source-id" && "$(<"${hook_dir}/.source-id")" == "${source_id}" ]]
}

# A stale generation is rebuilt here rather than deferred to a restart. Editing
# a guard's source is ordinary work, and a session that answers it by refusing
# every tool call until Claude Code restarts cannot be used to do that work.
# The rebuild is what must succeed: if it does not, this denies, so a guard
# whose sources have changed never runs from the previous build.
if ! hooks_are_current; then
	if ! rebuild="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/build-hooks.sh" 2>&1 < /dev/null)"; then
		printf 'Flowstate Claude hook %q is out of date and could not be rebuilt:\n%s\n' "${name}" "${rebuild}" >&2
		exit 2
	fi
	if ! hooks_are_current; then
		printf 'Flowstate Claude hook %q is still not current after a rebuild; restart Claude Code.\n' "${name}" >&2
		exit 2
	fi
fi

if ! "${hook_dir}/${name}"; then
	printf 'Flowstate Claude hook %q could not run; restart Claude Code to rebuild the session hooks.\n' "${name}" >&2
	exit 2
fi
