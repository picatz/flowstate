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
if [[ ! -f "${hook_dir}/.ready" || ! -x "${hook_dir}/${name}" ]]; then
	printf 'Flowstate Claude hook %q is not ready; restart Claude Code to rebuild the session hooks.\n' "${name}" >&2
	exit 2
fi
if ! source_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${hook_dir}/.source-dirs")" ||
	[[ ! -f "${hook_dir}/.source-id" || "$(<"${hook_dir}/.source-id")" != "${source_id}" ]]; then
	printf 'Flowstate Claude hook sources changed; restart Claude Code to rebuild the session hooks.\n' >&2
	exit 2
fi

if ! "${hook_dir}/${name}"; then
	printf 'Flowstate Claude hook %q could not run; restart Claude Code to rebuild the session hooks.\n' "${name}" >&2
	exit 2
fi
