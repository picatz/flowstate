#!/usr/bin/env bash
set -euo pipefail

project_dir="${CLAUDE_PROJECT_DIR:-}"
if [[ -z "${project_dir}" || ! -d "${project_dir}" ]]; then
	printf 'CLAUDE_PROJECT_DIR does not name a checkout directory; restart Claude Code before using tools.\n' >&2
	exit 2
fi

name="${1:-}"
# The merge tool's entry passes `strict`, because refusing a merge there stands
# between nobody and repairing a broken tree.
strict="${2:-}"
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

# Emits the neutral fail-open shape the guards themselves use for a check that
# could not run: the reason on stderr and a systemMessage on stdout, with no
# permission decision, because a blind check must not silently approve either.
warn() {
	local reason="$1" escaped
	escaped="$(printf '%s' "${reason}" | tr '\t\n\r' '   ' | tr -d '\000-\037' | sed 's/\\/\\\\/g; s/"/\\"/g')"
	printf '%s\n' "${reason}" >&2
	printf '{"systemMessage":"%s"}\n' "${escaped}"
	exit 0
}

# Reports whether this call could be a merge, for the one case where the merge
# guard cannot be consulted about it. mergeguard is wired on Bash as well as on
# the merge tool, and it returns immediately from every Bash call that is not a
# merge, so refusing them all when it cannot be built would take away `go
# build`, `git` and `make` — the tools a repair needs — to guard calls the guard
# itself would have ignored. That is a worse failure than the one being
# prevented, and it is not what this entry did before it was prebuilt: `go run`
# on a tree that does not compile exits 1, which Claude Code does not treat as
# a block.
#
# This is deliberately a coarse over-approximation, not a second recognizer:
# tools/hooks/mergeguard/main.go remains the only thing that decides what a
# merge is and which one. Anything that could reach that decision is refused
# here; only a payload that provably cannot is let through.
#
# Reading stdin is safe only because every path that consults this exits
# without running the guard. On every other path the guard is still waiting for
# this payload.
payload_could_merge() {
	local payload
	payload="$(cat)"
	# Not provably an ordinary shell call — the merge tool's own entry, or a
	# payload this could not read. Neither may fail open.
	if ! printf '%s' "${payload}" | grep -Eq '"tool_name"[[:space:]]*:[[:space:]]*"Bash"'; then
		return 0
	fi
	if printf '%s' "${payload}" | grep -Eq 'gh[^"]*[[:space:]]pr[[:space:]]+merge|merge_pull_request'; then
		return 0
	fi
	return 1
}

# Decides the fail-open paths below. A guard that could not be built has not
# refused anything, and these guards match the very tools a repair needs, so
# the default is to warn loudly and let the call through. The exceptions are a
# call site that never fails open, and a call the merge guard would have judged.
may_fail_open() {
	if [[ -n "${strict}" ]]; then
		return 1
	fi
	if [[ "${name}" == "mergeguard" ]] && payload_could_merge; then
		return 1
	fi
	return 0
}

# A stale generation is rebuilt here rather than deferred to a restart. Editing
# a guard's source is ordinary work, and a session that answers it by refusing
# every tool call until Claude Code restarts cannot be used to do that work.
#
# A rebuild that fails because the tree does not compile is the ordinary state
# between two edits of a refactor, and it is the one failure that must not
# block: the guards match Edit, Write and Bash, so denying here would take away
# the tools needed to repair the very file that broke, and a restart would
# rebuild and fail identically. That case warns loudly and lets the call
# through, which is what these hooks did before they were prebuilt and what
# `hook.Warn` exists for. Every other failure denies, because it means the
# build is incoherent rather than merely unfinished.
if ! hooks_are_current; then
	# Captured with `|| status=$?` because `set -e` would otherwise end this
	# script at the failing assignment, before the policy below can tell a
	# tree that does not compile from a build that is incoherent.
	rebuild=""
	rebuild_status=0
	rebuild="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/build-hooks.sh" 2>&1 < /dev/null | tail -c 4096)" || rebuild_status=$?
	case "${rebuild_status}" in
		0) ;;
		3)
			if ! may_fail_open; then
				printf 'Flowstate Claude hook %q could not be rebuilt and this call does not fail open:\n%s\n' "${name}" "${rebuild}" >&2
				exit 2
			fi
			warn "Flowstate Claude hook ${name} did not run: its sources do not compile right now. ${rebuild}"
			;;
		*)
			printf 'Flowstate Claude hook %q is out of date and could not be rebuilt:\n%s\n' "${name}" "${rebuild}" >&2
			exit 2
			;;
	esac
	if ! hooks_are_current; then
		# The build published, so the other guards are current; this one did
		# not compile. Treat it like the whole tree not compiling, for the
		# same reason, unless this call site never fails open.
		if grep -qxF "${name}" "${hook_dir}/.unbuilt" 2>/dev/null; then
			if may_fail_open; then
				warn "Flowstate Claude hook ${name} did not run: its own sources do not compile right now."
			fi
			printf 'Flowstate Claude hook %q does not compile right now, and this call is not one it may skip. Repair %s and retry; other commands still run.\n' \
				"${name}" "tools/hooks/${name}" >&2
			exit 2
		fi
		printf 'Flowstate Claude hook %q is still not current after a rebuild; restart Claude Code.\n' "${name}" >&2
		exit 2
	fi
fi

if ! "${hook_dir}/${name}"; then
	printf 'Flowstate Claude hook %q could not run; restart Claude Code to rebuild the session hooks.\n' "${name}" >&2
	exit 2
fi
