#!/usr/bin/env bash
set -euo pipefail

project_dir="${CLAUDE_PROJECT_DIR:-}"
if [[ -z "${project_dir}" || ! -d "${project_dir}" ]]; then
	printf 'CLAUDE_PROJECT_DIR does not name a checkout directory; restart Claude Code before using tools.\n' >&2
	exit 2
fi
# Resolved the same way build-hooks.sh resolves it, so both hand source-id.sh
# the same root. A package directory named "." is walked from the root itself,
# and a walk rooted at a symbolic link finds none of its files, so disagreeing
# here would mean the two never compute the same identity.
if ! project_dir="$(cd "${project_dir}" && pwd -P)"; then
	printf 'CLAUDE_PROJECT_DIR could not be resolved; restart Claude Code before using tools.\n' >&2
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
# This is the backstop, not the recognizer: a coarse over-approximation whose
# completeness is not claimed. The recognizer is the retained guard consulted
# before it, and this runs only when there is none or it found nothing. A text
# test cannot equal a tokenizer, so some spelling will always reach past this;
# what it covers is every spelling a caller writes without trying to evade it,
# and every spelling the retained guard is too old to know about.
# `./tools/hooks/mergeguard` is not the word, which keeps the repair of this
# very guard runnable; `git merge` is, and refusing it for the seconds this
# guard cannot be built costs a repair nothing.
#
# Reading stdin is safe only because every path that consults this exits
# without running the guard. On every other path the guard is still waiting for
# this payload.
# The tool-call payload, read once. Two readers need it now -- the retained
# guard and the text backstop -- and stdin can only be read once, so this holds
# it. `payload_truncated` is kept separately because an empty payload and a
# payload too large to judge are different facts that both fail closed.
payload_text=""
payload_truncated=""
payload_read=""
read_payload() {
	if [[ -n "${payload_read}" ]]; then
		return 0
	fi
	payload_read=1
	# Bounded, as hook.Read bounds the same stdin, though tighter than its
	# 16 MiB: no tool call this decides on is a megabyte. A payload at the bound
	# was truncated, and a decision read off a truncated payload is not evidence.
	# The trailing marker survives command substitution stripping newlines, so a
	# payload ending in one still measures its true length.
	payload_text="$(
		head -c 1048576
		printf x
	)"
	payload_text="${payload_text%x}"
	# Bytes, not characters: the bound is a byte count, and in a UTF-8 locale
	# ${#payload_text} would measure a truncated payload short.
	local LC_ALL=C
	if [[ "${#payload_text}" -ge 1048576 ]]; then
		payload_truncated=1
	fi
}

payload_could_merge() {
	local LC_ALL=C stripped status
	read_payload
	if [[ -n "${payload_truncated}" ]]; then
		return 0
	fi
	# Not provably an ordinary shell call — the merge tool's own entry, or a
	# payload this could not read. Neither may fail open.
	if ! printf '%s' "${payload_text}" | grep -Eq '"tool_name"[[:space:]]*:[[:space:]]*"Bash"'; then
		return 0
	fi
	# Also matched with the things the guard's tokenizer drops removed: the JSON
	# escapes that carry a line continuation, then quoting and backslashes. A
	# word split by any of them is one word to the guard. The strip can only
	# join characters that were already adjacent, so it cannot manufacture the
	# word across two JSON fields, and none of the commands a repair needs grows
	# it -- both directions are pinned by the tables in the launcher's test.
	stripped="$(printf '%s' "${payload_text}" | sed 's/\\\\[nrt]//g' | tr -d '\\"'"'")"
	# grep's status is read rather than tested, because 0 and 1 are answers and
	# anything above is a failure to answer. Treating an error as "no match"
	# would make a broken grep quietly the same as a safe command.
	status=0
	printf '%s\n%s' "${payload_text}" "${stripped}" |
		grep -Eqi '(^|[^[:alnum:]])merge([^[:alnum:]_]|$)|merge_pull_request' || status=$?
	if [[ "${status}" -ne 1 ]]; then
		return 0
	fi
	return 1
}

# Asks the last merge guard that compiled about this call, so the decision is
# made by the recognizer rather than by a text test standing in for it. The
# guard tokenizes a command the way a shell would; no pattern here can equal
# that, which is why every round of review found one more spelling that reached
# past the backstop below.
#
# What is retained is the binary from the previous successful build, so its
# rules can be older than the sources being repaired. That is sound in the
# direction that matters: a refusal it issues is a real recognition, and
# anything it is too old to recognize still meets the backstop. It is consulted
# only while the current sources will not compile.
#
# A decision is passed through exactly as the guard wrote it, so the operator
# reads the guard's own reasons; the note saying it came from a retained binary
# goes to stderr, which is shown alongside a refusal.
retained_guard="${project_dir}/.claude/hooks/.lkg/${name}"
retained_denies() {
	local decision status=0
	[[ "${name}" == "mergeguard" && -x "${retained_guard}" ]] || return 1
	read_payload
	if [[ -n "${payload_truncated}" ]]; then
		return 1
	fi
	# Fed from a here-string rather than a pipe, so `status` is the guard's own
	# exit code. Under `pipefail` a pipeline reports the writer's death too, and
	# a retained guard that exits without draining a large payload would kill the
	# writer with SIGPIPE -- discarding a refusal it had already made. Today's
	# guard always drains, but a retained binary is by design one the launcher
	# cannot inspect.
	decision="$("${retained_guard}" <<< "${payload_text}" 2>/dev/null)" || status=$?
	# A retained binary that cannot run -- built for another platform, or
	# truncated -- has not judged anything. The backstop still applies.
	if [[ "${status}" -ne 0 ]]; then
		return 1
	fi
	if [[ "${decision}" != *'"permissionDecision"'*'"deny"'* ]]; then
		return 1
	fi
	# Names the sources that build came from, so the operator can see which one
	# decided rather than being told only that some earlier one did. Recorded
	# beside the binary in the same rename, and absent only if the directory
	# predates that.
	local built_from="an earlier build"
	if [[ -r "${retained_guard%/*}/.source-id" ]]; then
		built_from="the build of $(<"${retained_guard%/*}/.source-id")"
	fi
	printf 'Flowstate Claude hook %q refused this call. Its current sources do not compile, so the decision was made by %s, the last one that did.\n' \
		"${name}" "${built_from}" >&2
	printf '%s\n' "${decision}"
	exit 0
}

# Decides the fail-open paths below. A guard that could not be built has not
# refused anything, and these guards match the very tools a repair needs, so
# the default is to warn loudly and let the call through. The exceptions are a
# call site that never fails open, and a call the merge guard would have judged.
may_fail_open() {
	if [[ -n "${strict}" ]]; then
		return 1
	fi
	if [[ "${name}" != "mergeguard" ]]; then
		return 0
	fi
	# The recognizer first: it exits when it refuses. Only what it did not
	# refuse, or could not be asked about, reaches the text backstop.
	retained_denies || true
	if payload_could_merge; then
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
