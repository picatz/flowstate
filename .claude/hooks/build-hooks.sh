#!/usr/bin/env bash
set -euo pipefail

# Compiles the Flowstate guard hooks once and publishes them as a generation
# that the launcher can check. SessionStart runs this, and so does the launcher
# when it finds the published generation stale, so an edit to a hook's own
# source costs the next tool call a rebuild rather than costing the session a
# restart. There is one implementation of the build because a second one would
# be a second answer to what the hooks are compiled from.

project_dir="${CLAUDE_PROJECT_DIR:-}"
if [[ -z "${project_dir}" || ! -d "${project_dir}" ]]; then
	printf 'CLAUDE_PROJECT_DIR does not name a checkout directory.\n' >&2
	exit 2
fi
# Resolved once, because `go list` reports a package directory with every
# symbolic link already followed. Comparing those against an unresolved root
# classified every first-party package as outside the checkout the moment any
# component of the path was a link, which is ordinary on macOS, where a
# temporary directory lives under /var -> /private/var.
if ! project_dir="$(cd "${project_dir}" && pwd -P)"; then
	printf 'CLAUDE_PROJECT_DIR could not be resolved.\n' >&2
	exit 2
fi

hooks=(genguard gofmtcheck pidguard mergeguard)
packages=()
for hook in "${hooks[@]}"; do
	packages+=("./tools/hooks/${hook}")
done

hook_dir="${project_dir}/.claude/hooks/.bin"
cache_dir="${project_dir}/.claude/hooks/.cache"
# The last merge guard that compiled, kept across generations so a tree that
# does not compile still has one to consult.
retained_dir="${project_dir}/.claude/hooks/.lkg"
install -d -m 0700 "${cache_dir}"

# Exit 3 says the tree does not compile right now, which is an ordinary state
# between two edits of a refactor. The launcher answers that differently from
# every other failure here, because a guard that cannot be built has not
# refused anything and must not take the session down with it.
readonly not_buildable=3

# One builder at a time: the guards run on every matching tool call, so two
# launchers can find the same generation stale at once. `mkdir` is the mutex
# because it is atomic on every POSIX filesystem and needs no `flock`, which
# a stock macOS install does not ship; the wait is counted in the shell for
# the same reason, so the lock depends on no command outside it.
lock_dir="${cache_dir}/build.lock"
lock_held=""
for ((attempt = 0; attempt < 600; attempt++)); do
	if mkdir "${lock_dir}" 2>/dev/null; then
		lock_held=1
		break
	fi
	# A lock older than ten minutes belonged to a build its session killed.
	if [[ -n "$(find "${lock_dir}" -maxdepth 0 -mmin +10 2>/dev/null)" ]]; then
		rm -rf "${lock_dir}"
		continue
	fi
	sleep 0.5
done
if [[ -z "${lock_held}" ]]; then
	printf 'another build of the Flowstate Claude hooks holds the lock.\n' >&2
	exit 2
fi
trap 'rm -rf "${lock_dir}"' EXIT

# The waiter that just took the lock may be looking at the generation the
# previous holder published, in which case there is nothing left to build.
generation_complete=1
for hook in "${hooks[@]}"; do
	if [[ ! -x "${hook_dir}/${hook}" ]] && ! grep -qxF "${hook}" "${hook_dir}/.unbuilt" 2>/dev/null; then
		generation_complete=""
		break
	fi
done
if [[ -n "${generation_complete}" ]] &&
	[[ -f "${hook_dir}/.ready" && -s "${hook_dir}/.source-dirs" && -f "${hook_dir}/.source-id" ]] &&
	current_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${hook_dir}/.source-dirs" 2>/dev/null)" &&
	[[ "$(<"${hook_dir}/.source-id")" == "${current_id}" ]]; then
	exit 0
fi

# Stale builds leave their staging directories behind when a hook timeout
# kills the build, and nothing else prunes them.
find "${cache_dir}" -maxdepth 1 -name 'build.*' -type d -mmin +60 -exec rm -rf {} + 2>/dev/null || true
find "${cache_dir}" -maxdepth 1 -name 'dirs.*' -type f -mmin +60 -delete 2>/dev/null || true

go_version="$(awk '$1 == "go" { print $2; exit }' "${project_dir}/go.mod")"
if [[ -z "${go_version}" ]]; then
	printf 'go.mod names no Go toolchain.\n' >&2
	exit 2
fi

# The manifest is the set of first-party package directories the compiler
# reaches from the four hooks, asked of the compiler rather than maintained by
# hand, so a hook that starts importing another package is covered the moment
# it does.
list_source_dirs() {
	GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" list -deps \
		-f '{{if .Module}}{{if .Module.Main}}{{.Dir}}{{end}}{{end}}' \
		"${packages[@]}" |
		while IFS= read -r directory; do
			case "${directory}" in
				"${project_dir}") printf '.\n' ;;
				"${project_dir}"/*) printf '%s\n' "${directory#"${project_dir}"/}" ;;
				"") ;;
				*)
					printf 'hook dependency %q is outside the checkout.\n' "${directory}" >&2
					touch "${cache_dir}/incoherent"
					exit 2
					;;
			esac
		done | LC_ALL=C sort -u
}

# The walk in source-id.sh covers the source extensions a package directory
# holds, but the compiler also reads embedded files, assembly, and cgo sources,
# which can sit anywhere the package names. Those paths are recorded so the
# identity hashes them too. Refusing them instead would lock the session out of
# every tool the moment a shared package embedded a template, which is the
# failure this launcher exists to prevent.
list_extra_inputs() {
	GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" list -deps \
		-f '{{if .Module}}{{if .Module.Main}}{{$dir := .Dir}}{{range .EmbedFiles}}{{$dir}}/{{.}}{{"\n"}}{{end}}{{range .SFiles}}{{$dir}}/{{.}}{{"\n"}}{{end}}{{range .CgoFiles}}{{$dir}}/{{.}}{{"\n"}}{{end}}{{end}}{{end}}' \
		"${packages[@]}" |
		while IFS= read -r input; do
			case "${input}" in
				"") ;;
				"${project_dir}"/*) printf '%s\n' "${input#"${project_dir}"/}" ;;
				*)
					printf 'a Flowstate Claude hook input %q is outside the checkout.\n' "${input}" >&2
					exit 2
					;;
			esac
		done | LC_ALL=C sort -u
}

rm -f "${hook_dir}/.ready"
stage_dir="$(mktemp -d "${cache_dir}/build.XXXXXX")"
trap 'rm -rf "${stage_dir}" "${lock_dir}"' EXIT

for hook in "${hooks[@]}"; do
	if [[ ! -d "${project_dir}/tools/hooks/${hook}" ]]; then
		printf 'the Flowstate Claude hook package %q is missing; it is a control, not an optional build target.\n' "${hook}" >&2
		exit 2
	fi
done

# `exit 2` inside list_source_dirs leaves the pipeline's subshell, not this
# script, so the sentinel is what carries "incoherent" back to here. Without it
# every failure of the query would read as "does not compile", and an
# incoherent dependency set would take the one branch that fails open.
incoherent="${cache_dir}/incoherent"
rm -f "${incoherent}"
if ! list_source_dirs > "${stage_dir}/.source-dirs" || [[ ! -s "${stage_dir}/.source-dirs" ]]; then
	if [[ -f "${incoherent}" ]]; then
		rm -f "${incoherent}"
		printf 'the Flowstate Claude hook dependencies are not coherent.\n' >&2
		exit 2
	fi
	printf 'could not determine what the Flowstate Claude hooks are built from.\n' >&2
	exit "${not_buildable}"
fi

if ! list_extra_inputs > "${stage_dir}/.source-extra"; then
	printf 'could not determine the other inputs the Flowstate Claude hooks compile.\n' >&2
	exit 2
fi
if ! source_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${stage_dir}/.source-dirs")"; then
	printf 'could not identify the Flowstate Claude hook sources before compiling them.\n' >&2
	exit 2
fi

built=0
: > "${stage_dir}/.unbuilt"
for hook in "${hooks[@]}"; do
	if GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" build -o "${stage_dir}/${hook}" "./tools/hooks/${hook}"; then
		built=$((built + 1))
	else
		printf '%s\n' "${hook}" >> "${stage_dir}/.unbuilt"
	fi
done
if [[ "${built}" -eq 0 ]]; then
	exit "${not_buildable}"
fi

# Ask again after compiling. A source that gained an import while the build was
# running compiles a package the manifest does not name, and re-hashing the old
# manifest would agree with itself while missing exactly that package.
rm -f "${incoherent}"
post_build_dirs="$(mktemp "${cache_dir}/dirs.XXXXXX")"
post_build_extra="$(mktemp "${cache_dir}/dirs.XXXXXX")"
trap 'rm -rf "${stage_dir}" "${post_build_dirs}" "${post_build_extra}" "${lock_dir}"' EXIT
if ! list_source_dirs > "${post_build_dirs}"; then
	if [[ -f "${incoherent}" ]]; then
		rm -f "${incoherent}"
		printf 'the Flowstate Claude hook dependencies are not coherent.\n' >&2
		exit 2
	fi
	printf 'could not verify what the Flowstate Claude hooks were built from.\n' >&2
	exit "${not_buildable}"
fi
if ! cmp -s "${stage_dir}/.source-dirs" "${post_build_dirs}"; then
	printf 'the Flowstate Claude hook dependencies changed while they were compiling.\n' >&2
	exit 2
fi
# The extra inputs are asked again for the same reason the directories are, and
# separately from the identity: a file that matches an existing `//go:embed`
# glob and appears mid-build is compiled in, while no listed path changed, so
# the two identities agree over a manifest that never named it. Comparing the
# manifests is what notices, and an identity that omits an input keeps
# accepting a stale guard every time that input changes afterwards.
if ! list_extra_inputs > "${post_build_extra}"; then
	printf 'could not verify the other inputs the Flowstate Claude hooks compiled.\n' >&2
	exit 2
fi
if ! cmp -s "${stage_dir}/.source-extra" "${post_build_extra}"; then
	printf 'the Flowstate Claude hook inputs changed while they were compiling.\n' >&2
	exit 2
fi
if ! post_build_source_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${stage_dir}/.source-dirs")"; then
	printf 'could not verify the Flowstate Claude hook sources after compiling them.\n' >&2
	exit 2
fi
if [[ "${source_id}" != "${post_build_source_id}" ]]; then
	printf 'the Flowstate Claude hook sources changed while they were compiling.\n' >&2
	exit 2
fi

rm -f "${incoherent}"
printf '%s\n' "${source_id}" > "${stage_dir}/.source-id"
touch "${stage_dir}/.ready"
rm -rf "${hook_dir}"
mv "${stage_dir}" "${hook_dir}"

# Retain the merge guard that just compiled, so the next build that cannot
# compile one still has a real recognizer to consult. Only this guard: it is
# the only one whose answer to being unbuildable is a refusal rather than a
# warning, so it is the only one where deciding precisely beats failing open --
# and a retained genguard could refuse the very edit that repairs it, which is
# the lockout these hooks exist to avoid.
#
# Copied through a temporary name and renamed, so a reader never opens a
# half-written binary, and written only when this build produced one.
# Retains the merge guard that just compiled, so a later build that cannot
# compile one still has a real recognizer for the launcher to consult. Only
# this guard: it is the only one whose answer to being unbuildable is a refusal
# rather than a warning, so it is the only one where deciding precisely beats
# failing open -- and a retained genguard could refuse the very edit that
# repairs it, which is the lockout these hooks exist to avoid.
#
# A function, called with `|| true`, because that is what actually contains a
# failure: `set -e` is suppressed for the command in an `if` condition but not
# for the commands in its body, and it is suppressed through a whole function
# body invoked this way. The generation above is already published, so a build
# that succeeded must not report failure because it could not also keep a copy
# -- the launcher reads any status but 0 and 3 as an incoherent build and
# denies the call.
#
# Staged under the cache directory the stale-build sweep already prunes, then
# moved in by renaming each file over its predecessor. The directory itself is
# durable and is never removed: replacing it wholesale would mean deleting the
# guard before its replacement was in place, and a kill in that window would
# leave the next session with no recognizer at all. Renaming a file replaces it
# atomically, so the guard is only ever the previous one or the new one.
#
# The binary moves first. The two renames cannot be made one, so the recorded
# identity can briefly describe the previous build; the note that reads it says
# what is recorded rather than asserting the binary's provenance, which stays
# true either way.
retain_merge_guard() {
	local stage
	[[ -x "${hook_dir}/mergeguard" ]] || return 0
	stage="$(mktemp -d "${cache_dir}/build.lkg.XXXXXX")" || return 0
	if install -d -m 0700 "${retained_dir}" &&
		cp "${hook_dir}/mergeguard" "${stage}/mergeguard" &&
		chmod 0700 "${stage}/mergeguard" &&
		printf '%s\n' "${source_id}" > "${stage}/.source-id" &&
		mv "${stage}/mergeguard" "${retained_dir}/mergeguard"; then
		mv "${stage}/.source-id" "${retained_dir}/.source-id" || true
	fi
	rm -rf "${stage}" || true
	return 0
}
retain_merge_guard || true
trap 'rm -f "${post_build_dirs}" "${post_build_extra}"; rm -rf "${lock_dir}"' EXIT
