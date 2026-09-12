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

hooks=(genguard gofmtcheck pidguard mergeguard)
packages=()
for hook in "${hooks[@]}"; do
	packages+=("./tools/hooks/${hook}")
done

hook_dir="${project_dir}/.claude/hooks/.bin"
cache_dir="${project_dir}/.claude/hooks/.cache"
install -d -m 0700 "${cache_dir}"

# Exit 3 says the tree does not compile right now, which is an ordinary state
# between two edits of a refactor. The launcher answers that differently from
# every other failure here, because a guard that cannot be built has not
# refused anything and must not take the session down with it.
readonly not_buildable=3

# One builder at a time: the guards run on every matching tool call, so two
# launchers can find the same generation stale at once. `mkdir` is the mutex
# because it is atomic on every POSIX filesystem and needs no `flock`, which
# a stock macOS install does not ship.
lock_dir="${cache_dir}/build.lock"
lock_held=""
for _ in $(seq 1 600); do
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
					exit 2
					;;
			esac
		done | sort -u
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

if ! list_source_dirs > "${stage_dir}/.source-dirs" || [[ ! -s "${stage_dir}/.source-dirs" ]]; then
	printf 'could not determine what the Flowstate Claude hooks are built from.\n' >&2
	exit "${not_buildable}"
fi

# The walk covers the source extensions a package directory holds, but the
# compiler also reads embedded files, assembly, and cgo sources, which can sit
# anywhere the package names. Those paths are recorded so the identity hashes
# them too. Refusing them instead would lock the session out of every tool the
# moment a shared package embedded a template, which is the failure this
# launcher exists to prevent.
if ! GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" list -deps \
	-f '{{if .Module}}{{if .Module.Main}}{{$dir := .Dir}}{{range .EmbedFiles}}{{$dir}}/{{.}}{{"\n"}}{{end}}{{range .SFiles}}{{$dir}}/{{.}}{{"\n"}}{{end}}{{range .CgoFiles}}{{$dir}}/{{.}}{{"\n"}}{{end}}{{end}}{{end}}' \
	"${packages[@]}" | while IFS= read -r input; do
		case "${input}" in
			"") ;;
			"${project_dir}"/*) printf '%s\n' "${input#"${project_dir}"/}" ;;
			*)
				printf 'a Flowstate Claude hook input %q is outside the checkout.\n' "${input}" >&2
				exit 2
				;;
		esac
	done | LC_ALL=C sort -u > "${stage_dir}/.source-extra"; then
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
post_build_dirs="$(mktemp "${cache_dir}/dirs.XXXXXX")"
trap 'rm -rf "${stage_dir}" "${post_build_dirs}" "${lock_dir}"' EXIT
if ! list_source_dirs > "${post_build_dirs}"; then
	printf 'could not verify what the Flowstate Claude hooks were built from.\n' >&2
	exit "${not_buildable}"
fi
if ! cmp -s "${stage_dir}/.source-dirs" "${post_build_dirs}"; then
	printf 'the Flowstate Claude hook dependencies changed while they were compiling.\n' >&2
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

printf '%s\n' "${source_id}" > "${stage_dir}/.source-id"
touch "${stage_dir}/.ready"
rm -rf "${hook_dir}"
mv "${stage_dir}" "${hook_dir}"
trap 'rm -f "${post_build_dirs}"; rm -rf "${lock_dir}"' EXIT
