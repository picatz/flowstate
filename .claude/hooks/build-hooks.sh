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

# One builder at a time: the guards run on every matching tool call, so two
# launchers can find the same generation stale at once. The lock makes the
# second wait for the first and then find a current build rather than race it.
exec 9>"${cache_dir}/build.lock"
flock 9

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
trap 'rm -rf "${stage_dir}"' EXIT

list_source_dirs > "${stage_dir}/.source-dirs"
if [[ ! -s "${stage_dir}/.source-dirs" ]]; then
	printf 'could not determine what the Flowstate Claude hooks are built from.\n' >&2
	exit 2
fi
source_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${stage_dir}/.source-dirs")"

GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" build -o "${stage_dir}/" "${packages[@]}"

# Ask again after compiling. A source that gained an import while the build was
# running compiles a package the manifest does not name, and re-hashing the old
# manifest would agree with itself while missing exactly that package.
post_build_dirs="$(mktemp "${cache_dir}/dirs.XXXXXX")"
trap 'rm -rf "${stage_dir}" "${post_build_dirs}"' EXIT
if ! list_source_dirs > "${post_build_dirs}"; then
	printf 'could not verify what the Flowstate Claude hooks were built from.\n' >&2
	exit 2
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
trap 'rm -f "${post_build_dirs}"' EXIT
