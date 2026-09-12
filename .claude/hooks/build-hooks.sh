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
# launchers can find the same generation stale at once. The lock makes the
# second wait for the first and then find a current build rather than race it.
exec 9>"${cache_dir}/build.lock"
flock 9

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
trap 'rm -rf "${stage_dir}"' EXIT

if ! list_source_dirs > "${stage_dir}/.source-dirs" || [[ ! -s "${stage_dir}/.source-dirs" ]]; then
	printf 'could not determine what the Flowstate Claude hooks are built from.\n' >&2
	exit "${not_buildable}"
fi

# The identity walks each package directory for the source extensions the
# compiler reads there. An embedded file, assembly, or cgo source would be
# compiled in without being hashed, so refuse loudly now rather than run a
# stale guard quietly later.
extra_inputs="$(GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" list -deps \
	-f '{{if .Module}}{{if .Module.Main}}{{range .EmbedFiles}}{{.}} {{end}}{{range .SFiles}}{{.}} {{end}}{{range .CgoFiles}}{{.}} {{end}}{{end}}{{end}}' \
	"${packages[@]}" | tr -d '[:space:]')"
if [[ -n "${extra_inputs}" ]]; then
	printf 'a Flowstate Claude hook now compiles an embedded, assembly, or cgo input that the source identity does not cover.\n' >&2
	exit 2
fi
source_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${stage_dir}/.source-dirs")"

if ! GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" build -o "${stage_dir}/" "${packages[@]}"; then
	exit "${not_buildable}"
fi

# Ask again after compiling. A source that gained an import while the build was
# running compiles a package the manifest does not name, and re-hashing the old
# manifest would agree with itself while missing exactly that package.
post_build_dirs="$(mktemp "${cache_dir}/dirs.XXXXXX")"
trap 'rm -rf "${stage_dir}" "${post_build_dirs}"' EXIT
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
trap 'rm -f "${post_build_dirs}"' EXIT
