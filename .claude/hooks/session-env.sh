#!/usr/bin/env bash
set -euo pipefail

# Claude's remote container puts the base image's Go installation first on
# PATH. Persist the toolchain named by go.mod for every Bash command in this
# session, so a bare gofmt agrees with the Makefile and CI.
project_dir="${CLAUDE_PROJECT_DIR:-}"
if [[ -z "${project_dir}" || ! -d "${project_dir}" ]]; then
	printf 'CLAUDE_PROJECT_DIR does not name a checkout directory.\n' >&2
	exit 2
fi

hook_dir="${project_dir}/.claude/hooks/.bin"
rm -f "${hook_dir}/.ready"

go_version="$(awk '$1 == "go" { print $2; exit }' "${project_dir}/go.mod")"
cache_dir="${project_dir}/.claude/hooks/.cache"
install -d -m 0700 "${hook_dir}" "${cache_dir}"
stage_dir="$(mktemp -d "${cache_dir}/build.XXXXXX")"
trap 'rm -rf "${stage_dir}"' EXIT
GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" list -deps \
	-f '{{if .Module}}{{if .Module.Main}}{{.Dir}}{{end}}{{end}}' \
	./tools/hooks/genguard \
	./tools/hooks/gofmtcheck \
	./tools/hooks/pidguard \
	./tools/hooks/mergeguard |
	while IFS= read -r directory; do
		case "${directory}" in
			"${project_dir}") printf '.\n' ;;
			"${project_dir}"/*) printf '%s\n' "${directory#"${project_dir}"/}" ;;
			"") ;;
			*) printf 'hook dependency %q is outside the checkout.\n' "${directory}" >&2; exit 2 ;;
		esac
	done | sort -u > "${stage_dir}/.source-dirs"
source_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${stage_dir}/.source-dirs")"
GOTOOLCHAIN="go${go_version}" go -C "${project_dir}" build -o "${stage_dir}/" \
	./tools/hooks/genguard \
	./tools/hooks/gofmtcheck \
	./tools/hooks/pidguard \
	./tools/hooks/mergeguard
if ! post_build_source_id="$(CLAUDE_PROJECT_DIR="${project_dir}" bash "${project_dir}/.claude/hooks/source-id.sh" "${stage_dir}/.source-dirs")"; then
	printf 'could not verify hook sources after compiling them; restart Claude Code.\n' >&2
	exit 2
fi
if [[ "${source_id}" != "${post_build_source_id}" ]]; then
	printf 'hook sources changed while SessionStart was compiling them; restart Claude Code.\n' >&2
	exit 2
fi
printf '%s\n' "${source_id}" > "${stage_dir}/.source-id"
touch "${stage_dir}/.ready"
rm -rf "${hook_dir}"
mv "${stage_dir}" "${hook_dir}"
trap - EXIT

if [[ -z "${CLAUDE_ENV_FILE:-}" ]]; then
	exit 0
fi

goroot="$(GOTOOLCHAIN="go${go_version}" go env GOROOT)"
printf 'export PATH=%q:$PATH\n' "${goroot}/bin" >> "${CLAUDE_ENV_FILE}"
