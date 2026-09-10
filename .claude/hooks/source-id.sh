#!/usr/bin/env bash
set -euo pipefail

project_dir="${CLAUDE_PROJECT_DIR:-}"
if [[ -z "${project_dir}" || ! -d "${project_dir}" ]]; then
	printf 'CLAUDE_PROJECT_DIR does not name a checkout directory.\n' >&2
	exit 2
fi
source_dirs="${1:-}"
if [[ -z "${source_dirs}" || ! -s "${source_dirs}" ]]; then
	printf 'the Flowstate Claude hook build manifest is unavailable.\n' >&2
	exit 2
fi
mapfile -t directories < "${source_dirs}"
pathspecs=(go.mod go.sum)
pathspecs+=("${directories[@]}")

all_paths="$(mktemp)"
build_paths="$(mktemp)"
trap 'rm -f "${all_paths}" "${build_paths}"' EXIT
if ! git -C "${project_dir}" ls-files -z --cached --others --exclude-standard -- \
	"${pathspecs[@]}" > "${all_paths}"; then
	printf 'could not enumerate the Flowstate Claude hook build inputs.\n' >&2
	exit 2
fi
while IFS= read -r -d '' path; do
	case "${path}" in
		go.mod | go.sum | *.go)
			[[ "${path}" == *_test.go ]] || printf '%s\0' "${path}" >> "${build_paths}"
			;;
	esac
done < "${all_paths}"
if [[ ! -s "${build_paths}" ]]; then
	printf 'could not find the Flowstate Claude hook build inputs.\n' >&2
	exit 2
fi

{
	printf '%s\0' "${directories[@]}"
	while IFS= read -r -d '' path; do
		printf '%s\0' "${path}"
		git -C "${project_dir}" hash-object -- "${path}"
	done < "${build_paths}"
} | git -C "${project_dir}" hash-object --stdin
