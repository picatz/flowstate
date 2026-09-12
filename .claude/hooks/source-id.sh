#!/usr/bin/env bash
set -euo pipefail

# Prints an identity for the sources the hook binaries were compiled from, so a
# launcher can tell a current build from one whose sources have since changed.
#
# The inputs are enumerated from the filesystem rather than from Git. `go build`
# does not consult `.gitignore`, so a `.go` file that Git ignores is compiled
# all the same, and an identity that skipped it would keep accepting a stale
# guard after that source changed. Test files are excluded because the compiler
# does not read them, which also keeps an edit to a hook's own test from
# invalidating the build in the middle of running it.

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
directories=()
while IFS= read -r directory || [[ -n "${directory}" ]]; do
	[[ -n "${directory}" ]] && directories+=("${directory}")
done < "${source_dirs}"
if [[ ${#directories[@]} -eq 0 ]]; then
	printf 'the Flowstate Claude hook build manifest is empty.\n' >&2
	exit 2
fi

# A template, not a bare `mktemp`: BSD `mktemp` requires one, and this script
# stands under every guard, so failing here would deny every tool at once.
build_paths="$(mktemp "${TMPDIR:-/tmp}/flowstate-hook-source-id.XXXXXX")"
trap 'rm -f "${build_paths}"' EXIT

# go.mod and go.sum select the toolchain and the module versions, so a change to
# either compiles different code from the same package sources.
for manifest in go.mod go.sum; do
	if [[ -f "${project_dir}/${manifest}" ]]; then
		printf '%s\n' "${manifest}" >> "${build_paths}"
	fi
done
for directory in "${directories[@]}"; do
	package_dir="${project_dir}/${directory}"
	if [[ "${directory}" == "." ]]; then
		package_dir="${project_dir}"
	fi
	if [[ ! -d "${package_dir}" ]]; then
		printf 'the Flowstate Claude hook source directory %q is missing.\n' "${directory}" >&2
		exit 2
	fi
	# Only this directory's own files: a package does not compile its
	# subdirectories, and each package the hooks import is listed in its own
	# right by the manifest. The extensions are the ones these packages use,
	# together with the embedded, assembly and cgo inputs `build-hooks.sh`
	# records beside the manifest. That covers what the hooks compile today,
	# which is pure Go; a dependency that gained a C++, Objective-C or SWIG
	# source would need both files widened, since `go list` reports those
	# kinds separately and neither this walk nor that query names them
	# (#1967). Symbolic links count, because the compiler follows them.
	while IFS= read -r -d '' path; do
		relative="${path#"${project_dir}/"}"
		if [[ "${relative}" == *$'\n'* ]]; then
			printf 'a Flowstate Claude hook source name contains a newline.\n' >&2
			exit 2
		fi
		printf '%s\n' "${relative}" >> "${build_paths}"
	done < <(find "${package_dir}" -maxdepth 1 \( -type f -o -type l \) \
		\( -name '*.go' -o -name '*.s' -o -name '*.c' -o -name '*.h' -o -name '*.syso' \) \
		! -name '*_test.go' -print0)
done
# Inputs the compiler names rather than the directory walk finds: embedded
# files, assembly, cgo sources. `build-hooks.sh` records them beside the
# manifest, so an identity computed later covers the same set the build did.
extra_manifest="${source_dirs%/*}/.source-extra"
if [[ -f "${extra_manifest}" ]]; then
	while IFS= read -r extra || [[ -n "${extra}" ]]; do
		[[ -n "${extra}" ]] || continue
		if [[ ! -e "${project_dir}/${extra}" ]]; then
			printf 'a Flowstate Claude hook input %q is missing.\n' "${extra}" >&2
			exit 2
		fi
		printf '%s\n' "${extra}" >> "${build_paths}"
	done < "${extra_manifest}"
fi

if [[ ! -s "${build_paths}" ]]; then
	printf 'could not find the Flowstate Claude hook build inputs.\n' >&2
	exit 2
fi

{
	printf '%s\n' "${directories[@]}"
	while IFS= read -r path; do
		printf '%s\n' "${path}"
		git -C "${project_dir}" hash-object -- "${path}"
	done < <(LC_ALL=C sort "${build_paths}")
} | git -C "${project_dir}" hash-object --stdin
