#!/bin/sh
# Build every in-tree plugin and write the portable contract catalog reviewed in
# source control. When a second path is supplied, also write the complete native
# catalog used to validate examples/plugins during this invocation.
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
	echo "usage: $0 <reviewed-catalog> [validation-catalog]" >&2
	exit 2
fi

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
out=$1
case "$out" in
	/*) ;;
	*) out="$root/$out" ;;
esac
validation_out=${2:-}
case "$validation_out" in
	"") ;;
	/*) ;;
	*) validation_out="$root/$validation_out" ;;
esac

command -v jq >/dev/null 2>&1 || {
	echo "$0: jq is required to canonicalize the generated catalog" >&2
	exit 1
}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/flowstate-plugin-catalog.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

cd "$root"
for module in plugins/*/; do
	[ -f "$module/go.mod" ] || continue
	name=$(basename "$module")
	echo "==> building $name" >&2
	(
		cd "$module"
		go build -trimpath -buildvcs=false -o "$tmp/flowstate-plugin-$name" .
	)
done

# The SDK's worked example is a first-party plugin too, and
# examples/plugins/greet depends on its descriptors.
echo "==> building example" >&2
go build -trimpath -buildvcs=false \
	-o "$tmp/flowstate-plugin-example" \
	./pkg/flowstate/v1/plugin/examples/flowstate-plugin-example

go run ./cmd/flow plugins --plugin-dir "$tmp" --output json >"$tmp/catalog.json"

# SearchPath and Path report where this particular host found the binaries.
# DistributionDigest reports the native executable bytes, so it legitimately
# differs across GOOS/GOARCH and is retained only in the invocation-local
# validation catalog. Task schema and claims digests are portable plugin
# contracts and remain pinned in the reviewed document.
if [ -n "$validation_out" ]; then
	jq 'del(.searchPath) | .plugins |= map(del(.path))' \
		"$tmp/catalog.json" >"$validation_out"
fi
jq 'del(.searchPath) | .plugins |= map(del(.path, .distributionDigest))' \
	"$tmp/catalog.json" >"$out"
