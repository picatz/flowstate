#!/bin/sh
# Print the fuzz targets in one tier, one `<target> <package directory>` line
# each, from tools/fuzztargets/targets.txt — the one written source of that
# list. Used by the Makefile's fuzz-smoke target and by deep.yml's fuzz-deep
# job, so neither holds a copy of the list and neither holds its own idea of
# how the file is spelled. tools/fuzztargets' test runs this script and
# compares its output against the Go parser in targets.go, so the two readers
# of one file cannot disagree about what it says.
#
# Usage: tools/fuzztargets/list.sh <tier>
set -eu

if [ $# -ne 1 ]; then
	echo "usage: $0 <tier>" >&2
	exit 2
fi

dir=$(dirname "$0")
tier=$1

# A target's tier column is a comma-separated list, so the match is anchored on
# either a comma or an end of the field: a bare substring match would let a
# hypothetical "deeper" tier answer for "deep".
out=$(awk -v tier="$tier" '
	/^[[:space:]]*#/ { next }
	NF == 0          { next }
	NF != 3          { printf "%s:%d: want 3 fields, got %d\n", FILENAME, FNR, NF > "/dev/stderr"; exit 1 }
	("," $3 ",") ~ ("," tier ",") { print $1, $2 }
' "$dir/targets.txt")

if [ -z "$out" ]; then
	echo "$0: no fuzz targets in tier '$tier'" >&2
	exit 1
fi

echo "$out"
