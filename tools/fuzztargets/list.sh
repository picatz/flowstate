#!/bin/sh
# Print the fuzz targets in one tier, one `<target> <package directory>` line
# each, from tools/fuzztargets/targets.txt — the one written source of that
# list. Used by the Makefile's fuzz-smoke target and by deep.yml's fuzz-deep
# job, so neither holds a copy of the list and neither holds its own idea of
# how the file is spelled. tools/fuzztargets' test runs this script and
# compares its output against the Go parser in targets.go, so the two readers
# of one file cannot disagree about what it says.
#
# Target names after the tier narrow the answer to those targets, in the
# file's order. That is how CI's fuzz-smoke job runs the targets the plan
# selected rather than the tier (#1726), and it fails closed: a name that is
# not in the tier is an error naming it, never an empty line, because a plan
# and a target list that disagree should stop the job rather than fuzz whatever
# is left.
#
# Usage: tools/fuzztargets/list.sh <tier> [target ...]
set -eu

if [ $# -lt 1 ]; then
	echo "usage: $0 <tier> [target ...]" >&2
	exit 2
fi

dir=$(dirname "$0")
tier=$1
shift

# A target's tier column is a comma-separated list, so the match is anchored on
# either a comma or an end of the field: a bare substring match would let a
# hypothetical "deeper" tier answer for "deep".
out=$(awk -v tier="$tier" -v want="$*" -v self="$0" '
	BEGIN {
		n = split(want, names, " ")
		for (i = 1; i <= n; i++) if (names[i] != "") keep[names[i]] = 1
		selecting = length(keep) > 0
	}
	/^[[:space:]]*#/ { next }
	NF == 0          { next }
	NF != 3          { printf "%s:%d: want 3 fields, got %d\n", FILENAME, FNR, NF > "/dev/stderr"; exit 1 }
	("," $3 ",") ~ ("," tier ",") {
		if (!selecting || ($1 in keep)) { print $1, $2; seen[$1] = 1 }
	}
	END {
		for (name in keep) if (!(name in seen)) {
			printf "%s: %s is not a fuzz target in tier %s\n", self, name, tier > "/dev/stderr"
			bad = 1
		}
		if (bad) exit 1
	}
' "$dir/targets.txt")

if [ -z "$out" ]; then
	echo "$0: no fuzz targets in tier '$tier'" >&2
	exit 1
fi

echo "$out"
