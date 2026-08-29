package flowstatev1

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// schemaPathPrefix is what makes a linked file one of *this* schema's.
const schemaPathPrefix = "flowstate/v1/"

// TestEveryFileOfTheSchemaIsProvided walks the registry against the hand-written
// roots in [engineProvidedFiles].
//
// # Why a walk rather than a careful list
//
// The set says which descriptor paths anything speaking this schema already
// has, so a file missing from it is a file the SDK ships a *copy* of — and a
// plugin naming one of that file's messages as a task input then has its
// manifest rejected, because `plugin.link` resolves the path from the global
// registry without adding it to the private one `resolveMessage` searches. Host
// and plugin built from the same commit, refusing each other.
//
// The list has to be written out, because Go cannot enumerate a package's
// variables at run time. That is the position `tools/fuzztargets` and
// `tools/vacuity`'s corpus registry are in, and the answer is theirs: a
// written-out list is trustworthy only when something walks the thing it claims
// to cover and fails when the two disagree.
//
// It was not trustworthy. Its comment promised "every file of flowstate/v1" and
// said a thirteenth belonged there the day it existed — and by the time anybody
// looked, three were missing: `audit.proto` and `authorization.proto`, which no
// other file imports so the transitive walk never reached them, and
// `debug.proto`, which arrived with #928's wire messages and is what turned
// this up (Codex, #1194).
func TestEveryFileOfTheSchemaIsProvided(t *testing.T) {
	t.Parallel()

	provided := engineProvidedFiles()

	// A registry that answered with nothing would make every claim below
	// vacuous, and it is exactly what a build that linked no descriptors gives.
	linked := 0
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if strings.HasPrefix(file.Path(), schemaPathPrefix) {
			linked++
		}

		return true
	})
	if linked < 10 {
		t.Fatalf("the registry holds %d files under %s, which is fewer than this schema has — the walk below would pass by finding nothing", linked, schemaPathPrefix)
	}

	missing := make([]string, 0)
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		path := file.Path()
		if !strings.HasPrefix(path, schemaPathPrefix) {
			return true
		}
		if _, ok := provided[path]; !ok {
			missing = append(missing, path)
		}

		return true
	})

	for _, path := range missing {
		t.Errorf("%s is part of this schema and is not in engineProvidedFiles, so the SDK sends a copy of it and a plugin naming one of its messages has its manifest rejected; add its File_… root there", path)
	}
}
