package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestReleaseVersions(t *testing.T) {
	for _, version := range []string{"v0.1.0", "v1.2.3", "v1.2.3-rc.1", "v1.2.3-beta-2"} {
		if !validReleaseVersion(version) {
			t.Errorf("release version %q was refused", version)
		}
	}
	for _, version := range []string{"", "1.2.3", "v1.2", "v01.2.3", "v1.02.3", "v1.2.3-01", "v1.2.3+dirty", "main"} {
		if validReleaseVersion(version) {
			t.Errorf("non-release version %q was accepted", version)
		}
	}
}

func TestParseTargets(t *testing.T) {
	targets, err := parseTargets("linux/amd64, darwin/arm64")
	if err != nil {
		t.Fatal(err)
	}
	want := []target{{OS: "linux", Arch: "amd64"}, {OS: "darwin", Arch: "arm64"}}
	if targetsString(targets) != targetsString(want) {
		t.Fatalf("targets = %q, want %q", targetsString(targets), targetsString(want))
	}

	for _, invalid := range []string{"", "linux", "linux/amd64/extra", "../linux/amd64", "linux/../amd64", "linux/amd64,linux/amd64"} {
		if _, err := parseTargets(invalid); err == nil {
			t.Errorf("parseTargets(%q) succeeded", invalid)
		}
	}
}

func TestReleaseBuildEnvironmentPinsGoInputs(t *testing.T) {
	t.Setenv("GOENV", "/tmp/host-goenv")
	t.Setenv("GOFLAGS", "-tags=host")
	t.Setenv("GOWORK", "/tmp/host.work")
	t.Setenv("GOAMD64", "v4")
	t.Setenv("GOARM64", "v9.0")

	environment := releaseBuildEnvironment(target{OS: "linux", Arch: "amd64"})
	values := make(map[string][]string)
	for _, entry := range environment {
		name, value, _ := strings.Cut(entry, "=")
		values[name] = append(values[name], value)
	}
	for name, want := range map[string]string{
		"CGO_ENABLED": "0",
		"GOARCH":      "amd64",
		"GOAMD64":     "v1",
		"GOENV":       "off",
		"GOFLAGS":     "",
		"GOOS":        "linux",
		"GOWORK":      "off",
	} {
		if got := values[name]; len(got) != 1 || got[0] != want {
			t.Errorf("%s values = %q, want [%q]", name, got, want)
		}
	}
	if got := values["GOARM64"]; len(got) != 0 {
		t.Errorf("GOARM64 values = %q, want none for amd64 target", got)
	}
}

func TestArchivesAreDeterministicAndCarryTheReleaseFiles(t *testing.T) {
	directory := t.TempDir()
	files := []archiveFile{
		{name: "flowstate_0.1.0_linux_amd64/flow", data: []byte("binary"), mode: 0o755},
		{name: "flowstate_0.1.0_linux_amd64/LICENSE", data: []byte("license"), mode: 0o644},
		{name: "flowstate_0.1.0_linux_amd64/README.txt", data: []byte("readme"), mode: 0o644},
	}

	firstTar := filepath.Join(directory, "first.tar.gz")
	secondTar := filepath.Join(directory, "second.tar.gz")
	if err := writeTarGz(firstTar, files); err != nil {
		t.Fatal(err)
	}
	if err := writeTarGz(secondTar, files); err != nil {
		t.Fatal(err)
	}
	assertSameFile(t, firstTar, secondTar)
	assertTarMembers(t, firstTar, files)

	firstZIP := filepath.Join(directory, "first.zip")
	secondZIP := filepath.Join(directory, "second.zip")
	if err := writeZIP(firstZIP, files); err != nil {
		t.Fatal(err)
	}
	if err := writeZIP(secondZIP, files); err != nil {
		t.Fatal(err)
	}
	assertSameFile(t, firstZIP, secondZIP)
	assertZIPMembers(t, firstZIP, files)
}

func TestWriteSBOMDescribesTheBuiltBinary(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "flow.cdx.json")
	if err := writeSBOM(path, executable, "v0.1.0", target{OS: runtime.GOOS, Arch: runtime.GOARCH}); err != nil {
		t.Fatal(err)
	}

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document bom
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	if document.Schema != "http://cyclonedx.org/schema/bom-1.6.schema.json" || document.Format != "CycloneDX" || document.Spec != "1.6" || document.Version != 1 {
		t.Fatalf("unexpected BOM identity: %#v", document)
	}
	if document.Metadata.Component.Name != "flow" || document.Metadata.Component.Version != "v0.1.0" {
		t.Fatalf("unexpected root component: %#v", document.Metadata.Component)
	}
	wantHash, err := hashFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Metadata.Component.Hashes) != 1 || document.Metadata.Component.Hashes[0].Content != wantHash {
		t.Fatalf("root hashes = %#v, want SHA-256 %s", document.Metadata.Component.Hashes, wantHash)
	}
	if !sort.SliceIsSorted(document.Components, func(i, j int) bool {
		return document.Components[i].Ref < document.Components[j].Ref
	}) {
		t.Fatal("SBOM dependency components are not deterministic")
	}
}

func TestChecksumsCoverOnlyPayloadFiles(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "b.zip"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "a.cdx.json"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(directory, ".bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeChecksums(directory); err != nil {
		t.Fatal(err)
	}
	checksums, err := os.ReadFile(filepath.Join(directory, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(checksums)), "\n")
	if len(lines) != 2 || !strings.HasSuffix(lines[0], "  a.cdx.json") || !strings.HasSuffix(lines[1], "  b.zip") {
		t.Fatalf("unexpected checksums:\n%s", checksums)
	}
}

func assertSameFile(t *testing.T, first, second string) {
	t.Helper()
	firstHash, err := hashFile(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := hashFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("same archive input produced %s and %s", firstHash, secondHash)
	}
}

func assertTarMembers(t *testing.T, path string, want []archiveFile) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	for _, expected := range want {
		header, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if header.Name != expected.name || string(content) != string(expected.data) || os.FileMode(header.Mode).Perm() != expected.mode {
			t.Fatalf("tar member = %q mode %o content %q; want %q mode %o content %q", header.Name, header.Mode, content, expected.name, expected.mode, expected.data)
		}
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("tar has an unexpected extra member: %v", err)
	}
}

func assertZIPMembers(t *testing.T, path string, want []archiveFile) {
	t.Helper()
	reader, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if len(reader.File) != len(want) {
		t.Fatalf("zip has %d members, want %d", len(reader.File), len(want))
	}
	for i, expected := range want {
		member := reader.File[i]
		opened, err := member.Open()
		if err != nil {
			t.Fatal(err)
		}
		content, readErr := io.ReadAll(opened)
		closeErr := opened.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if member.Name != expected.name || string(content) != string(expected.data) || member.Mode().Perm() != expected.mode {
			t.Fatalf("zip member = %q mode %o content %q; want %q mode %o content %q", member.Name, member.Mode(), content, expected.name, expected.mode, expected.data)
		}
	}
}
