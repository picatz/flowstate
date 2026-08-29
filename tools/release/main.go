// Command release builds the immutable, checksummed payload for a Flowstate
// release. It does not publish anything; the release workflow owns that
// separate, permission-bearing step.
package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const modulePath = "github.com/picatz/flowstate"

var releaseVersion = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)
var targetPart = regexp.MustCompile(`^[a-z0-9]+$`)

type target struct {
	OS   string
	Arch string
}

var defaultTargets = []target{
	{OS: "linux", Arch: "amd64"},
	{OS: "linux", Arch: "arm64"},
	{OS: "darwin", Arch: "amd64"},
	{OS: "darwin", Arch: "arm64"},
	{OS: "windows", Arch: "amd64"},
}

type config struct {
	version string
	output  string
	targets []target
}

func main() {
	var (
		version = flag.String("version", "", "release version, including the leading v (for example v0.1.0)")
		output  = flag.String("output", "dist", "directory to create with release artifacts")
		targets = flag.String("targets", targetsString(defaultTargets), "comma-separated GOOS/GOARCH targets")
	)
	flag.Parse()

	parsedTargets, err := parseTargets(*targets)
	if err == nil {
		err = run(config{version: *version, output: *output, targets: parsedTargets})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "release:", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	if !validReleaseVersion(cfg.version) {
		return fmt.Errorf("version %q is not a release version such as v0.1.0 or v0.1.0-rc.1", cfg.version)
	}
	if len(cfg.targets) == 0 {
		return errors.New("at least one target is required")
	}
	cfg.output = filepath.Clean(cfg.output)
	if _, err := os.Stat("go.mod"); err != nil {
		return fmt.Errorf("run from the repository root: %w", err)
	}
	if _, err := os.Stat(cfg.output); err == nil {
		return fmt.Errorf("output directory %q already exists; refusing to mix release payloads", cfg.output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checking output directory: %w", err)
	}

	parent := filepath.Dir(cfg.output)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("creating output parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".flowstate-release-")
	if err != nil {
		return fmt.Errorf("creating staging directory: %w", err)
	}
	defer os.RemoveAll(staging)

	license, err := os.ReadFile("LICENSE")
	if err != nil {
		return fmt.Errorf("reading LICENSE: %w", err)
	}

	for _, target := range cfg.targets {
		if err := buildTarget(staging, cfg.version, target, license); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(filepath.Join(staging, ".bin")); err != nil {
		return fmt.Errorf("removing staged binaries: %w", err)
	}
	if err := writeChecksums(staging); err != nil {
		return err
	}
	if err := os.Rename(staging, cfg.output); err != nil {
		return fmt.Errorf("publishing completed payload directory: %w", err)
	}
	fmt.Printf("release payload: %s\n", cfg.output)
	return nil
}

func buildTarget(staging, version string, target target, license []byte) error {
	base := artifactBase(version, target)
	binaryName := "flow"
	if target.OS == "windows" {
		binaryName += ".exe"
	}
	binaryPath := filepath.Join(staging, ".bin", base, binaryName)
	if err := os.MkdirAll(filepath.Dir(binaryPath), 0o755); err != nil {
		return fmt.Errorf("creating binary staging directory: %w", err)
	}

	cmd := exec.Command("go", "build", "-buildvcs=true", "-trimpath", "-mod=readonly",
		"-ldflags=-s -w -X main.version="+version, "-o", binaryPath, "./cmd/flow")
	cmd.Env = releaseBuildEnvironment(target)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("building %s/%s: %w", target.OS, target.Arch, err)
	}

	sbomPath := filepath.Join(staging, base+".cdx.json")
	if err := writeSBOM(sbomPath, binaryPath, version, target); err != nil {
		return err
	}

	readme := []byte(fmt.Sprintf("Flowstate %s (%s/%s)\n\nDocumentation: https://github.com/picatz/flowstate/tree/%s/docs\nSecurity policy: https://github.com/picatz/flowstate/blob/%s/SECURITY.md\n", version, target.OS, target.Arch, version, version))
	files := []archiveFile{
		{name: filepath.Join(base, binaryName), path: binaryPath, mode: 0o755},
		{name: filepath.Join(base, "LICENSE"), data: license, mode: 0o644},
		{name: filepath.Join(base, "README.txt"), data: readme, mode: 0o644},
	}
	archivePath := filepath.Join(staging, base+archiveSuffix(target.OS))
	if target.OS == "windows" {
		return writeZIP(archivePath, files)
	}
	return writeTarGz(archivePath, files)
}

func releaseBuildEnvironment(target target) []string {
	overridden := map[string]bool{
		"CGO_ENABLED": true,
		"GOARCH":      true,
		"GOARM64":     true,
		"GOAMD64":     true,
		"GOENV":       true,
		"GOFLAGS":     true,
		"GOOS":        true,
		"GOWORK":      true,
	}
	environment := make([]string, 0, len(os.Environ())+6)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !overridden[name] {
			environment = append(environment, entry)
		}
	}
	environment = append(environment,
		"CGO_ENABLED=0",
		"GOARCH="+target.Arch,
		"GOENV=off",
		"GOFLAGS=",
		"GOOS="+target.OS,
		"GOWORK=off",
	)
	switch target.Arch {
	case "amd64":
		environment = append(environment, "GOAMD64=v1")
	case "arm64":
		environment = append(environment, "GOARM64=v8.0")
	}
	return environment
}

func parseTargets(value string) ([]target, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("targets must not be empty")
	}
	seen := map[target]bool{}
	var targets []target
	for _, item := range strings.Split(value, ",") {
		parts := strings.Split(strings.TrimSpace(item), "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("target %q must be GOOS/GOARCH", item)
		}
		t := target{OS: parts[0], Arch: parts[1]}
		if !targetPart.MatchString(t.OS) || !targetPart.MatchString(t.Arch) {
			return nil, fmt.Errorf("target %q contains characters outside a GOOS/GOARCH name", item)
		}
		if seen[t] {
			return nil, fmt.Errorf("target %s/%s is repeated", t.OS, t.Arch)
		}
		seen[t] = true
		targets = append(targets, t)
	}
	return targets, nil
}

func validReleaseVersion(version string) bool {
	if !releaseVersion.MatchString(version) {
		return false
	}
	_, prerelease, found := strings.Cut(version, "-")
	if !found {
		return true
	}
	for _, identifier := range strings.Split(prerelease, ".") {
		if len(identifier) > 1 && identifier[0] == '0' {
			numeric := true
			for _, character := range identifier {
				if character < '0' || character > '9' {
					numeric = false
					break
				}
			}
			if numeric {
				return false
			}
		}
	}
	return true
}

func targetsString(targets []target) string {
	values := make([]string, len(targets))
	for i, target := range targets {
		values[i] = target.OS + "/" + target.Arch
	}
	return strings.Join(values, ",")
}

func artifactBase(version string, target target) string {
	return "flowstate_" + strings.TrimPrefix(version, "v") + "_" + target.OS + "_" + target.Arch
}

func archiveSuffix(goos string) string {
	if goos == "windows" {
		return ".zip"
	}
	return ".tar.gz"
}

type archiveFile struct {
	name string
	path string
	data []byte
	mode os.FileMode
}

var archiveTime = time.Date(1980, time.January, 1, 0, 0, 0, 0, time.UTC)

func writeTarGz(path string, files []archiveFile) (returnErr error) {
	out, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	defer closeInto(out, &returnErr)

	gz := gzip.NewWriter(out)
	gz.Header.ModTime = time.Time{}
	gz.Header.OS = 255
	defer closeInto(gz, &returnErr)
	tarWriter := tar.NewWriter(gz)
	defer closeInto(tarWriter, &returnErr)

	for _, file := range files {
		content, err := fileContent(file)
		if err != nil {
			return err
		}
		header := &tar.Header{Name: filepath.ToSlash(file.name), Mode: int64(file.mode.Perm()), Size: int64(len(content)), ModTime: archiveTime}
		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("writing tar header %s: %w", file.name, err)
		}
		if _, err := tarWriter.Write(content); err != nil {
			return fmt.Errorf("writing tar member %s: %w", file.name, err)
		}
	}
	return nil
}

func writeZIP(path string, files []archiveFile) (returnErr error) {
	out, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	defer closeInto(out, &returnErr)
	zipWriter := zip.NewWriter(out)
	defer closeInto(zipWriter, &returnErr)

	for _, file := range files {
		content, err := fileContent(file)
		if err != nil {
			return err
		}
		header := &zip.FileHeader{Name: filepath.ToSlash(file.name), Method: zip.Deflate}
		header.Modified = archiveTime
		header.SetMode(file.mode)
		member, err := zipWriter.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("writing zip header %s: %w", file.name, err)
		}
		if _, err := member.Write(content); err != nil {
			return fmt.Errorf("writing zip member %s: %w", file.name, err)
		}
	}
	return nil
}

func fileContent(file archiveFile) ([]byte, error) {
	if file.path == "" {
		return file.data, nil
	}
	content, err := os.ReadFile(file.path)
	if err != nil {
		return nil, fmt.Errorf("reading archive member %s: %w", file.path, err)
	}
	return content, nil
}

func closeInto(closer io.Closer, returnErr *error) {
	if err := closer.Close(); err != nil && *returnErr == nil {
		*returnErr = err
	}
}

type bom struct {
	Schema     string      `json:"$schema"`
	Format     string      `json:"bomFormat"`
	Spec       string      `json:"specVersion"`
	Version    int         `json:"version"`
	Metadata   bomMetadata `json:"metadata"`
	Components []component `json:"components,omitempty"`
}

type bomMetadata struct {
	Component  component     `json:"component"`
	Properties []bomProperty `json:"properties,omitempty"`
}

type component struct {
	Type    string    `json:"type"`
	Ref     string    `json:"bom-ref"`
	Name    string    `json:"name"`
	Version string    `json:"version,omitempty"`
	PURL    string    `json:"purl,omitempty"`
	Hashes  []bomHash `json:"hashes,omitempty"`
}

type bomHash struct {
	Algorithm string `json:"alg"`
	Content   string `json:"content"`
}

type bomProperty struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func writeSBOM(path, binaryPath, version string, target target) error {
	info, err := buildinfo.ReadFile(binaryPath)
	if err != nil {
		return fmt.Errorf("reading build information from %s: %w", binaryPath, err)
	}
	binaryHash, err := hashFile(binaryPath)
	if err != nil {
		return err
	}

	components := make([]component, 0, len(info.Deps))
	for _, dependency := range info.Deps {
		module := dependency
		if dependency.Replace != nil {
			module = dependency.Replace
		}
		moduleVersion := module.Version
		if moduleVersion == "" {
			moduleVersion = "unknown"
		}
		purl := goPURL(module.Path, moduleVersion)
		components = append(components, component{Type: "library", Ref: purl, Name: module.Path, Version: moduleVersion, PURL: purl})
	}
	sort.Slice(components, func(i, j int) bool { return components[i].Ref < components[j].Ref })

	rootPURL := goPURL(modulePath+"/cmd/flow", version)
	document := bom{
		Schema:  "http://cyclonedx.org/schema/bom-1.6.schema.json",
		Format:  "CycloneDX",
		Spec:    "1.6",
		Version: 1,
		Metadata: bomMetadata{
			Component: component{
				Type: "application", Ref: rootPURL, Name: "flow", Version: version, PURL: rootPURL,
				Hashes: []bomHash{{Algorithm: "SHA-256", Content: binaryHash}},
			},
			Properties: []bomProperty{
				{Name: "flowstate:go-version", Value: info.GoVersion},
				{Name: "flowstate:target", Value: target.OS + "/" + target.Arch},
			},
		},
		Components: components,
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding SBOM: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("writing SBOM: %w", err)
	}
	return nil
}

func goPURL(path, version string) string {
	return "pkg:golang/" + path + "@" + version
}

func writeChecksums(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("reading release payload: %w", err)
	}
	var lines []string
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "SHA256SUMS" {
			continue
		}
		hash, err := hashFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return err
		}
		lines = append(lines, hash+"  "+entry.Name())
	}
	if err := os.WriteFile(filepath.Join(directory, "SHA256SUMS"), []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		return fmt.Errorf("writing checksums: %w", err)
	}
	return nil
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("opening %s for hashing: %w", path, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hashing %s: %w", path, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
