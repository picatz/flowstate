// Command flowstate-plugin-github provides two GitHub forge tasks:
// github.pull_request_get (read) and github.issue_comment (write), proving
// one forge operation end to end rather than a wide, thin API surface. See
// the README for what was deliberately left out and why.
//
// # Naming: why github.* and not forge.*
//
// This plugin's sibling, flowstate-plugin-vcs, ships task names -
// vcs.log, vcs.diff - meant to mean the same thing regardless of which
// version-control backend implements them. This plugin does not attempt
// that: its tasks are named github.pull_request_get and github.issue_comment,
// admitting plainly that a Flowfile using them only runs against GitHub.
//
// That is not an oversight; it is what the current schema allows. A
// [pluginv1.TaskManifest]'s name is qualified by the plugin's own advertised
// name - `qualified := p.name + "." + name` in
// pkg/flowstate/v1/plugin/task.go - and a plugin's name is one string,
// checked at startup against a pattern with no dot in it
// (`^[a-z0-9][a-z0-9-]*$` in plugin.proto). One plugin process therefore
// gets exactly one qualifier for every task it provides; there is no way,
// today, for this binary to expose some tasks as "forge.pull_request_get"
// and others as "github.check_run.create" the way a design with a portable
// forge vocabulary would want. Making that possible is a schema change -
// either relaxing TaskManifest.name to permit a caller-chosen qualifier
// segment, or adding a field that lets a manifest entry declare its own
// prefix independent of the plugin's name - and this plugin does not make
// that change; it is reported in the README instead.
//
// If a portable "forge.*" vocabulary existed today, pull_request_get would
// be the task to carry it: creating, reading, listing, and commenting on a
// pull or merge request are the operations every major forge (GitHub,
// GitLab, Gitea) implements, even though the objects are not identical
// (a GitLab "merge request" and a GitHub "pull request" differ in exactly
// the ways a lossy shared vocabulary would paper over). issue_comment is
// nearly as portable - GitHub, GitLab, and Gitea all have issues and
// comments. Neither would be named after GitHub if this build's task naming
// were free to assume a portable vocabulary already existed.
package main
