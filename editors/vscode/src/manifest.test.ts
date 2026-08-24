import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

// The two settings that decide what this extension executes must be
// `machine`-scoped, and the distinction is not a matter of taste.
//
// VS Code's `machine-overridable` scope means exactly what its name says: the
// machine value is a default a workspace may override, so a `.vscode/settings.json`
// committed to a repository is read. `activate()` probes the configured path and
// starts it as the language server, so under that scope, opening a cloned
// repository runs an executable that repository chose.
//
// `machine` is the scope that does not read workspace or folder settings. Both
// settings shipped as `machine-overridable` while their own descriptions promised
// the property only `machine` provides — the manifest said one thing and did
// another, and nothing could see the difference, which is why this is a test
// rather than a comment. Codex found it on review.
//
// Read from the manifest on disk rather than a copy, because the manifest is what
// VS Code actually reads: a test that asserts against its own constant would agree
// with itself forever.
const manifest = JSON.parse(
  readFileSync(resolve(__dirname, "..", "package.json"), "utf8"),
) as {
  contributes: {
    configuration: {
      properties: Record<string, { scope?: string; description?: string }>;
    };
  };
};

const properties = manifest.contributes.configuration.properties;

test("every configuration setting declares a scope", () => {
  const names = Object.keys(properties);

  // The anti-vacuity guard. A rename of `contributes.configuration` would make
  // the loops below iterate over nothing and pass, on a manifest nobody read.
  assert.ok(names.length >= 2, `expected at least two settings, found ${names.length}`);

  for (const name of names) {
    assert.ok(
      properties[name].scope !== undefined,
      `${name} declares no scope; VS Code then defaults it to a workspace-readable one`,
    );
  }
});

test("the settings that choose what runs are machine-scoped", () => {
  for (const name of ["flowstate.path", "flowstate.lsp.args"]) {
    assert.ok(properties[name], `${name} is missing from the manifest`);
    assert.equal(
      properties[name].scope,
      "machine",
      `${name} must be "machine": "machine-overridable" lets a cloned repository's ` +
        `.vscode/settings.json choose the executable this extension launches`,
    );
  }
});

test("no setting is machine-overridable", () => {
  // The negative direction, and the one that catches the next setting rather
  // than re-checking the two above. A new setting added with the scope that
  // reads workspace files fails here even if nobody thinks to list it.
  for (const [name, property] of Object.entries(properties)) {
    assert.notEqual(
      property.scope,
      "machine-overridable",
      `${name} is machine-overridable, so a workspace can override it`,
    );
  }
});

// An example in a setting's description is documentation a user copies, and this
// one is documentation that stopped working.
//
// `flow lsp` refuses a relative --plugin-dir (#958): an editor starts the
// language server with the opened workspace as its working directory, so a
// relative path names a directory inside whatever repository happens to be open
// — the same reason both settings above are `machine`. The manifest still
// advertised `["--plugin-dir", "./plugins"]`, so the one example a VS Code user
// is shown in the settings UI was the one shape the command now rejects.
//
// Written against every description rather than that one string, so the next
// example added anywhere in the manifest is caught too. Nothing else compares
// this file to the Go code, and CI cannot: the manifest is hand-written and the
// refusal lives in another language.
test("no setting advertises a plugin directory the language server would refuse", () => {
  const described = Object.entries(properties).filter(
    ([, property]) => property.description !== undefined,
  );

  // The anti-vacuity guard, as above: a manifest whose descriptions were all
  // renamed away would iterate over nothing and pass.
  assert.ok(
    described.length >= 2,
    `expected at least two described settings, found ${described.length}`,
  );

  for (const [name, property] of described) {
    const description = property.description as string;
    // The separator class deliberately excludes `/`: `\W+` would swallow the
    // leading slash of an absolute path along with the `", "` before it, and
    // then report every path as relative — a guard that fails on the shape it
    // exists to permit.
    for (const match of description.matchAll(/--plugin-dir[",\s]*([^"\s,\]]+)/g)) {
      const path = match[1];
      assert.ok(
        path.startsWith("/"),
        `${name} shows --plugin-dir ${path}, and \`flow lsp\` refuses a relative ` +
          `plugin directory: an editor starts it in the opened workspace`,
      );
    }
  }
});

// The test-file associations must not expose workflow commands (Codex, #1109):
// a `*.test.yaml` shares the `flowfile` language id, so a `when` clause gated
// on the language alone offers `flow validate` on a suite — a command that can
// only refuse it. Every workflow-only entry therefore excludes both test-file
// shapes, and `flowstate.test` excludes the shared fixture file, which no
// command takes directly. Read from the manifest on disk, as above: the menus
// are what VS Code evaluates, and a test asserting its own constant would
// agree with itself forever.
const menus = (
  JSON.parse(readFileSync(resolve(__dirname, "..", "package.json"), "utf8")) as {
    contributes: { menus: Record<string, { command: string; when?: string }[]> };
  }
).contributes.menus;

test("workflow-only commands exclude the test-file shapes in every menu", () => {
  const workflowOnly = new Set(["flowstate.validate", "flowstate.fix", "flowstate.runLocal"]);
  let checked = 0;
  for (const entries of Object.values(menus)) {
    for (const entry of entries) {
      if (!entry.when) {
        continue;
      }
      if (workflowOnly.has(entry.command)) {
        checked += 1;
        assert.match(entry.when, /testdefaults\.yaml/, `${entry.command}: ${entry.when}`);
        assert.match(entry.when, /test\\\.ya\?ml/, `${entry.command}: ${entry.when}`);
      }
      if (entry.command === "flowstate.test") {
        checked += 1;
        assert.match(entry.when, /testdefaults\.yaml/, `${entry.command}: ${entry.when}`);
      }
    }
  }
  // The anti-vacuity guard, in the shape the scope test above uses: three
  // workflow-only commands and one test command, in two menus each.
  assert.equal(checked, 8, `expected all eight gated menu entries, saw ${checked}`);
});
