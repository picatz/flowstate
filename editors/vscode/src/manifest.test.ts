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
      properties: Record<string, { scope?: string }>;
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
