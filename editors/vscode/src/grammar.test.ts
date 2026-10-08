import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import * as tm from "vscode-textmate";
import * as onig from "vscode-oniguruma";

// The TextMate grammars are tested by tokenizing real Flowfile text with the
// engine VS Code itself uses (vscode-textmate over Oniguruma), against the real
// YAML grammar they inject into. A test over the JSON alone could pass while the
// injection never fired inside a YAML string, which is the one thing that has to
// work.
//
// What is asserted is the boundary this repository owns — where CEL starts and
// stops — not YAML's own scopes, which belong to the YAML grammar.

const root = resolve(__dirname, "..");
const manifest = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8")) as {
  contributes: { grammars: { language?: string; scopeName: string; path: string }[] };
};

const files: Record<string, string> = {
  "source.yaml": resolve(root, "node_modules/tm-grammars/grammars/yaml.json"),
};
for (const g of manifest.contributes.grammars) {
  files[g.scopeName] = resolve(root, g.path);
}

const wasm = readFileSync(resolve(root, "node_modules/vscode-oniguruma/release/onig.wasm"));
const onigLib = onig.loadWASM(wasm.buffer as ArrayBuffer).then(() => ({
  createOnigScanner: (patterns: string[]) => new onig.OnigScanner(patterns),
  createOnigString: (s: string) => new onig.OnigString(s),
}));
const registry = new tm.Registry({
  onigLib,
  loadGrammar: async (scope) => {
    const path = files[scope];
    return path ? tm.parseRawGrammar(readFileSync(path, "utf8"), path) : null;
  },
});

interface Token {
  text: string;
  scopes: string[];
}

async function tokenize(source: string): Promise<Token[]> {
  const grammar = await registry.loadGrammar("source.flowfile");
  assert.ok(grammar, "source.flowfile did not load");
  const out: Token[] = [];
  let state = tm.INITIAL;
  for (const line of source.split("\n")) {
    const result = grammar.tokenizeLine(line, state);
    state = result.ruleStack;
    for (const t of result.tokens) {
      out.push({ text: line.slice(t.startIndex, t.endIndex), scopes: t.scopes });
    }
  }
  return out;
}

/**
 * The classified CEL tokens as `text scope`. Punctuation CEL does not name, such
 * as a dot or a parenthesis, carries only the `source.cel` scope and is skipped:
 * the claim here is what was recognised, and where.
 */
function cel(tokens: Token[]): string[] {
  return tokens
    .filter((t) => t.scopes.includes("source.cel") && t.text.trim() !== "")
    .map((t) => ({ text: t.text, scope: t.scopes[t.scopes.length - 1] }))
    .filter((t) => t.scope !== "source.cel")
    .map((t) => `${t.text} ${t.scope}`);
}

test("a fence in a plain, quoted, single-quoted or block scalar is CEL", async () => {
  const source = [
    "if: ${inputs.n > 1}",
    'message: "hi ${size(x)} and ${y}"',
    "other: 'v ${z}'",
    "block: |",
    "  line ${a.b}",
  ].join("\n");

  assert.deepEqual(cel(await tokenize(source)), [
    "inputs variable.language.cel",
    "n variable.other.property.cel",
    "> keyword.operator.cel",
    "1 constant.numeric.cel",
    "size entity.name.function.cel",
    "x variable.other.cel",
    "y variable.other.cel",
    "z variable.other.cel",
    "a variable.other.cel",
    "b variable.other.property.cel",
  ]);
});

test("text between fences stays YAML's", async () => {
  const tokens = await tokenize('message: "hi ${x} and more"');
  const between = tokens.filter((t) => t.text === " and more");
  assert.equal(between.length, 1);
  assert.ok(!between[0].scopes.includes("source.cel"));
  assert.ok(!between[0].scopes.includes("meta.embedded.expression.flowfile"));
});

test("a closing brace inside a map literal or a string does not end the fence", async () => {
  const got = cel(await tokenize(`v: \${{'a': 1}.a + "}" + b}`));

  assert.deepEqual(got, [
    "' string.quoted.single.cel",
    "a string.quoted.single.cel",
    "' string.quoted.single.cel",
    ": keyword.operator.cel",
    "1 constant.numeric.cel",
    "a variable.other.property.cel",
    "+ keyword.operator.cel",
    '" string.quoted.double.cel',
    "} string.quoted.double.cel",
    '" string.quoted.double.cel',
    "+ keyword.operator.cel",
    "b variable.other.cel",
  ]);
});

test("must: is bare CEL, plain or quoted, and stops at a YAML comment", async () => {
  const source = [
    "must: this > 0",
    "must: 'this.matches(r\"^v\")'",
    "must: this != \"\" # why",
  ].join("\n");
  const tokens = await tokenize(source);
  const got = cel(tokens);

  assert.ok(got.includes("this variable.language.cel"));
  assert.ok(got.includes("matches entity.name.function.member.cel"));
  assert.ok(got.includes("!= keyword.operator.cel"));
  const comment = tokens.find((t) => t.text.includes("why"));
  assert.ok(comment, "the trailing comment was not tokenized");
  assert.ok(!comment.scopes.includes("source.cel"), "a YAML comment must not be CEL");
});

test("prose that looks like CEL is not CEL", async () => {
  // `description:` is a literal; so is a fence-looking string in a YAML comment.
  const source = [
    "description: this > 0",
    "# a comment mentioning ${inputs.n}",
    "id: must",
    "name: not must: this > 0",
  ].join("\n");

  assert.deepEqual(cel(await tokenize(source)), []);
});

test("the contributed grammars exist and name the scopes they contribute", () => {
  assert.ok(manifest.contributes.grammars.length >= 2, "expected the flowfile and cel grammars");
  for (const g of manifest.contributes.grammars) {
    const path = resolve(root, g.path);
    assert.ok(existsSync(path), `${g.path} is contributed and missing`);
    const body = JSON.parse(readFileSync(path, "utf8")) as { scopeName: string };
    assert.equal(body.scopeName, g.scopeName, `${g.path} declares a different scopeName`);
  }
  assert.ok(
    manifest.contributes.grammars.some((g) => g.language === "flowfile" && g.scopeName === "source.flowfile"),
    "the flowfile language has no grammar",
  );
});
