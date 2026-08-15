import { test } from "node:test";
import assert from "node:assert/strict";
import { flowCommandArgs, commandTitle } from "./commandLine";

test("validate", () => {
  assert.deepEqual(flowCommandArgs("validate", "Flowfile.yaml"), ["validate", "Flowfile.yaml"]);
});

test("test", () => {
  assert.deepEqual(flowCommandArgs("test", "Flowfile.yaml"), ["test", "Flowfile.yaml"]);
});

test("fix", () => {
  assert.deepEqual(flowCommandArgs("fix", "Flowfile.yaml"), ["fix", "Flowfile.yaml"]);
});

test("runLocal is `run local`, not `runLocal`", () => {
  assert.deepEqual(flowCommandArgs("runLocal", "Flowfile.yaml"), ["run", "local", "Flowfile.yaml"]);
});

test("the file path is passed through untouched, spaces and all", () => {
  assert.deepEqual(flowCommandArgs("validate", "/a path/with spaces/Flowfile.yaml"), [
    "validate",
    "/a path/with spaces/Flowfile.yaml",
  ]);
});

test("every kind has a distinct, human-legible title", () => {
  const kinds: Array<Parameters<typeof commandTitle>[0]> = ["validate", "test", "fix", "runLocal"];
  const titles = kinds.map(commandTitle);
  assert.equal(new Set(titles).size, titles.length, "titles must be unique");
  for (const t of titles) {
    assert.match(t, /^Flowstate: /);
  }
});
