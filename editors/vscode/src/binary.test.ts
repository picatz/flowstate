import { test } from "node:test";
import assert from "node:assert/strict";
import { resolveBinaryPath, checkBinaryAvailable } from "./binary";

test("resolveBinaryPath falls back to 'flow' when unset", () => {
  assert.equal(resolveBinaryPath(undefined), "flow");
});

test("resolveBinaryPath falls back to 'flow' when blank", () => {
  assert.equal(resolveBinaryPath("   "), "flow");
});

test("resolveBinaryPath passes through a configured path unchanged", () => {
  assert.equal(resolveBinaryPath("/usr/local/bin/flow"), "/usr/local/bin/flow");
});

test("checkBinaryAvailable reports not-found for a binary that does not exist", async () => {
  const result = await checkBinaryAvailable("flowstate-definitely-not-a-real-binary-xyz");
  assert.equal(result.ok, false);
  if (!result.ok) {
    assert.equal(result.reason, "not-found");
    assert.match(result.detail, /flowstate\.path/);
  }
});

test("checkBinaryAvailable reports ok for a real binary exiting zero", async () => {
  // `node --version` stands in for `flow version`: both are "run this
  // command with no meaningful args and expect exit 0", which is all this
  // function inspects.
  const result = await checkBinaryAvailable(process.execPath, ["--version"]);
  assert.deepEqual(result, { ok: true });
});

test("checkBinaryAvailable reports error for a real binary exiting non-zero", async () => {
  const result = await checkBinaryAvailable(process.execPath, ["--this-flag-does-not-exist"]);
  assert.equal(result.ok, false);
  if (!result.ok) {
    assert.equal(result.reason, "error");
  }
});
