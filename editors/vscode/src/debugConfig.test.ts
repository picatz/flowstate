import { test } from "node:test";
import assert from "node:assert/strict";
import { adapterArgs, resolveDebugConfig, withAbsoluteProgram } from "./debugConfig";

test("the adapter command is `flow dap` followed by the machine-scoped arguments, in order", () => {
  assert.deepEqual(adapterArgs([]), ["dap"]);
  assert.deepEqual(adapterArgs(["--plugin-dir", "/opt/plugins", "--task-policy", "/etc/p.yaml"]), [
    "dap",
    "--plugin-dir",
    "/opt/plugins",
    "--task-policy",
    "/etc/p.yaml",
  ]);
});

test("F5 on a workflow with no launch.json launches that workflow", () => {
  const resolved = resolveDebugConfig({}, "/repo/workflow.yaml");
  assert.ok(resolved.ok);
  assert.equal(resolved.config.type, "flowstate");
  assert.equal(resolved.config.request, "launch");
  assert.equal(resolved.config.program, "/repo/workflow.yaml");
});

test("an explicit program is never replaced by the active file", () => {
  const resolved = resolveDebugConfig(
    { type: "flowstate", request: "launch", program: "/repo/other.yaml", stopOnEntry: false },
    "/repo/workflow.yaml",
  );
  assert.ok(resolved.ok);
  assert.equal(resolved.config.program, "/repo/other.yaml");
  // Everything the adapter reads passes through untouched.
  assert.equal(resolved.config.stopOnEntry, false);
});

test("an explicit program is not second-guessed even when it names a test file", () => {
  // The adapter owns that refusal, with the compiler's own words; the client
  // only declines to guess on the active file's behalf.
  const resolved = resolveDebugConfig({ request: "launch", program: "/repo/a.test.yaml" }, undefined);
  assert.ok(resolved.ok);
  assert.equal(resolved.config.program, "/repo/a.test.yaml");
});

test("a launch with no program and no Flowfile open is refused, saying what to do", () => {
  const resolved = resolveDebugConfig({ request: "launch" }, undefined);
  assert.ok(!resolved.ok);
  assert.match(resolved.message, /"program"/);
});

test("the active file is not launched when it is a test suite or the shared fixture", () => {
  for (const file of ["/repo/orders.test.yaml", "/repo/orders.test.yml", "C:\\repo\\testdefaults.yaml"]) {
    const resolved = resolveDebugConfig({}, file);
    assert.ok(!resolved.ok, file);
    assert.match(resolved.message, /flow test --debug/, file);
  }
});

test("an attach keeps its run and optional program exactly as written", () => {
  const resolved = resolveDebugConfig(
    { type: "flowstate", request: "attach", workflowId: "wf-1", program: "/repo/workflow.yaml" },
    "/repo/other.yaml",
  );
  assert.ok(resolved.ok);
  assert.equal(resolved.config.request, "attach");
  assert.equal(resolved.config.workflowId, "wf-1");
  assert.equal(resolved.config.program, "/repo/workflow.yaml");
});

test("an attach does not gain a program from the active file", () => {
  // The program of an attach maps lines only when it is byte for byte the file
  // the run executes; guessing the active file would put frames on a file the
  // run never read.
  const resolved = resolveDebugConfig({ request: "attach", workflowId: "wf-1" }, "/repo/workflow.yaml");
  assert.ok(resolved.ok);
  assert.equal("program" in resolved.config, false);
});

test("an unknown request is refused by name", () => {
  const resolved = resolveDebugConfig({ request: "restart" }, "/repo/workflow.yaml");
  assert.ok(!resolved.ok);
  assert.match(resolved.message, /"restart"/);
});

test("a relative program is resolved against the debugged folder, never left for the adapter's directory", () => {
  assert.equal(withAbsoluteProgram({ program: "flows/workflow.yaml" }, "/repo").program, "/repo/flows/workflow.yaml");
  // An attach's optional program is made absolute the same way.
  assert.equal(
    withAbsoluteProgram({ request: "attach", workflowId: "wf-1", program: "workflow.yaml" }, "/repo").program,
    "/repo/workflow.yaml",
  );
});

test("an absolute program, or none, is left exactly as written", () => {
  assert.equal(withAbsoluteProgram({ program: "/elsewhere/workflow.yaml" }, "/repo").program, "/elsewhere/workflow.yaml");
  assert.equal(withAbsoluteProgram({ program: "C:\\flows\\workflow.yaml" }, "/repo").program, "C:\\flows\\workflow.yaml");
  assert.equal("program" in withAbsoluteProgram({ workflowId: "wf-1" }, "/repo"), false);
  assert.equal(withAbsoluteProgram({ program: "workflow.yaml" }, undefined).program, "workflow.yaml");
});

test("resolving before variable substitution never joins the folder onto a variable", () => {
  // VS Code's first resolve hook sees `${file}` unsubstituted; that is why
  // absolutizing happens in the hook that runs after substitution.
  const resolved = resolveDebugConfig({ request: "launch", program: "${file}" }, undefined);
  assert.ok(resolved.ok);
  assert.equal(resolved.config.program, "${file}");
});
