// Pure construction of the `flowstate` debug type's adapter command and of a
// launch configuration for the active file. No `vscode` import: this module
// takes plain strings in and gives plain values out, so debugConfig.test.ts
// can exercise it without a running editor.
//
// The extension launches `flow dap` and decides nothing a debugger decides:
// breakpoints, stepping, conditions and scopes are the adapter's. What lives
// here is argv, and the one convenience a debug type owes a person who pressed
// F5 on a file with no launch.json: a launch configuration naming that file.

// adapterArgs is the argv (excluding the binary) that starts the adapter. The
// extra arguments are the machine-scoped `flowstate.dap.args` setting — the
// plugin directory, the deployment policy flags and the server address an
// attach reaches — so a workspace cannot choose them.
export function adapterArgs(extra: readonly string[]): string[] {
  return ["dap", ...extra];
}

// DebugConfig is a launch.json entry. VS Code adds its own bookkeeping keys
// (`__sessionId`, `__configurationTarget`, ...) and the adapter ignores keys
// it does not read, so this names only what is decided here.
export interface DebugConfig {
  type?: string;
  request?: string;
  name?: string;
  program?: string;
  [key: string]: unknown;
}

export type Resolved =
  | { ok: true; config: DebugConfig }
  | { ok: false; message: string };

// A test suite and the shared fixture file are Flowfile-language documents the
// adapter cannot run as a workflow: `flow test --debug` is how a case is
// stepped, and it is a terminal command rather than a debug request.
function isRunnable(file: string): boolean {
  const base = file.split(/[\\/]/).pop() ?? "";
  return base !== "testdefaults.yaml" && !/\.test\.ya?ml$/.test(base);
}

// resolveDebugConfig fills in what a launch without a program, or no
// configuration at all, leaves out, and refuses what cannot run.
//
// activeFile is the active Flowfile's path, or undefined when the active editor
// is not a Flowfile. An explicit `program` is never replaced: a configuration
// that names one means that one, and a wrong path is the adapter's to refuse
// with the compiler's own words.
export function resolveDebugConfig(config: DebugConfig, activeFile: string | undefined): Resolved {
  const request = config.request ?? "launch";
  if (request === "attach") {
    // Attach names a durable run, never a file; the optional `program` only
    // maps lines and is left exactly as written.
    return { ok: true, config: { ...config, type: "flowstate", request, name: config.name ?? "Attach to a durable run" } };
  }
  if (request !== "launch") {
    return { ok: false, message: `Flowstate: "${request}" is not a debug request; use "launch" or "attach".` };
  }
  const program = config.program ?? activeFile;
  if (program === undefined || program === "") {
    return {
      ok: false,
      message: 'Flowstate: a launch needs a "program"; open a Flowfile or set "program" in launch.json.',
    };
  }
  if (config.program === undefined && !isRunnable(program)) {
    return {
      ok: false,
      message: `Flowstate: ${program.split(/[\\/]/).pop()} is a test file, not a workflow; open the workflow to debug, or run "flow test --debug" on the case.`,
    };
  }
  return {
    ok: true,
    config: { ...config, type: "flowstate", request, name: config.name ?? "Debug this Flowfile", program },
  };
}
