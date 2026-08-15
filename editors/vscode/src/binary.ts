// Resolving and probing the `flow` binary. No `vscode` import: this module
// takes plain strings in and reports plain results out, so binary.test.ts can
// exercise it without a running editor. extension.ts is what turns a
// `NotFound` result into a `window.showErrorMessage` with a settings link.

import { spawn } from "node:child_process";

// resolveBinaryPath mirrors the sketch in #585: an empty or unset
// configuration value falls back to `flow` on PATH, exactly like the
// `cfg.get<string>('path', 'flow')` default in the design's TypeScript.
export function resolveBinaryPath(configured: string | undefined): string {
  const trimmed = (configured ?? "").trim();
  return trimmed.length > 0 ? trimmed : "flow";
}

export type BinaryCheck =
  | { ok: true }
  | { ok: false; reason: "not-found"; detail: string }
  | { ok: false; reason: "error"; detail: string };

// checkBinaryAvailable runs `<bin> <probeArgs>` (default: `version`) and
// reports whether the process could be spawned and exited zero. It exists so
// the extension can fail with a legible message — "flow is not on your
// PATH, and flowstate.path is not set" — rather than silently doing nothing,
// which is the failure mode #585 calls out by name.
//
// This is a real process launch, so callers must not run it on a keystroke
// path (e.g. is not called on every document change) — only on activation
// and before a command a person explicitly invoked.
export function checkBinaryAvailable(bin: string, probeArgs: string[] = ["version"]): Promise<BinaryCheck> {
  return new Promise((resolve) => {
    let settled = false;
    const child = spawn(bin, probeArgs, { stdio: "ignore" });

    child.once("error", (err: NodeJS.ErrnoException) => {
      if (settled) {
        return;
      }
      settled = true;
      if (err.code === "ENOENT") {
        resolve({
          ok: false,
          reason: "not-found",
          detail: `"${bin}" was not found. Set flowstate.path to the flow binary, or put flow on your PATH.`,
        });
        return;
      }
      resolve({ ok: false, reason: "error", detail: err.message });
    });

    child.once("exit", (code, signal) => {
      if (settled) {
        return;
      }
      settled = true;
      if (code === 0) {
        resolve({ ok: true });
        return;
      }
      resolve({
        ok: false,
        reason: "error",
        detail: signal
          ? `"${bin} ${probeArgs.join(" ")}" was killed by signal ${signal}.`
          : `"${bin} ${probeArgs.join(" ")}" exited with status ${code}.`,
      });
    });
  });
}
