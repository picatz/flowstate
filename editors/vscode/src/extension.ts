// The VS Code client for `flow lsp` (#585). Everything this file knows about
// a Flowfile is "it is the language server's job" — diagnostics, hover,
// completion, definition, symbols, formatting and the fix-all action all
// arrive from vscode-languageclient wiring the server's own answers to the
// corresponding VS Code surface. This file's job is to launch that server
// correctly, fail legibly when it cannot, and shell out to the same binary
// for validate/test/fix/run local so the palette never re-implements what
// the CLI already does.
//
// No YAML parsing, no diagnostics of this extension's own, no policy
// evaluation — see CLAUDE.md and #585 §3. If you are about to add one of
// those here, it belongs in flowfile/validate.go instead.

import * as vscode from "vscode";
import { LanguageClient, LanguageClientOptions, ServerOptions, TransportKind } from "vscode-languageclient/node";
import { checkBinaryAvailable, resolveBinaryPath } from "./binary";
import { commandTitle, flowCommandArgs, FlowCommandKind } from "./commandLine";

let client: LanguageClient | undefined;
let outputChannel: vscode.LogOutputChannel | undefined;

function config(): vscode.WorkspaceConfiguration {
  return vscode.workspace.getConfiguration("flowstate");
}

function configuredBinary(): string {
  return resolveBinaryPath(config().get<string>("path"));
}

// missingBinaryMessage surfaces the failure #585 asks for by name: "fail
// with a legible message when [the binary] is missing rather than silently
// doing nothing." It never guesses further than that — the offer is to open
// the setting, not to search the filesystem for a `flow` the editor didn't
// configure.
async function warnBinaryMissing(bin: string, detail: string): Promise<void> {
  const openSettings = "Open Settings";
  const choice = await vscode.window.showErrorMessage(
    `Flowstate: could not run "${bin}". ${detail}`,
    openSettings,
  );
  if (choice === openSettings) {
    await vscode.commands.executeCommand("workbench.action.openSettings", "flowstate.path");
  }
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  // `{ log: true }` makes this a LogOutputChannel, which is what
  // vscode-languageclient 10 requires: it drives the channel's log level
  // from the editor rather than from a trace setting the client owns.
  outputChannel = vscode.window.createOutputChannel("Flowstate Language Server", { log: true });
  context.subscriptions.push(outputChannel);

  context.subscriptions.push(
    vscode.commands.registerCommand("flowstate.restartLanguageServer", async () => {
      if (!client) {
        await startLanguageClient(context);
        return;
      }
      await client.restart();
    }),
  );

  for (const kind of ["validate", "test", "fix", "runLocal"] as FlowCommandKind[]) {
    context.subscriptions.push(
      vscode.commands.registerCommand(`flowstate.${kind}`, () => runFlowCommand(kind)),
    );
  }

  // A change to either setting only takes effect for new runs — the LSP
  // connection is not torn down and rebuilt on every keystroke in Settings,
  // only when the user asks (restart command) or reloads the window, which
  // matches how vscode-languageclient itself expects `command`/`args` to be
  // treated as fixed at construction.
  await startLanguageClient(context);
}

async function startLanguageClient(context: vscode.ExtensionContext): Promise<void> {
  const bin = configuredBinary();
  const check = await checkBinaryAvailable(bin);
  if (!check.ok) {
    await warnBinaryMissing(bin, check.detail);
    return;
  }

  const cfg = config();
  const serverOptions: ServerOptions = {
    command: bin,
    args: ["lsp", ...cfg.get<string[]>("lsp.args", [])],
    transport: TransportKind.stdio,
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "flowfile" }],
    outputChannel,
  };

  const newClient = new LanguageClient("flowstate", "Flowstate", serverOptions, clientOptions);
  context.subscriptions.push({ dispose: () => void newClient.stop() });

  try {
    await newClient.start();
    client = newClient;
  } catch (err) {
    const detail = err instanceof Error ? err.message : String(err);
    await warnBinaryMissing(bin, `The language server failed to start: ${detail}`);
  }
}

// runFlowCommand is the entire body of every palette command: resolve the
// active Flowfile, resolve the binary, and hand both to a VS Code Task so
// the CLI's own stdout/stderr and exit status are what the user sees. It
// composes a command line (commandLine.ts) and shows the output — it does
// not parse that output to re-decide whether something passed.
async function runFlowCommand(kind: FlowCommandKind): Promise<void> {
  const editor = vscode.window.activeTextEditor;
  if (!editor || editor.document.languageId !== "flowfile") {
    void vscode.window.showErrorMessage("Flowstate: open a Flowfile first.");
    return;
  }

  // The menus already hide what cannot succeed here (package.json's `when`
  // clauses), and this is the same decision for the paths that bypass menus —
  // a keybinding, another extension. A test suite takes only `flow test`, and
  // the shared fixture file takes nothing directly: `flow validate` on a
  // suite would refuse it as a workflow, which reads as breakage rather than
  // guidance.
  const baseName = editor.document.uri.path.split("/").pop() ?? "";
  const isSuite = /\.test\.ya?ml$/.test(baseName);
  if (baseName === "testdefaults.yaml" || (isSuite && kind !== "test")) {
    void vscode.window.showErrorMessage(
      baseName === "testdefaults.yaml"
        ? 'Flowstate: testdefaults.yaml is a directory\'s shared fixture; run "Flowstate: Test Flowfile" on the suites beside it.'
        : `Flowstate: ${baseName} is a test suite — run "Flowstate: Test Flowfile" (flow test) on it.`,
    );
    return;
  }

  const bin = configuredBinary();
  const check = await checkBinaryAvailable(bin);
  if (!check.ok) {
    await warnBinaryMissing(bin, check.detail);
    return;
  }

  const document = editor.document;
  if (document.isDirty) {
    await document.save();
  }

  const filePath = document.uri.fsPath;
  const args = flowCommandArgs(kind, filePath);
  const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);

  const task = new vscode.Task(
    { type: "flowstate", command: kind },
    workspaceFolder ?? vscode.TaskScope.Workspace,
    commandTitle(kind),
    "flowstate",
    new vscode.ProcessExecution(bin, args, {
      cwd: workspaceFolder?.uri.fsPath,
    }),
  );
  task.presentationOptions = {
    reveal: vscode.TaskRevealKind.Always,
    panel: vscode.TaskPanelKind.Dedicated,
    clear: true,
  };

  await vscode.tasks.executeTask(task);
}

export async function deactivate(): Promise<void> {
  if (client) {
    await client.stop();
  }
}
