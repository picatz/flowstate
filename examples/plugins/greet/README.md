# A task a plugin provides

[`workflow.yaml`](workflow.yaml) has one step that is not a built-in:

```yaml
  - id: hello
    example.greet:
      name: ${vars.who}
      greeting: Hello
```

`example.greet` is the `greet` task of the `example` plugin — the worked plugin in
[`pkg/flowstate/v1/plugin/examples/flowstate-plugin-example`](../../../pkg/flowstate/v1/plugin/examples/flowstate-plugin-example),
which advertises both a secrets backend and a task from one process. Nothing about
the step is special: it takes inputs, it produces outputs a later step reads, and
its schema is checked before it runs. What is special is that the schema belongs to
the plugin. The engine has never compiled `example.v1.GreetInputs`; it learns the
shape from descriptors the plugin ships in its manifest and hands over at launch.

## Running it

A plugin is a separate executable a worker launches, so this example needs two
things a Flowfile cannot carry: the binary, and a worker told where to look.

```console
$ mkdir -p ./plugins
$ go build -o ./plugins/flowstate-plugin-example \
    ./pkg/flowstate/v1/plugin/examples/flowstate-plugin-example
```

The name matters. Discovery looks for executables called `flowstate-plugin-*` in
the directories it is given, and refuses a directory other users can write to —
a plugin directory is a list of programs this worker will execute.

Check what a worker would find, without starting one:

```console
$ flow plugins --plugin-dir ./plugins
```

Then start a worker over the same directory, and run the file against it:

```console
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins &
$ flow server &
$ flow run examples/plugins/greet/workflow.yaml
```

`flow run local` has no `--plugin-dir`: the local driver runs in whatever process
invoked it, and that process launches nothing. So this is the one example in this
directory tree that a rehearsal cannot run — the worker is the path, which is also
the path production uses.

## Why this is not `examples/<name>/workflow.yaml`

Every other example lives one directory down, and the corpus is enumerated as
`examples/*/workflow.yaml` by CI, by `flow validate examples/*/workflow.yaml`, and
by half a dozen tests that check each file with the built-in task registry. This
one is deliberately outside that glob, because with no plugin loaded the right
answer for this file is a diagnostic:

```console
$ flow validate examples/plugins/greet/workflow.yaml
examples/plugins/greet/workflow.yaml:25:5: step "hello": no plugin task "example.greet" is
registered here; if the "example" plugin is installed on the worker this will run on, the
file is fine and this process simply has not loaded it — `flow plugins` shows what a
plugin directory provides
```

That diagnostic is correct and is worth keeping correct. Whether a plugin is
installed is a deployment's decision, not a property of the file — so a checker
that has not been told about one says what it does not know rather than passing the
file silently, and the corpus checks stay strict for everything else rather than
growing an exception.

It is still run in CI, and by more than a validator: `TestAFlowfileCanNameAPluginTask`
in `pkg/flowstate/v1/plugin` builds this plugin, launches it, registers it into the
registry the engine reads, and then validates and executes *this file* — the same
bytes, from disk. `TestThePluginExampleIsAShippedFile` beside it runs with no
toolchain and no plugin at all, so a rename or a deletion here is a red test either
way, and the package refuses to finish green if the running half was silently
skipped.
