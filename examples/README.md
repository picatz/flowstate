# Examples

Each directory holds one `workflow.yaml` demonstrating a feature. Run any of them locally,
without Temporal:

```console
$ flow run local examples/hello-world/workflow.yaml
```

Check one without running it — worth preferring for the network examples, since running
them makes real requests:

```console
$ flow validate examples/hello-world/workflow.yaml
```

| Example | Shows | Network |
| --- | --- | --- |
| [hello-world](hello-world) | The smallest possible workflow: one `log:` step | no |
| [hello-world-multi-step](hello-world-multi-step) | Several steps in order, each reading a value named once at the top | no |
| [logging](logging) | `log:` — a message for a person to read, with `level:` and `fields:`, and no outputs | no |
| [string-formatting](string-formatting) | `format()` from the profile, building a message from a var | no |
| [conditional-and-retry](conditional-and-retry) | `if:`, `timeout:`, `retry:` and `continue_on_error:` per step, tolerating a step that really does fail | no |
| [fan-out-and-parallel](fan-out-and-parallel) | `for_each` fan-out over a computed list, and concurrent `parallel:` branches | no |
| [workflow-vars](workflow-vars) | `vars:` at the top of a file, read as `vars.<name>`, beside a loop's bare binding | no |
| [step-vars](step-vars) | `vars:` on a step and on a loop, bare and private to what declares them | no |
| [expressions](expressions) | Expressions as values: a step's own `vars:`, and one dialect an `if:` reaches too | no |
| [approval-gate](approval-gate) | `wait_for_signal:` as a human approval gate, and branching on `payload` versus `timed_out` | no |
| [wait-until-a-moment](wait-until-a-moment) | `wait_until:` a computed moment, with `now` and the duration builders | no |
| [headers-and-nested](headers-and-nested) | Request headers, and selecting into a nested result | yes |
| [http-json](http-json) | Parsing a JSON body with `json_parse`, named once in a step's `vars:` | yes |
| [http-query-and-json](http-query-and-json) | `query:` parameters, a structured `json:` body, and `parse_json:` | yes |
| [http-form](http-form) | A url-encoded `form:` body, as OAuth token endpoints expect | yes |
| [http-expect](http-expect) | `expect:` — accepting a 404, and rejecting a 200 with an error in the body | yes |
| [http-output-shaping](http-output-shaping) | Returning only chosen fields from a response via `outputs` | yes |
| [simple-http-multi-step](simple-http-multi-step) | Using a response status code in a later step | yes |
| [edition-and-descriptions](edition-and-descriptions) | `description:` as a property of the step, and the required `edition:` naming the grammar the file is written in | no |

The examples marked as needing network reach `httpbin.org`. They will fail without internet
access, and the `http` task's egress policy denies internal addresses by default — see
[Governed network access](../README.md#governed-network-access) if you point one at a
service on `localhost`.
