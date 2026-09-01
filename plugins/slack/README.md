# Slack outbound notification plugin

`slack.post` sends one accessible text message through Slack's
`chat.postMessage`. It exists to notify a human that a durable approval or other
human-in-the-loop gate needs attention. It is intentionally not a Slack API
wrapper: no Block Kit, attachments, lookup, reactions, updates, file uploads,
webhooks, socket mode, or inbound interaction handling.

## Contract

Inputs are `token`, `channel`, `text`, `message_key`, and optional `thread_ts`;
outputs are the acknowledged `channel` and Slack message timestamp `ts`.
Channel names are refused in favor of stable C/D/G conversation IDs. Text is
valid UTF-8 capped at 4,000 characters, and resolved credentials are capped at
4 KiB before entering a header. Link and media unfurling are always off, so
notification text cannot ask Slack to fetch an arbitrary URL. Responses are read
through a 64 KiB `netpolicy` ceiling supplied by the operator policy.

`message_key` is a canonical UUID sent as Slack's `client_msg_id`. Reuse it only
for the identical logical message and destination. Slack's official method
reference names duplicate-related errors for `client_msg_id`, but does not make
a complete deduplication guarantee. Therefore:

- a clean Slack success returns `channel` and `ts`;
- HTTP 429, explicit `ratelimited` responses, and an operator rate bucket that
  refuses the initial hop are definite no-write outcomes and return retryable
  `UnavailableAfter`, with the delay capped at five minutes;
- authentication, destination, and input refusals are permanent;
- a timeout, connection loss, malformed acknowledgement, HTTP 5xx, or Slack
  `internal_error`/`fatal_error` is `OutcomeUnknown` and is never retried
  automatically, because the message may already exist;
- there are no hidden retries inside the plugin. The workflow's retry policy is
  the one retry mechanism, and only definite no-write classifications reach it.

Slack documents approximately one message per second per channel plus a
workspace-wide limit. The operator egress policy may add a per-process rate
bucket; a refusal before the initial request is sent propagates its delay, while
a refusal after a redirect is an unknown outcome because an earlier hop already
received the write. Slack's authoritative 429 remains the fleet-wide limit and
is propagated durably.

## Security boundary

`token` is both `secret_inputs` and `required_secret_inputs`. The host must
receive `${secret('provider:name')}`, resolve it under the run namespace, and
scrub the resolved value from plugin errors and outputs. A literal is refused
before plugin invocation.

Credential release is not destination authorization. The host forwards the
exact bounded bytes it already parsed from `--egress-policy` as an immutable
launch-time snapshot. The plugin builds one `netpolicy.Policy` from those bytes
and its actual HTTP client performs every DNS, address, port, redirect, TLS, and
response-byte check. Missing or malformed policy fails closed. The plugin
manifest is only a declaration, not authority; process separation prevents a
crash from taking down the worker but is not filesystem or network confinement.

Finally, `slack.post` positively requires the host-attested production execution
mode. Rehearsal and unknown/older modes are refused before input decoding or
network access, so `flow run local` cannot send a real message while pretending
to be a safe preview. That mode check is an additional side-effect posture, not
authorization: task policy, secret release, and egress policy must still permit
the task, credential, and destination. `TestSlackPostOnlyAcceptsAnEstablishedProductionMode`
proves production is the only accepted value and that rehearsal, unspecified,
unknown future values, and a missing caller all fail closed without credentials.

## Build and use

From the repository root:

```console
go -C plugins/slack build -o ../../bin/flowstate-plugin-slack .
flow plugins --plugin-dir ./bin
flow validate --plugin-dir ./bin examples/plugins/slack/approval.yaml
flow worker --plugin-dir ./bin --plugin slack \
  --egress-policy examples/plugins/slack/egress-policy.yaml \
  --task-policy /path/to/task-policy.yaml
```

The operator must separately configure the `env:` secret provider to admit
`SLACK_BOT_TOKEN` and task policy to admit `slack.post`. See
[`examples/plugins/slack`](../../examples/plugins/slack) for the approval flow.
