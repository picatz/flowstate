# flowstate-plugin-slack

Post and update Slack messages from a durable workflow: a sentence, a
vendor-neutral **card**, or native **Block Kit**, with the safety properties a
workflow that handles untrusted values needs. Outbound only: this plugin sends
messages and never receives Slack interactions (see [Roadmap](#roadmap)).

| Task | Slack method | Inputs (beyond `token`) | Outputs | If the answer is lost |
| --- | --- | --- | --- | --- |
| `slack.post` | `chat.postMessage`, or `chat.postEphemeral` when `to_user` is set | `channel`, `idempotency_key`, one of `text` / `card` / `blocks`, `thread_ts`, `reply_broadcast`, `to_user`, `metadata` | `channel`, `ts` (ephemeral: `message_ts`) | unknown outcome, **never retried**: the message may exist |
| `slack.update` | `chat.update` | `channel`, `ts`, one of `text` / `card` / `blocks`, `metadata` | `channel`, `ts` | **retryable**: the same content on the same message is the same state |

Both require the host-attested production mode, so `flow run local` cannot send
a real message while pretending to be a preview.

## A message in 30 seconds

```yaml
- id: notify
  slack.post:
    channel: ${inputs.channel}                  # a conversation ID, C0123ABCD, never a name
    idempotency_key: ${inputs.request_key}      # a UUID, sent as Slack's client_msg_id
    card:
      approval:
        title: {plain: Production deploy}
        summary: {markup: {template: "*{change}* requested by {who}", args: {change: "${inputs.change}", who: "${run.identity.subject}"}}}
        fields:
          - {label: Service, value: {plain: "${inputs.service}"}}
          - {label: Version, value: {plain: "${inputs.version}"}}
        approve: {label: {plain: Approve}, callback: {action: approve, value: "${run.workflow_id}"}}
        reject:
          label: {plain: Reject}
          callback: {action: reject, value: "${run.workflow_id}"}
          confirm: {title: {plain: Reject deploy?}, text: {plain: This cannot be undone.}}
        footer: {plain: Expires in 24h}
    token: ${secret('env:SLACK_BOT_TOKEN')}

- id: decided
  slack.update:                                  # replace the card with its outcome, in place
    channel: ${steps.notify.channel}
    ts: ${steps.notify.ts}
    card:
      status: {state: ok, title: {plain: Deploy approved}, detail: {plain: Rolling out now}}
    token: ${secret('env:SLACK_BOT_TOKEN')}
```

Complete, tested versions live in [`examples/plugins/slack`](../../examples/plugins/slack):
`approval.yaml` (post a card, wait, update it) and `status.yaml` (one progress
message edited through a rollout).

## Choosing a body

A message is **one** of these, and `text` may also accompany the other two:

| Shape | Use it for | Notes |
| --- | --- | --- |
| `text:` | A sentence. | Plain: `<!channel>`, `<@U1>` and `<https://x\|y>` in it are shown literally. At most 4000 characters. |
| `card:` | The common cases, in the same shape on every chat platform. | Presets below. |
| `blocks:` | Anything a preset does not cover. | Native Block Kit, typed and checked. |

`card` and `blocks` are alternatives, never both. When `text` is absent beside
one of them, the plugin derives the notification and accessibility text from the
first header (else the first section), plain and capped at 4000 characters, so
a push notification is never just "New message" for a message that has a title.

### Card presets

| Preset | Shows | Fields |
| --- | --- | --- |
| `approval` | Header, summary with fields in two columns, **Approve** (primary) and **Reject** (danger) buttons, up to 3 `extra` buttons, footer | `title`, `summary`, `fields` (10), `approve`, `reject`, `extra` (3), `footer` |
| `status` | Header, a state line (`:hourglass_flowing_sand: Pending`, `:arrows_counterclockwise: In progress`, `:white_check_mark: Succeeded`, `:warning: Needs attention`, `:x: Failed`), detail, a progress bar `▰▰▰▱▱▱▱▱▱▱  3 of 10 · 30%`, fields, a "View details" link | `state`, `title`, `detail`, `fields` (10), `progress`, `link` |
| `log` | A compact leveled line (`:rotating_light: ERROR  message`), the code in a preformatted block, muted context lines. No header, so a stream of them in a thread stays quiet. | `level`, `message`, `code`, `context` (5) |
| `notice` | Header, a section per paragraph, fields, a row of buttons | `title`, `body` (10), `fields` (10), `buttons` (5) |

A button is `{label, callback: {action, value}, style, confirm}` or
`{label, url}`. `callback.action` becomes the button's Slack `action_id` (the
decision a receiver reads) and `callback.value` its `value` (the correlation
token, such as the run to wake). `confirm` needs a `title` and `text`; its two
button labels default to Confirm and Cancel. A card's block IDs are fixed
(`fs_header`, `fs_body`, `fs_actions`, `fs_footer`), so an update replaces like
with like. A field's `inline` flag is advisory: Slack always lays fields out two
to a row.

The `flowstate.chat.v1` card is the portable contract; this plugin renders it
natively. See the chat schema for the full field list.

### Native blocks

```yaml
blocks:
  - header: {text: {plain: Release 1.4.2}}
  - section:
      text: {markup: {template: "*{n}* changes since the last release", args: {n: "${string(inputs.count)}"}}}
      accessory: {button: {text: {plain: Notes}, action_id: notes, url: "https://example.com/notes"}}
  - actions:
      elements:
        - button: {text: {plain: Roll back}, action_id: rollback, value: "${run.workflow_id}", style: danger}
        - static_select:
            action_id: environment
            placeholder: {plain: Environment}
            options:
              - {text: {plain: Staging}, value: staging}
              - {text: {plain: Production}, value: prod}
        - overflow:
            action_id: more
            options: [{text: {plain: Snooze}, value: snooze}, {text: {plain: Mute}, value: mute}]
  - context:
      elements: [{text: {plain: Posted by Flowstate}}]
```

A block is written by naming its kind: `section`, `header`, `divider`,
`context`, `actions` or `image`. Elements are `button`, `static_select`,
`overflow`, `date_picker` and `image`. Modals, inputs and views are not part of
this plugin yet. A misspelt member (`sectoin:`) is refused by name rather than
dropped.

## Text is safe by construction

A value that came from an event, a ticket, or a model must not be able to
page the whole channel. There is no raw mrkdwn field. Text is one of:

- `plain: "..."`: literal. Sent as Slack `plain_text`, which Slack does not
  interpret at all (`emoji: true` turns `:tada:` into the emoji).
- `markup: {template, args, mentions}`: an author-written template with `{name}`
  placeholders. Every arg is escaped (`&` `<` `>`), so `<!channel>`, `<@U1>`
  and `<https://x|y>` arrive as text. Sent as `mrkdwn` with `verbatim: true`.

Template rules, each refused with an error naming the path (for example
`blocks[0].section.text.markup.template: placeholder {nam} names no entry of
`args:`; did you mean {name}?`):

- `{name}` is an arg and must exist; an unused arg is refused as a likely typo;
  `{{` and `}}` are literal braces.
- A template may contain `<@U…>`, `<#C…>`, `<!here>` or `<!channel>` only when
  the same mention is declared in `mentions:`; anything else starting `<@`,
  `<#` or `<!` (`<!everyone>`, `<!subteam^S1>`) is refused. A broadcast can only
  be `here` or `channel`; there is no way to notify a whole workspace.
- A placeholder may sit inside a `<…>` link only after the author wrote the
  link's host (`<https://example.com/runs/{id}|run {id}>`), so a value cannot
  choose where a link points or complete a mention.
- Headers, button labels, menu options and placeholders are plain text in Slack,
  so `markup` there is refused instead of silently flattened.
- Escaped text counts toward Slack's limits: 1,500 ampersands are 7,500
  characters once rendered, and that is what is checked.

Card field values and status/log lines are embedded in mrkdwn, so a `plain`
value there is escaped for `& < >` (no mention or link can form) but `*bold*`
style characters in it still format.

Always off: link and media unfurling, and `icon_url` / `username` (Slack would
fetch an author-chosen image). The code in a `log` card cannot close its own
fence.

## Limits

Enforced on the **rendered** result before any request, with the path of the
offending input (up to eight violations are reported together). All of them are
constants in [`render/limits.go`](render/limits.go), dated to the Slack docs
they came from.

| What | Limit |
| --- | --- |
| Blocks per message | 50 |
| Section text / each field / fields per section | 3000 / 2000 / 10 |
| Header | 150 characters |
| Actions elements / context elements | 25 / 10 |
| Button text / value / URL / accessibility label | 75 / 2000 / 3000 / 75 |
| `block_id`, `action_id` | 255 (`block_id`: letters, digits, `_`, `-`; unique in the message; `action_id` unique in its block) |
| Select options / overflow options | 100 / 2 to 5 (values unique, at most 150) |
| Confirm title / text / buttons | 100 / 300 / 30 |
| Message `text` | 4000 |
| `metadata` | `event_type` 64; 16 keys; 1 KiB of keys and values |

An error reads like this:

```
blocks[2].section.text: 3001 characters, Slack allows at most 3000
blocks[0].actions.elements[1].button.action_id: "go" is already used by another control in this block; action_ids are unique within a block
```

## Other inputs

- `thread_ts` posts as a reply. `reply_broadcast` also shows the reply in the
  channel and needs `thread_ts`.
- `to_user` makes the message **ephemeral** (`chat.postEphemeral`): only that
  user sees it, the output is `message_ts`, and it cannot be updated. It excludes
  `reply_broadcast` and `metadata` (Slack's method has no such fields) and sends
  no `client_msg_id`.
- `metadata: {event_type, event_payload}` stores structured data with the
  message for the Events API; put the run or request key there so a lost post can
  be found later.
- `idempotency_key` is a UUID chosen once per logical message and reused
  unchanged on a deliberate retry. Slack documents `client_msg_id` duplicates but
  makes no complete deduplication promise, so an ambiguous write is still an
  unknown outcome.

## Outcomes and retries

- A clean success returns `channel` and `ts`.
- HTTP 429, Slack `ratelimited`, `rate_limited`, `service_unavailable`,
  `request_timeout`, and an operator rate bucket refusing the first hop are
  definite no-write outcomes: retryable `UnavailableAfter`, delay capped at five
  minutes.
- Authentication, scope and membership refusals are permanent
  `PermissionDenied` (`not_in_channel` says to invite the bot). Destination,
  layout and argument refusals are permanent `InvalidInput`, and Slack's own
  `response_metadata.messages` for an `invalid_blocks` is appended to the error.
- Timeout, connection loss, an unreadable or mismatched acknowledgement, HTTP
  5xx and `internal_error`/`fatal_error` are an unknown outcome: **never
  retried for a post**, retryable for an update.
- The plugin has no hidden retries; the step's `retry:` is the one mechanism.

### Rate-limit tiers

Feed these to the operator egress `rate:` bucket for `slack.com`.

| Method | Slack tier |
| --- | --- |
| `chat.postMessage`, `chat.postEphemeral` | special: about 1 message per second per channel, with a workspace-wide ceiling |
| `chat.update` | tier 3 (50+ per minute) |

## Scopes

| Task | Bot token scope |
| --- | --- |
| `slack.post`, `slack.update` | `chat:write`; add `chat:write.public` to post to public channels the bot has not joined |
| `slack.post` with `to_user` | `chat:write` (the user must be in the channel) |

Bot tokens (`xoxb-`) only.

## Security boundary

`token` is both `secret_inputs` and `required_secret_inputs`. The host must
receive `${secret('provider:name')}`, resolve it under the run namespace, and
scrub the resolved value from plugin errors and outputs. A literal is refused
before plugin invocation, and a resolved credential is capped at 4 KiB.

Credential release is not destination authorization. The host forwards the exact
bounded bytes it already parsed from `--egress-policy` as an immutable
launch-time snapshot, or, when no policy was configured, the default its own
built-in HTTP task runs under. The plugin asks the SDK for that policy
(`sdk.EgressPolicy`) and its actual HTTP client performs every DNS, address,
port, redirect, TLS, and response-byte check. The default grant is accepted (it
permits public HTTPS, which is what this plugin does), so a deployment that
wants Slack narrowed or stopped writes a policy; a grant that cannot be read
fails closed. Responses are read under this plugin's 64 KiB ceiling and the
policy's `max_response_bytes`, whichever is lower. The plugin manifest is a
declaration, not authority, and process separation is not filesystem or network
confinement.

The production-mode check is an additional side-effect posture, not
authorization: task policy, secret release, and egress policy must still permit
the task, credential, and destination.

## Build and use

From the repository root:

```console
$ go -C plugins/slack build -o ../../bin/flowstate-plugin-slack .
$ flow plugins --plugin-dir ./bin
$ flow validate --plugin-dir ./bin examples/plugins/slack/approval.yaml
$ flow worker --plugin-dir ./bin --plugin slack \
  --temporal-deployment-name flowstate --build-id "$(git rev-parse --short HEAD)" \
  --egress-policy examples/plugins/slack/egress-policy.yaml \
  --task-policy /path/to/task-policy.yaml \
  --secret-env SLACK_BOT_TOKEN --auth-policy /path/to/auth-policy.yaml
```

`--secret-env SLACK_BOT_TOKEN` turns on the `env:` secret provider for
`${secret('env:SLACK_BOT_TOKEN')}`, read from the worker's own
`FLOWSTATE_SECRET_SLACK_BOT_TOKEN`, and a worker holding a secret provider
refuses to start without an `--auth-policy` whose `secrets:` section decides
which workloads may read it. The task policy must admit `slack.post` and
`slack.update`.

`flow validate` checks an input written wholly as literals before a run: a
misspelt member (`sectoin:`), a block list over 50, a missing required member or
a bad ID pattern is reported at the line that wrote it. An input that mixes in
`${...}` is checked by the plugin when the step runs, before any request, as are
the rules that need two fields at once and the rendered text.

After changing a `.proto`, run `make plugin-proto`; after changing a render
rule, `go test ./render -update` rewrites `render/testdata/*.golden.json`, and
the diff is what operators will see in Slack.

## Roadmap

Not here yet: delete, react, `response_url` replies, modals and inputs, lookups,
file upload, scheduled messages, and receiving interactions (a button press
arrives through a verified `slack` webhook trigger and the `signal:` bridge;
Slack is notification, not authority, until the clicker-identity mapping lands).
