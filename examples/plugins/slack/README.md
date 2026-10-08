# Slack messages

Two tested examples of the [`slack` plugin](../../../plugins/slack/README.md):

- [`approval.yaml`](approval.yaml) completes the outbound half of a practical
  human-in-the-loop flow. `slack.post` sends an **approval card** (summary,
  fields, Approve and Reject buttons, a confirmation on Reject), the workflow
  waits durably for Flowstate's separately authenticated signal, and
  `slack.update` replaces the card with its outcome in place. The buttons
  already carry the decision (`action_id`) and the run to wake (`value`) a
  receiver will read.
- [`status.yaml`](status.yaml) posts one **status card** when a rollout starts
  and edits the same message as each stage finishes, with a progress bar, a
  state, and a link. `slack.update` names its target, so a retry policy may
  repeat it safely.

Slack is notification, not authority. This plugin has no inbound listener and
does not treat a button click or message as an approval; provider verification
and bridging into signals remain the control-plane work tracked by #96.

The examples require two deployment-owned controls that the Flowfile cannot
grant itself:

- `SLACK_BOT_TOKEN` must be admitted by the configured `env:` secret backend
  (`--secret-env SLACK_BOT_TOKEN` on the worker, which reads
  `FLOWSTATE_SECRET_SLACK_BOT_TOKEN`), and the worker then needs an
  `--auth-policy` whose `secrets:` section allows it: a worker holding a
  secret provider with no access policy refuses to start. `token:` is a
  whole secret reference and literals are rejected.
- [`egress-policy.yaml`](egress-policy.yaml) must be supplied through
  `--egress-policy`. It authorizes only Slack's HTTPS API endpoint. A plugin
  declaration is not destination authority.

`flow run local` refuses `slack.post` and `slack.update`: rehearsal must never
send a real notification. Validate the examples without executing them by
building the plugin and using `flow validate --plugin-dir` (or a saved
`flow plugins --output json` catalog). `approval.test.yaml` and
`status.test.yaml` exercise every step deterministically with task stubs and an
inert fixture credential.
