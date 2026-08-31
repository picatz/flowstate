# Slack approval notification

[`approval.yaml`](approval.yaml) completes the outbound half of a practical
human-in-the-loop flow: `slack.post` tells a human an approval is waiting, the
workflow waits durably for Flowstate's separately authenticated signal, and a
second post records the outcome in the first message's thread.

Slack is notification, not authority. This plugin has no inbound listener and
does not treat a button click or message as an approval; provider verification
and bridging into signals remain the control-plane work tracked by #96.

The example requires two deployment-owned controls that the Flowfile cannot
grant itself:

- `SLACK_BOT_TOKEN` must be admitted by the configured `env:` secret backend;
  `token:` is a whole secret reference and literals are rejected.
- [`egress-policy.yaml`](egress-policy.yaml) must be supplied through
  `--egress-policy`. It authorizes only Slack's HTTPS API endpoint. A plugin
  declaration is not destination authority.

`flow run local` refuses `slack.post`: rehearsal must never send a real
notification. Validate the example without executing it by building the plugin
and using `flow validate --plugin-dir` (or a saved `flow plugins --output json`
catalog). `approval.test.yaml` exercises the authorized signal and both outbound
steps deterministically with task stubs and an inert fixture credential.
