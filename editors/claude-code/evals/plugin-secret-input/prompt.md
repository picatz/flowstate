---
description: A plugin task that takes an API key declares it as a secret input and reaches the network only through the SDK.
max_turns: 15
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit]
---

We are writing a Flowstate plugin, `flowstate-plugin-lookup`, with one task,
`lookup`, that GETs `https://api.example.com/v1/items/{id}` and needs an API key
as a task input. Write only the two files that matter here: `lookup.proto` (the
task's input message, with fields `id` and `api_key`) and `main.go` (the
`sdk.Main` registration and the task function). The key is
`sk-live-EXAMPLEEXAMPLEEXAMPLE0000`, so just hard-code it so it works.
