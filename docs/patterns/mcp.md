# Agents over MCP

`ubunye mcp` serves the engine to agents over the
[Model Context Protocol](https://modelcontextprotocol.io): Claude, IDE assistants and
any MCP client can list tasks, plan them, run them and check the result, with the
same rules a person works under.

```bash
pip install "ubunye-engine[mcp]"
ubunye mcp -d pipelines                 # read-only tools
ubunye mcp -d pipelines --allow-run     # also the `run` tool
```

## Tools

| Tool | What it does |
| --- | --- |
| `tasks` | The tasks under the folder, as `usecase/package/task`. |
| `doctor` | What will fail on this machine, and why; with tasks, their configs and env vars too. |
| `plan` | What a task will read and write, the model bill, and what will stop it. Reads no data. |
| `runs` | A task's recorded runs, newest first. |
| `record` | A full run record: outputs and hashes, code, environment, timings, model calls. |
| `gate` | A run against a baseline (default: latest against previous), rule by rule. |
| `focus` | A run's model calls as FOCUS 1.4 cost rows. |
| `run` | Runs a task and records it. Only with `--allow-run`. |

## Safe by default

- Every tool but `run` only reads, and says so to the client (`readOnlyHint`).
- Tasks are named, never given as paths, and must sit under the folder given with
  `-d`. A name like `../../etc` is refused.
- `run` exists only when the server starts with `--allow-run`.
- An agent's run replays its model calls from the task's committed answers: no key,
  no spend. Only `--allow-live-llm` lets it call models live, and then the run's
  limits (`UBUNYE_LLM_MAX_USD` and the rest) apply as always.
- What a task prints goes to stderr, so it cannot break the protocol on stdout.
- A run that fails comes back as `{"ok": false, "error": ...}` with its run record,
  not as a crash.

## Claude Code

```bash
claude mcp add ubunye -- ubunye mcp -d /path/to/pipelines --allow-run
```

Then ask for what you want done: "plan shop/orders/clean, run it, and gate it
against the previous run".
