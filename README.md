# omosense

omosense watches your messengers and tools on behalf of one agent session. Run it inside a project folder and it becomes one foreground process that hosts every source that folder enables: Telegram and Discord listeners, reminders, Google calendar and mail, herdr panes, webchat rpc sessions and memory tidy. Everything it notices comes out on stdout as one line per event, so a session needs exactly one monitor on it.

One session, one omosense. There's no shared daemon, and two folders never share config or state.

## Quick start

Install it, or run it through npx/bunx:

```sh
npm i -g omosense
omosense --help

npx omosense --help
bunx omosense --help
```

Then give the project folder a config and start it:

```sh
cd ~/work/my-agent
mkdir -p .omosense
$EDITOR .omosense/config.json
omosense
```

Bare `omosense` (no subcommand) is the session host. It runs until it gets SIGINT, SIGTERM or SIGHUP, then stops every source, releases its locks and exits 0. Without `.omosense/config.json` it exits 1 with `omosense: read config: ...` naming the path it tried.

To stop it from another shell, run `omosense stop` (or `npx omosense stop`, `bunx omosense stop`) in the same folder. See [Stopping the host](#stopping-the-host).

## Which sources run

| Source | Starts when | Prints |
|---|---|---|
| listen (Telegram) | `telegram.bot` is set | `EVENT` |
| listen (Discord) | `discord.bot` is set | `EVENT` |
| remind | always | `REMIND` |
| google | always (needs the `zele` CLI on `PATH`) | `CAL`, `SOON`, `MAIL` |
| herdr | always, unless `herdr.enabled` is `false` | `HERDR` |
| rpc | `rpc.enabled` is `true` | `RPC` |
| tidy | `tidy.enabled` is `true` | `TIDY` |

Every source can also print `LOG` lines. On start the host prints one line, `LOG omosense host starting dir=<dir> sources=<names>`, so you can see what it picked up.

Before any source starts, the host checks `memory`. When it's set and `<agents>/<memory>/repo` isn't a directory (agents is `$OMO_MEMORY_AGENTS`, else `~/.omo/memory/agents`), the host exits 1 with `omosense: config.json: memory "<id>": repo not found at <path>` and takes no lock. With `memory` unset and `tidy.enabled` on, it prints `LOG memory is not set; tidy has no own memory repo to skip` once and starts normally. Subcommands skip this check.

herdr is on by default because for a setup without rpc it's the main signal. Turn it off with `"herdr": {"enabled": false}`.

A source that crashes logs `LOG omosense source <name> crashed: <err>` and is restarted after a backoff that starts at 5 seconds and doubles up to 5 minutes. It doesn't restart forever. On the sixth consecutive crash the host stops that source for good with one `LOG source-stopped {"source":<name>,"crashes":6,"last_error":<err>}` line and releases its lock. The other sources keep running. A source that ran 5 minutes or more before crashing starts the count over at one. Lock errors and `ALREADY_RUNNING` never count as crashes. If another live process already holds that source's lock (for example a second `omosense` in the same folder), the source logs `ALREADY_RUNNING ...` and retries every 30 seconds.

## Config

The config is flat. Every key is optional.

```json
{
  "telegram": { "bot": "my_tg_bot", "roles": { "123456789": "owner", "987654321": "wife" } },
  "discord":  { "bot": "my_dc_bot", "roles": { "111122223333444455": "owner" } },
  "rpc":      { "enabled": true, "all": false },
  "tidy":     { "enabled": true, "learnOthers": true, "exclude": ["other-agent-1234abcd"] },
  "herdr":    { "enabled": true },
  "memory":   "my-agent-5678efgh",
  "calendars": ["me@example.com"],
  "mail":     true
}
```

Newer optional keys, shown with example values:

```json
{
  "verify":        { "command": ["/path/to/check-done.sh"], "timeoutSec": 60 },
  "rpc":           { "enabled": true, "verify": true, "labels": { "task": "task", "ack": "ack" } },
  "herdr":         { "verify": false, "blockedAll": false, "agentPattern": "senpi|omo|claude|codex|opencode|(^|/)pi( |$)" },
  "silentMinutes": 30,
  "transcriber":   ["whisper-cli", "{audio}"],
  "guard":         { "stateFileBytes": 16777216 }
}
```

| Key | Meaning |
|---|---|
| `telegram.bot`, `discord.bot` | One bot name per platform, looked up in `~/.config/agent-messenger/<platform>bot-credentials.json` (`telegrambot-credentials.json`, `discordbot-credentials.json`). Unset means that listener doesn't run. |
| `telegram.roles`, `discord.roles` | User id to role (`owner`, `wife`, `trusted`, ...), attached to each `EVENT`. |
| `rpc.enabled`, `rpc.all` | Watch webchat sessions; `all` watches every session, not only those registered in `threads.json`. |
| `tidy.enabled`, `tidy.learnOthers`, `tidy.exclude` | Memory tidy watcher and which agents' memory repos it skips. |
| `tidy.checkMin` | How often tidy checks the memory repos, in minutes. Default `10`. |
| `tidy.quietMin` | How long a changed repo's HEAD commit must be quiet before it's reported, in minutes. Default `60`. See [Memory tidy](#memory-tidy). |
| `herdr.enabled` | Absent means on. Only an explicit `false` turns herdr off. |
| `memory` | This agent's memory id. |
| `calendars` | Calendars google watches. Absent means all of them. |
| `mail` | Whether google also watches mail. |
| `verify.command` | Done-verification hook, an argv array. Unset means no hook. See [Done verification](#done-verification). |
| `verify.timeoutSec` | Hook timeout in seconds. Default `60`. |
| `rpc.verify` | Run the hook for rpc dones. Absent means on (when `verify.command` is set); only `false` turns it off. |
| `rpc.labels` | Overrides for the rpc batch labels. See [rpc done notifications](#rpc-done-notifications). |
| `herdr.verify` | Run the hook for each herdr working to idle/done that goes into a done batch. Default `false`. |
| `herdr.blockedAll` | Print a `blocked` line for every pane (except omosense's own pane), not just registered job panes. Default `false`. A non-boolean fails config load. |
| `herdr.agentPattern` | Go regular expression a pane's foreground command line must match to count as alive. Default `senpi\|omo\|claude\|codex\|opencode\|(^\|/)pi( \|$)`. An invalid pattern fails config load. |
| `silentMinutes` | Minutes a working session may go without change before `silent-session`. Default `30`. |
| `transcriber` | Voice transcriber argv. Every `{audio}` is replaced by the audio file path, or the path is appended when no element has `{audio}`. Its trimmed stdout is the transcript. Unset means the built-in ffmpeg and mlx_whisper pipeline. A failure shows up as `transcribe_error` text starting with `transcriber:`. |
| `guard.stateFileBytes` | Size above which a state file gets `state-file-large`. Default `16777216` (16 MiB). |

One bot belongs to one session. If two sessions need Telegram, give each its own bot.

A key with the wrong type fails loudly and names the key, for example `config.json: telegram.bot must be a string`. The old shapes fail the same way (exit 1): a `profiles` object, the legacy `owner`/`wife` top-level keys, a top-level `bots`, or `telegram.bots`/`discord.bots`. If you're coming from the profiles config, see [docs/single-session-migration.md](docs/single-session-migration.md).

`--profile` is gone. Passing it anywhere exits 2 with `omosense: --profile was removed; config.json is per folder now ...`.

## Where things live

By default the config dir is `<cwd>/.omosense` and the state dir is `<cwd>/.omosense/state`. Two environment variables override them:

- `OMOSENSE_DIR` replaces the config dir (`$OMOSENSE_DIR/config.json`).
- `OMOSENSE_STATE` replaces the state dir. Unset, it's `<config dir>/state`.

The state dir holds:

| File | What it is |
|---|---|
| `reminders.json` | Reminder queue. Agents add entries here; remind sends them. |
| `google-seen.json` | Calendar and mail items already reported. |
| `rpc-pending.json` (+ `rpc-pending.lock`) | rpc completions not yet acked. |
| `rpc-subscription.json` | The session that receives rpc done batches. |
| `herdr-pending.json` | herdr job pane completions waiting for their done batch. A malformed file is renamed to `herdr-pending.json.bad-<unix>` and herdr starts empty. |
| `threads.json` | Job thread registry read by herdr and rpc. Written by `omosense thread`. |
| `threads.lock` | Lock guarding `threads.json` writes. |
| `sessions.json` | Left over from older versions. omosense no longer reads it. |
| `memory-tidy.json` | Tidy watermark. |
| `tidy-announced.json` | Which HEAD tidy last reported for each repo, and when. Keeps the 6 hour re-report rule across restarts. |
| `tg-offset-<bot>` | Telegram fetch offset for that bot. |
| `inbox/` | Downloaded Telegram attachments. |
| `*.lock.json` | Source locks: `listen`, `remind`, `watch-google`, `watch-herdr`, `watch-rpc`, `memory-tidy`. |
| `rpc-subscription.lock` | Lock guarding `rpc-subscription.json` updates. |

A few things stay global on purpose: bot credentials in `~/.config/agent-messenger`, tidy backups in `~/.omo/memory-backups`, and memory repos in `~/.omo/memory/agents` (or `$OMO_MEMORY_AGENTS`).

Messages that arrive while the session (and its omosense) is off aren't received. That's by design: there's no queue or retry for an offline session.

## Watching it from an agent session

Arm one monitor on `omosense` in the project folder. The command is `exec omosense` so a stop signal reaches the binary. Most `LOG` lines are housekeeping, so keep them out of the monitor's event budget, but let the four alert lines through:

```
command: cd ~/work/my-agent && exec omosense
filter:  ^(EVENT|CAL|SOON|MAIL|REMIND|HERDR|RPC|TIDY) |^LOG (silent-session|dead-pane|source-stopped|state-file-large) 
```

The alert lines:

| Line | When | JSON fields |
|---|---|---|
| `LOG dead-pane {json}` | A registered pane is gone from `herdr pane list`, or no foreground process matches `herdr.agentPattern`. Checked once a minute, printed once until the pane is alive again. | `machine`, `pane`, `thread`, `reason` (`pane gone` or `no agent process`), `foreground`, `pattern` |
| `LOG silent-session {json}` | A registered session stays `working` with no status change and no output growth for `silentMinutes`. Printed once until something changes. | rpc: `source`, `session`, `id`, `thread`, `since`, `minutes`, `messageCount`. herdr: `source`, `machine`, `pane`, `thread`, `since`, `minutes`, `signal` |
| `LOG source-stopped {json}` | A source hit six consecutive crashes and was stopped. | `source`, `crashes`, `last_error` |
| `LOG state-file-large {json}` | A file in the state dir is over `guard.stateFileBytes`. Checked at start and every 10 minutes, printed once until it shrinks. | `path`, `bytes`, `limit` |

Silent isn't dead. A long tool call that adds no new messages can trip `silent-session` too. A herdr command that fails is never reported as `dead-pane`; it shows up as `LOG herdr <machine> <err>` instead.

## Threads

`threads.json` tells herdr and rpc which panes and sessions are jobs to watch. Write it with `omosense thread` instead of by hand:

```sh
omosense thread register <thread-id> [--session <id>] [--pane <pane-id>] [--machine <name>] [--cwd <dir>] [--name <text>] [--platform <name>]
omosense thread close <thread-id>
```

`register` upserts the entry. It sets only the fields you pass, sets `status` to `active`, removes `closed`, and sets `started` only when it's missing. Every other key, and the key order of the file and the entry, stays as it was. It needs `--session` or `--pane`. Flags take `--flag value` or `--flag=value`.

| Flag | Field |
|---|---|
| `--session` | `session_id` |
| `--pane` | `pane` |
| `--machine` | `machine` |
| `--cwd` | `cwd` |
| `--name` | `name` |
| `--platform` | `platform` |

`close` sets `status` to `done` and `closed` to the current time.

Both print one `THREAD {"id":<thread-id>,...}` line and exit 0. Exit codes:

- 0: written.
- 1: `close` on an unknown id (`omosense: thread close: unknown thread "<id>"`), or a `threads.json` that doesn't parse or is an array (`omosense: thread <verb>: threads.json is an array; thread <verb> needs the object form`). The file is left byte-identical.
- 2: usage error (no `--session`/`--pane`, a missing value, an unknown or repeated flag). The help goes to stderr and nothing is written.

Writes hold `threads.lock` and replace the file atomically, so concurrent registers don't lose entries.

A thread whose `status` is `done` or `closed`, or that has a non-empty `closed`, isn't watched. It stays unwatched until `thread register` makes it active again. Closing a thread in the middle of a turn drops that turn's completion.

When an active entry has both a session and a pane, and rpc is enabled and its socket lists a matching session, rpc owns it and herdr stays quiet for that pane (no done batch entries, no `blocked` line unless `herdr.blockedAll` is on, no `silent-session`, no `dead-pane`). If the socket is down or doesn't list the session, herdr reports the pane. With `rpc.enabled` set but the rpc source not running in this host (`ALREADY_RUNNING`, or stopped after crashes), herdr still defers to the socket's session list.

### herdr lines

A `blocked` line prints right away, on the first time herdr sees the pane blocked too. By default it only prints for registered job panes: an active `threads.json` entry that rpc doesn't own. Set `herdr.blockedAll: true` to get `blocked` for every pane again, except omosense's own.

A job pane going from `working` to `idle` or `done` doesn't print a line of its own. It's recorded in `herdr-pending.json`, and once 5 minutes pass with no newer completion, all recorded panes come out as one line:

```
HERDR {"event":"done-batch","entries":[{"machine":"local","pane":"p1","tab":"t1","agent":"senpi","cwd":"/work/a","from":"working","to":"idle","at":"2026-10-09T10:05:00.000Z","first_at":"2026-10-09T10:01:00.000Z","count":2}]}
```

The 5 quiet minutes count from the newest completion, the same rule rpc uses, so a busy stretch keeps the batch waiting. Each pane gets one entry. When it finished more than once, the entry keeps the last transition's `tab`, `agent`, `cwd`, `from`, `to` and `at`, `count` says how many times it finished, and `first_at` is the first one. Entries are sorted by `first_at`. Times are RFC 3339 UTC. With `herdr.verify` on, each entry also has `verify` and, when set, `verify_detail`.

The file is written on every completion, so a restart or crash keeps the waiting entries. The new host picks them up and prints the batch on the same rule, 5 minutes after the newest one. Shutdown never prints the batch early. `omosense herdr --once` doesn't read or touch the file.

Because `blocked` is immediate and completions wait, a pane's `blocked` can show up before the batch that holds its earlier `idle`.

## Sending messages

`say` sends through the folder's configured bot and prints the API response:

```sh
omosense say telegram send '{"chat_id":123456789,"text":"hello"}'
omosense say telegram send '{"bot":"other_bot","chat_id":123456789,"text":"hello"}'
```

Telegram actions: `send`, `edit`, `draft`, `typing`, `react`, `unreact`, `topic`, `topic-edit`, `photo`, `doc`. Discord actions: `send`, `edit`, `typing`, `react`, `unreact`, `thread`, `thread-edit`, `file`.

Discord `send`, `edit`, and `file` accept `content` as an alias of `text`. `send` and `edit` use `text` whenever the `text` key is present (even null); `file` uses `text` unless it is null, then `content`. `omosense say --help` lists each action with its JSON fields, and an unknown action's error lists the valid ones.

`{"bot":"name"}` overrides the bot and is stripped before the request. With no `<platform>.bot` and no override, say exits 2.

## rpc done notifications

With `rpc.enabled`, omosense watches webchat sessions. `blocked`, `opened` and `closed` still come out as `RPC` lines. A finished session (`done`) doesn't: it's recorded in `rpc-pending.json` and pushed in batches to one subscribed session.

```sh
omosense rpc subscribe <session-id>   # send done batches to this session
omosense rpc subscription             # show the current subscriber
omosense rpc unsubscribe              # stop sending
omosense rpc pending                  # list un-acked completions, oldest seq first
omosense rpc ack <id> [<seq>]         # clear one
```

`subscribe` checks the id against `omo thread list` before it saves anything. An id that isn't listed is rejected: it exits 1, writes nothing, and the error hints that you should pass the durable `thread_id`. If `omo` can't be queried at all, subscribe prints a warning and keeps the subscription.

How batching works:

- Each done restarts a 5-minute quiet timer. When 5 minutes pass with no new done, exactly one message goes out (`omo thread send`) listing every completion since the last batch. If a session finishes twice inside the window, only its newest seq is listed, with a count.
- Each entry in the message carries its own ack command with absolute paths, `OMOSENSE_DIR='<dir>' OMOSENSE_STATE='<state>' omosense rpc ack <id> <seq>`, so it works from anywhere. A long batch is cut at 32 KiB with a `+N more: ... omosense rpc pending` line.
- Nothing is re-sent. Entries stay in `rpc-pending.json` until acked.
- No subscriber: the batch is dropped (`LOG rpc batch dropped: no subscriber`).
- Subscriber not alive in `omo thread list`: the batch is dropped and the subscription is removed (`LOG rpc batch dropped: subscriber <id> not alive; unsubscribed`).
- A send that fails while the subscriber is alive is retried after a minute.

Every line in an entry is `<label>: <value>`. `rpc.labels` overrides any label; the defaults reproduce the original output. An unknown key or a non-string value fails config load naming `rpc.labels.<key>`. The ack command itself is never a label and is never cut.

| Key | Default |
|---|---|
| `task` | `작업` |
| `thread` | `thread` |
| `cwd` | `cwd` |
| `id` | `완료 id` |
| `seq` | `seq` |
| `doneAt` | `done_at` |
| `count` | `count` |
| `ack` | `확인 명령` |
| `unverified` | `미검증` |
| `verifyPending` | `검증이 끝나지 않음` |
| `more` | `more` (the word in the `+N more:` line) |

A session that comes back after being off should run `omosense rpc pending` to catch up, then `omosense rpc subscribe <its-id>` again if it was unsubscribed.

`ack <id> <seq>` leaves the entry pending and prints `newer` when a newer completion of that session has arrived since.

## Done verification

Idle means the turn ended, not that the work is done. Set `verify.command` to check it. The hook runs once per done, with one retry. Exit 0 means verified. Anything else (a non-zero exit, a timeout after `verify.timeoutSec`, a start error) means unverified, with a detail like `exit 1: <stderr, 200 runes>`. The done is never dropped.

The hook gets `OMOSENSE_DONE_SOURCE` (`rpc` or `herdr`), `OMOSENSE_DONE_THREAD` and `OMOSENSE_DONE_CWD`. rpc also sets `OMOSENSE_DONE_ID` and `OMOSENSE_DONE_SESSION`; herdr sets `OMOSENSE_DONE_PANE` and `OMOSENSE_DONE_MACHINE`. It runs in the done's cwd when that directory exists.

rpc (on by default once `verify.command` is set): the entry in `rpc-pending.json` gets `verify` (`pending`, `verified`, `unverified`) and `verify_detail`. The hook never delays the batch. An unverified entry gets one `<unverified>: <detail>` line in the batch; one still running gets `<unverified>: <verifyPending>`. A result that lands after its batch shows in `omosense rpc pending` but isn't re-sent. On shutdown a running hook is killed and the entry stays `pending`, and hooks don't re-run after a restart. Without a hook, entries and batches look exactly as before.

herdr (only with `herdr.verify: true`): the verdict lands in the pane's done batch entry. The entry is recorded with `"verify":"pending"` the moment the pane goes idle, then the hook's result replaces it with `verified`, or `unverified` plus `verify_detail`. While any hook is still running the batch waits, so the line carries finished verdicts. If the pane finishes again before its hook returns, the older result is dropped and only the newest transition's hook counts. On shutdown a running hook is killed and its entry gets `"verify":"unverified","verify_detail":"cancelled"`. An entry still `pending` after a crash stays `pending` in the batch: hooks don't re-run after a restart.

## Subcommands

Each source can also run alone, which is handy for checks:

```
omosense listen [--dry-run]
omosense google [--once]
omosense remind
omosense herdr [--once]
omosense rpc [--once] [--all]
omosense tidy [--once|--now] [--check-min m] [--quiet-min m] [flags]
omosense tidy --write-watermark [repo=sha ...]
omosense tidy --backup-now
omosense stop
omosense say <platform> <action> <json>
omosense thread register|close <thread-id> [flags]
```

`--once`, `--now` and `--dry-run` are read-only: they take no lock and don't create the state dir. Run `omosense <subcommand> --help` for details.

## Memory tidy

With `tidy.enabled`, the tidy source checks the memory repos every `checkMin` minutes (default 10). A repo that changed since its last tidy is reported with one `TIDY` line once its HEAD commit has been quiet for `quietMin` minutes (default 60). A repo that keeps getting commits isn't reported until it settles. The same HEAD isn't reported again within 6 hours. What was reported, and when, is kept in `tidy-announced.json`, so restarting the host within 6 hours doesn't report the same HEAD again.

One `TIDY` line carries at most 10 repos. When more are ready, the tick prints further `TIDY` lines until all of them are out. If the folder had no `memory-tidy.json` when tidy started, the first report also prints one plain LOG line, outside the monitor's alert filter:

```
LOG memory-tidy no watermark yet: 23 repos reported in 3 TIDY lines; run "omosense tidy --write-watermark" to mark the current HEADs as tidied
```

`omosense tidy --write-watermark` with no arguments marks every repo's current HEAD as tidied, which sets a baseline. `omosense tidy --write-watermark repo=sha ...` sets just those repos. `omosense tidy --backup-now` runs the daily backup once and exits, with exit 1 if a backup failed.

`tidy.checkMin` and `tidy.quietMin` take any number greater than 0, fractions included. A wrong type, `0` or a negative value exits 1 naming the key, for example `config.json: tidy.quietMin ...`. The `--check-min` and `--quiet-min` flags override the config, and the config overrides the defaults. The start line shows the values in effect:

```
LOG memory-tidy watcher starting (check 10m, quiet 60m)
```

## Stopping the host

```sh
omosense stop
```

`stop` reads this folder's `.omosense/state/*.lock.json` (or `$OMOSENSE_STATE`). It only signals a pid whose command name (the basename of argv[0]) starts with `omosense`. It sends SIGTERM, waits up to 10 seconds for the process to exit and its locks to go away, and prints one line. It doesn't need `config.json` and doesn't create the state dir.

Exit codes:

- 0: not running, stopped, or the lock is stale or held by a pid that isn't omosense. A stale or foreign lock is reported; nothing is signaled or deleted.
- 1: a verified omosense pid, or its lock file, still persists after the 10 s wait (the message names the pid and the remaining lock), or a lock file is unreadable or malformed, or argv could not be read.
- 2: usage error.

If a lock names a live pid that belongs to another program (a reused pid), omosense can't take that lock back on its own. Remove the lock file by hand, then start again.

## Supported platforms

The package ships four static (`CGO_ENABLED=0`) prebuilt binaries, and a small Node shim picks the right one at run time:

- darwin-arm64
- darwin-x64
- linux-arm64
- linux-x64

Windows isn't supported in this version. npm and npx refuse to install with `EBADPLATFORM`, because `package.json` limits `os` and `cpu`. If the shim does end up running on another platform, it stops with:

```
omosense: unsupported platform <key> (supported: darwin-arm64, darwin-x64, linux-arm64, linux-x64)
```

## Signals

The shim forwards SIGTERM and SIGHUP to the binary. Ctrl-C in a terminal reaches the binary directly through the terminal's process group. If you run omosense under a supervisor, send SIGTERM to the shim. A programmatic SIGINT sent only to the shim's pid isn't forwarded.

## npx cache

`npx omosense` runs the binary out of the npx cache, and clearing that cache while omosense runs deletes the binary under it. For a session that stays up for days, install globally with `npm i -g omosense`.

## Building from source

You need Go (see `go.mod` for the version).

```sh
git clone https://github.com/DevNewbie1826/omosense.git
cd omosense
go build ./cmd/omosense
./omosense --help
```

Maintainers: the npm release process is described in [docs/npm-release.md](docs/npm-release.md).

## License

MIT. Copyright (c) 2026 DevNewbie1826.
