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

herdr is on by default because for a setup without rpc it's the main signal. Turn it off with `"herdr": {"enabled": false}`.

A source that crashes is restarted after a backoff that starts at 5 seconds and doubles up to 5 minutes, with `LOG omosense source <name> crashed: <err>`. If another live process already holds that source's lock (for example a second `omosense` in the same folder), the source logs `ALREADY_RUNNING ...` and retries every 30 seconds.

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

| Key | Meaning |
|---|---|
| `telegram.bot`, `discord.bot` | One bot name per platform, looked up in `~/.config/agent-messenger/<platform>bot-credentials.json` (`telegrambot-credentials.json`, `discordbot-credentials.json`). Unset means that listener doesn't run. |
| `telegram.roles`, `discord.roles` | User id to role (`owner`, `wife`, `trusted`, ...), attached to each `EVENT`. |
| `rpc.enabled`, `rpc.all` | Watch webchat sessions; `all` watches every session, not only those registered in `threads.json`. |
| `tidy.enabled`, `tidy.learnOthers`, `tidy.exclude` | Memory tidy watcher and which agents' memory repos it skips. |
| `herdr.enabled` | Absent means on. Only an explicit `false` turns herdr off. |
| `memory` | This agent's memory id. |
| `calendars` | Calendars google watches. Absent means all of them. |
| `mail` | Whether google also watches mail. |

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
| `threads.json` | rpc session registrations. |
| `sessions.json` | Response session registration. |
| `memory-tidy.json` | Tidy watermark. |
| `tg-offset-<bot>` | Telegram fetch offset for that bot. |
| `inbox/` | Downloaded Telegram attachments. |
| `*.lock.json` | Source locks: `listen`, `remind`, `watch-google`, `watch-herdr`, `watch-rpc`, `memory-tidy`. |
| `rpc-subscription.lock` | Lock guarding `rpc-subscription.json` updates. |

A few things stay global on purpose: bot credentials in `~/.config/agent-messenger`, tidy backups in `~/.omo/memory-backups`, and memory repos in `~/.omo/memory/agents` (or `$OMO_MEMORY_AGENTS`).

Messages that arrive while the session (and its omosense) is off aren't received. That's by design: there's no queue or retry for an offline session.

## Watching it from an agent session

Arm one monitor on `omosense` in the project folder. The command is `exec omosense` so a stop signal reaches the binary. Filter out `LOG` so housekeeping lines don't spend the monitor's event budget:

```
command: cd ~/work/my-agent && exec omosense
filter:  ^(EVENT|CAL|SOON|MAIL|REMIND|HERDR|RPC|TIDY) 
```

## Sending messages

`say` sends through the folder's configured bot and prints the API response:

```sh
omosense say telegram send '{"chat_id":123456789,"text":"hello"}'
omosense say telegram send '{"bot":"other_bot","chat_id":123456789,"text":"hello"}'
```

Telegram actions: `send`, `edit`, `draft`, `typing`, `react`, `unreact`, `topic`, `topic-edit`, `photo`, `doc`. Discord actions: `send`, `edit`, `typing`, `react`, `unreact`, `thread`, `thread-edit`, `file`.

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

How batching works:

- Each done restarts a 5-minute quiet timer. When 5 minutes pass with no new done, exactly one message goes out (`omo thread send`) listing every completion since the last batch. If a session finishes twice inside the window, only its newest seq is listed, with a count.
- Each entry in the message carries its own ack command with absolute paths, `OMOSENSE_DIR='<dir>' OMOSENSE_STATE='<state>' omosense rpc ack <id> <seq>`, so it works from anywhere. A long batch is cut at 32 KiB with a `+N more: ... omosense rpc pending` line.
- Nothing is re-sent. Entries stay in `rpc-pending.json` until acked.
- No subscriber: the batch is dropped (`LOG rpc batch dropped: no subscriber`).
- Subscriber not alive in `omo thread list`: the batch is dropped and the subscription is removed (`LOG rpc batch dropped: subscriber <id> not alive; unsubscribed`).
- A send that fails while the subscriber is alive is retried after a minute.

A session that comes back after being off should run `omosense rpc pending` to catch up, then `omosense rpc subscribe <its-id>` again if it was unsubscribed.

`ack <id> <seq>` leaves the entry pending and prints `newer` when a newer completion of that session has arrived since.

## Subcommands

Each source can also run alone, which is handy for checks:

```
omosense listen [--dry-run]
omosense google [--once]
omosense remind
omosense herdr [--once]
omosense rpc [--once] [--all]
omosense tidy [--once|--now] [flags]
omosense say <platform> <action> <json>
```

`--once`, `--now` and `--dry-run` are read-only: they take no lock and don't create the state dir. Run `omosense <subcommand> --help` for details.

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
