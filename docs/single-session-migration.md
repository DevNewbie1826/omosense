# Moving from profiles to one omosense per folder

Older omosense ran one shared daemon for every profile, read `profiles.<name>` from a global `config.json`, and kept every profile's state in one global state dir with a `-<profile>` suffix. Now each session runs its own `omosense` in its own folder, with a flat config and its own state.

This is a one-time manual move, done once per profile. Repeat every step for each profile (for example `main` and `family`), each into its own folder.

Throughout, these names are used:

```sh
OLD=~/.omosense          # the old config dir: whatever OMOSENSE_DIR pointed at (for example ~/.omomeow)
OLDSTATE=$OLD/state      # the old state dir (OMOSENSE_STATE if it was set)
P=main                   # the profile you're moving
NEW=/path/to/session-folder/.omosense
```

## 1. Stop the old daemon first

Detach every `omosense attach ...` monitor of every profile first. An attach client restarts the daemon when its socket is missing. Then stop the daemon with the old binary:

```sh
~/.omomeow/bin/omosense daemon stop
```

Use the plain `daemon stop`. Never `daemon stop --profile <name>`: that's the permanent profile stop, and it marks every pending reminder of the profile `cancelled`.

Copying state while the daemon still runs can capture a half-written file, so don't skip this step.

## 2. Convert the config

The flat config is one former profile, minus the `profiles` wrapper, with two changes:

- `telegram.bots` / `discord.bots` (arrays) become `telegram.bot` / `discord.bot` (one string). Take `bots[0]`. A profile with more than one bot per platform needs a second session folder for the other bot, because one bot belongs to one session.
- `rpc.session` is gone. Instead, after the first start, run `omosense rpc subscribe <session-id>` (step 4).

`herdr` is a new key and defaults to on. Add `"herdr": {"enabled": false}` for a profile that shouldn't watch herdr panes (FAMILY, for example).

This snippet writes the flat config for `$P` and prints it with the role ids masked:

```sh
mkdir -p "$NEW/state"
OLD="$OLD" P="$P" NEW="$NEW" python3 -c '
import json, os
old = json.load(open(os.path.expanduser(os.environ["OLD"]) + "/config.json"))
p = old["profiles"][os.environ["P"]]
cfg = {}
for plat in ("telegram", "discord"):
    sec = p.get(plat) or {}
    bots = sec.get("bots") or []
    out = {"roles": sec.get("roles") or {}}
    if bots:
        out["bot"] = bots[0]
    cfg[plat] = out
rpc = p.get("rpc") or {}
cfg["rpc"] = {"enabled": bool(rpc.get("enabled")), "all": bool(rpc.get("all"))}
if "tidy" in p:
    cfg["tidy"] = p["tidy"]
for k in ("memory", "calendars", "mail"):
    if k in p:
        cfg[k] = p[k]
path = os.environ["NEW"] + "/config.json"
with open(path, "w") as f:
    json.dump(cfg, f, indent=2, ensure_ascii=False)
    f.write("\n")
for plat in ("telegram", "discord"):
    cfg[plat]["roles"] = {"<id-%d>" % i: r for i, r in enumerate(cfg[plat]["roles"].values())}
print(json.dumps(cfg, indent=2, ensure_ascii=False))
print("rpc.session was:", rpc.get("session"))
'
```

Write down the printed `rpc.session` value; step 4 needs it. If the profile should skip herdr, add the `herdr` key to the file by hand.

Check the result loads. `listen --dry-run` reads the config and prints its plan without taking a lock:

```sh
cd /path/to/session-folder && omosense listen --dry-run
```

## 3. Copy the profile's state

Copy, don't move, so the old layout stays intact until the new one works. The names change like this:

| Old (in `$OLDSTATE`) | New (in `$NEW/state`) |
|---|---|
| `reminders-<p>.json` | `reminders.json` |
| `google-seen-<p>.json` | `google-seen.json` |
| `rpc-pending-<p>.json` | `rpc-pending.json` |
| `tg-offset-<bot>` | `tg-offset-<bot>` (same name; only this profile's bot) |
| `memory-tidy.json` | `memory-tidy.json` |
| `threads.json` | `threads.json` |
| `sessions.json` | `sessions.json` |
| `inbox/` | `inbox/` |

Don't copy lock files (`*.lock.json`, `rpc-pending-*.json.lock`) or daemon leftovers (journals, pid, socket, `omosense-profile-*.stopped`). Locks belong to running processes, and the daemon files have no reader now.

```sh
BOT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["telegram"].get("bot",""))' "$NEW/config.json")
S="$NEW/state"
cp -p "$OLDSTATE/reminders-$P.json"    "$S/reminders.json"   2>/dev/null
cp -p "$OLDSTATE/google-seen-$P.json"  "$S/google-seen.json" 2>/dev/null
cp -p "$OLDSTATE/rpc-pending-$P.json"  "$S/rpc-pending.json" 2>/dev/null
[ -n "$BOT" ] && cp -p "$OLDSTATE/tg-offset-$BOT" "$S/" 2>/dev/null
for f in memory-tidy.json threads.json sessions.json; do cp -p "$OLDSTATE/$f" "$S/" 2>/dev/null; done
[ -d "$OLDSTATE/inbox" ] && cp -Rp "$OLDSTATE/inbox" "$S/"
ls -la "$S"
```

A file missing in the old dir just means that profile never wrote it. Then confirm each copy is byte-identical, for example `cmp "$OLDSTATE/reminders-$P.json" "$S/reminders.json"`.

`memory-tidy.json` only matters for the profile with `tidy.enabled`. `threads.json` and `sessions.json` were shared by all profiles, so each folder gets a full copy.

## 4. Start it and subscribe

Arm one monitor in the session for the folder. Use `exec` so a stop signal reaches the binary, and keep `LOG` out of the filter. One monitor now carries every prefix, and a monitor is capped at 200 events per 24 hours:

```
command: cd /path/to/session-folder && exec omosense
filter:  ^(EVENT|CAL|SOON|MAIL|REMIND|HERDR|RPC|TIDY) 
```

This replaces all of that profile's old `omosense attach <source> --profile <p>` monitors.

If the profile had rpc enabled, subscribe the session that should get done notifications (the old `rpc.session`, or the session's current id) and check it:

```sh
cd /path/to/session-folder
omosense rpc subscribe <session-id>
omosense rpc subscription
omosense rpc pending
```

Done notifications no longer come out as `RPC` lines. They arrive as one batched `omo thread send` message after 5 quiet minutes. See the README for the details.

## Things to know

- google needs the `zele` CLI on `PATH`, same as before. google always runs.
- Messages that arrive while a session's omosense is off aren't received. There's no queue for offline sessions.
- Agents that edited `$OLDSTATE/reminders-<p>.json` or `threads.json` now edit `<folder>/.omosense/state/reminders.json` and `<folder>/.omosense/state/threads.json`.
- Callers of `omosense say --profile <p> ...` drop `--profile` and run from the folder (or set `OMOSENSE_DIR`). A leftover `--profile` exits 2.
- The old config and state stay where they were. Remove them yourself once every profile runs from its folder.
