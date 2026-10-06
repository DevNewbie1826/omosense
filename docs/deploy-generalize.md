# omosense generalization — deployment runbook

One-time cutover of the 오우모드 install to the generalized omosense: profile-self-contained
`config.json` (new shape only), wrapper-installed binary, and the skill command swap
(plan `.omo/plans/omosense-generalize.md`, IS-9 / IS-10 / IS-11). Performed by 오우 MAIN on the
owner's account after the PR is merged to `main`. Expected downtime: a few minutes between
steps (d) and (h). Rollback: section (j), restores the old binary, the old config and the old
skill files.

Ground rules:

- Never run `omo update`; never edit `~/.config/agent-messenger`; never print tokens.
- Stop the daemon before replacing the binary (never copy over a running Mach-O).
- Monitor/attach command lines do **not** change (IS-9): step (h) re-arms the exact same
  command lines as before; only the config file content and the four patched skill files change.

All commands are zsh, run as the owner.

## (a) Build

```zsh
cd /Volumes/storage/workspace/omosense && git pull && go build -o /tmp/omosense.new ./cmd/omosense
echo "build exit: $?"
```

Expect `build exit: 0`.

## (b) Backup

```zsh
cp ~/.omomeow/config.json ~/.omomeow/config.json.bak
cp ~/.omomeow/bin/omosense ~/.omomeow/bin/omosense.old
tar -czf ~/.omomeow/skills-backup-$(date +%Y%m%d).tgz -C /Users/mirage/.omo/agent/skills \
  owo-mode/SKILL.md owo-mode/references/integrations.md owo-mode/references/runtime-maintenance.md \
  memory-tidy/SKILL.md
ls -l ~/.omomeow/config.json.bak ~/.omomeow/bin/omosense.old ~/.omomeow/skills-backup-*.tgz
```

Expect the three backup artifacts listed. Keep the tar's date suffix for (j).

## (c) Pre-checks

```zsh
~/.omomeow/bin/omosense daemon status
```

Expect one JSON line: `pid`, `version`, `features` (includes `profile-stop`), `sources` for both
`main` and `family`, and `clients` for both response sessions. Record this output; (i) compares
against it.

## (d) Shutdown

Follow the owo-mode skill's shutdown order exactly — quoted from the skill's
"Full stop from MAIN" section:

> 1. Ask the registered FAMILY pane to detach **every FAMILY monitor** via its saved `kill_bash` handles as part of a full shutdown; send the instruction with `herdr pane send-text`, then Enter. Keep its pane/registration until shutdown completes. Then run `~/.omomeow/bin/omosense daemon stop --profile family` and verify FAMILY is stopped in `daemon status`.
> 2. Stop **every MAIN monitor listed in the source matrix** using its saved `kill_bash` handles. Verify `daemon status` has no clients for either role; resolve any remaining owner before proceeding.
> 3. Run `~/.omomeow/bin/omosense daemon stop` through a bounded command monitor. Require a successful exit and verify the daemon PID/socket and source locks are released. Always-on workers stop here, not when their clients detach.
> 4. Close the FAMILY tab with `herdr tab close <tab>` and remove the stopped roles' `sessions.json` entries. Keep bots, credentials, config, reminders, offsets and zele logins. Report that `오우모드` activates the mode again.

For this deployment, keep both Herdr response panes open and skip only the tab close of
step 4; the monitors themselves must all be detached and the daemon fully stopped before (e).

```zsh
~/.omomeow/bin/omosense daemon stop --profile family   # after FAMILY monitors are detached
~/.omomeow/bin/omosense daemon stop                    # after MAIN monitors are detached
~/.omomeow/bin/omosense daemon status                  # expect: dial unix ... omosense.sock: no such file or directory (exit 1)
```

## (e) Install the new binary + wrapper

Write-to-temp then `mv` (atomic on the same volume); never `cp` over a binary path directly:

```zsh
cp /tmp/omosense.new ~/.omomeow/bin/omosense.real.tmp && mv ~/.omomeow/bin/omosense.real.tmp ~/.omomeow/bin/omosense.real
codesign -f -s - ~/.omomeow/bin/omosense.real
w=$(mktemp ~/.omomeow/wrapper.XXXXXX) && printf '#!/bin/sh\nexport OMOSENSE_DIR="$HOME/.omomeow"\nexec "$HOME/.omomeow/bin/omosense.real" "$@"\n' > "$w" && chmod 755 "$w" && mv "$w" ~/.omomeow/bin/omosense
head -n 3 ~/.omomeow/bin/omosense
~/.omomeow/bin/omosense --help > /dev/null && echo "wrapper ok"
```

Expect the wrapper's three lines from `head`, then `wrapper ok`.

Sanity — the still-legacy config must fail loudly with the new binary (IS-2):

```zsh
~/.omomeow/bin/omosense listen --profile main --dry-run; echo "exit: $?"
```

Expect exit 1 and stderr naming the legacy key, e.g.
`omosense: config.json: legacy top-level key "telegram" is no longer supported; move it into profiles.<name> (see new profile shape)`.
This is the intended loud failure; proceed to (f).

## (f) Convert the config (one command)

Reads `config.json.bak`, writes the new `config.json` (exact target from the plan:
main telegram bots `["owo_dm"]` + roles from the old telegram owner/wife ids, discord bots
`["omomeow"]` + the discord owner role, `rpc` enabled, `tidy` enabled/learnOthers with
exclude `owo-family-bccd4b63`, memory `owo-mode-57b805e5`; family telegram bots `["owo"]`
with the same telegram roles, empty discord, `rpc`/`tidy` disabled, memory
`owo-family-bccd4b63`; calendars/mail copied per profile; drops `telegram.bot`,
`telegram.dm_bot`, `discord.work_guild`, `discord.main_channel`, which have no readers
after this change), then prints the result with ids masked:

```zsh
python3 -c '
import json, os
d = os.path.expanduser("~/.omomeow")
old = json.load(open(d + "/config.json.bak"))
tg, dc = old["telegram"], old["discord"]
pm, pf = old["profiles"]["main"], old["profiles"]["family"]
tg_roles = {str(tg["owner"]): "owner", str(tg["wife"]): "wife"}
cfg = {"profiles": {
 "main": {"telegram": {"bots": pm["telegram"], "roles": tg_roles},
          "discord": {"bots": [dc["bot"]], "roles": {str(dc["owner"]): "owner"}},
          "rpc": {"enabled": True, "session": None, "all": False},
          "tidy": {"enabled": True, "learnOthers": True, "exclude": ["owo-family-bccd4b63"]},
          "memory": "owo-mode-57b805e5",
          "calendars": pm["calendars"], "mail": pm["mail"]},
 "family": {"telegram": {"bots": pf["telegram"], "roles": tg_roles},
            "discord": {"bots": [], "roles": {}},
            "rpc": {"enabled": False, "session": None, "all": False},
            "tidy": {"enabled": False, "learnOthers": False, "exclude": []},
            "memory": "owo-family-bccd4b63",
            "calendars": pf["calendars"], "mail": pf["mail"]}}}
with open(d + "/config.json", "w") as f:
    json.dump(cfg, f, indent=2, ensure_ascii=False)
    f.write("\n")
shown = json.loads(json.dumps(cfg))
hide = {str(tg["owner"]): "<tg-owner-id>", str(tg["wife"]): "<tg-wife-id>", str(dc["owner"]): "<dc-owner-id>"}
for p in shown["profiles"].values():
    for plat in ("telegram", "discord"):
        p[plat]["roles"] = {hide.get(k, k): v for k, v in p[plat]["roles"].items()}
print(json.dumps(shown, indent=2, ensure_ascii=False))
'
```

Expect the masked print to show exactly the structure above (both profiles, `main` rpc+tidy
enabled, `family` both disabled, `memory` fields set, calendars/mail carried over).

## (g) Apply the skill patch

The patch ships in the repo (paths `a/<skill-relpath>` / `b/<skill-relpath>`), so the exact
strip level is `-p1`:

```zsh
patch -p1 -d /Users/mirage/.omo/agent/skills < /Volumes/storage/workspace/omosense/docs/owo-mode-skill.patch
echo "patch exit: $?"
```

Expect `patching file owo-mode/SKILL.md`, `patching file owo-mode/references/integrations.md`,
`patching file owo-mode/references/runtime-maintenance.md`, `patching file memory-tidy/SKILL.md`
and `patch exit: 0`. (Full grep verification is in (i).)

## (h) Start

Re-arm the monitors exactly as the owo-mode skill's Turn ON (section 3, "MAIN start" /
"FAMILY start") specifies — unchanged command lines. Quoting the skill:

> 2. Arm any missing MAIN rows in the source matrix through the persistent monitor tool, never shell `&`. Keep the handles for shutdown and duplicate detection.

One monitor per source-matrix row, each with `command: "exec ~/.omomeow/bin/omosense attach <source> --profile <profile>"`, the listed `filter`, and `persistent: true`:

| Attach source | Profiles | Monitor filter |
|---|---|---|
| `listen` | MAIN, FAMILY | `^(EVENT\|LOG) ` |
| `google` | MAIN, FAMILY | `^(CAL\|SOON\|MAIL\|LOG) ` |
| `remind` | MAIN, FAMILY | `^(REMIND\|LOG) ` |
| `herdr` | MAIN | `^(HERDR\|LOG) ` |
| `rpc` | MAIN | `^(RPC\|LOG) ` |
| `tidy` | MAIN | `^(TIDY\|LOG) ` |

The first attach auto-starts the daemon, which reads the new config. Then run the skill's
subscription-armed check:

```zsh
~/.omomeow/bin/omosense daemon status
```

Expect every expected worker `running` (FAMILY's unsubscribed `herdr` worker may show
`paused`; FAMILY must have no `tidy` and no `rpc` source at all).

## (i) Post-checks

```zsh
# daemon + sources per profile (main: rpc+tidy present; family: neither)
~/.omomeow/bin/omosense daemon status
~/.omomeow/bin/omosense daemon status | python3 -c 'import json,sys; s=json.load(sys.stdin); print(sorted((x["profile"],x["name"]) for x in s["sources"]))'

# PLAN lines list the right bots
~/.omomeow/bin/omosense listen --profile main --dry-run    # PLAN telegram ["owo_dm"], discord ["omomeow"]
~/.omomeow/bin/omosense listen --profile family --dry-run  # PLAN telegram ["owo"], discord []

# SEND CHECK — explicit bot, then default bot (must be owo_dm for --profile main)
~/.omomeow/bin/omosense say telegram send '{"chat_id":6835736153,"bot":"owo_dm","text":"배포 확인"}'; echo "exit: $?"
~/.omomeow/bin/omosense say telegram send '{"chat_id":6835736153,"text":"배포 확인 (기본 봇)"}'; echo "exit: $?"

# TIDY CHECK
~/.omomeow/bin/omosense tidy --now --profile main; echo "exit: $?"     # one TIDY {...} line (or silence), exit 0
~/.omomeow/bin/omosense tidy --now --profile family; echo "exit: $?"   # LOG tidy disabled for profile family, exit 0

# skill patch greps — all three must print nothing
grep -rnE '\.omomeow/[a-z-]+\.ts' /Users/mirage/.omo/agent/skills
grep -rn 'bun ~/.omomeow' /Users/mirage/.omo/agent/skills
grep -rn 'owner/wife\|the wife\|telegram.wife' /Users/mirage/.omo/agent/skills/owo-mode/SKILL.md /Users/mirage/.omo/agent/skills/owo-mode/references /Users/mirage/.omo/agent/skills/memory-tidy/SKILL.md
```

Expect: both `say` sends exit 0 and the owner receives both messages **from @owo_dm_bot**
(the second proves the default bot is the profile's first bot); `tidy --now --profile main`
exits 0 (first run after the shape change may print a `TIDY` line for repos moved past the
watermark — that is a normal incremental report, handle per the memory-tidy skill);
`--profile family` prints exactly `LOG tidy disabled for profile family` and exits 0; the
three greps print nothing.

## (j) Rollback

Only if (i) failed. Stop as in (d) (FAMILY monitors + `daemon stop --profile family`, MAIN
monitors + `daemon stop`), then:

```zsh
mv ~/.omomeow/bin/omosense.old ~/.omomeow/bin/omosense
rm -f ~/.omomeow/bin/omosense.real
cp ~/.omomeow/config.json.bak ~/.omomeow/config.json
tar -xzf ~/.omomeow/skills-backup-<DATE>.tgz -C /Users/mirage/.omo/agent/skills   # same date suffix as (b)
```

Restart as in (h) (the old binary reads `~/.omomeow` by default — no wrapper, no
`OMOSENSE_DIR` involved), then verify:

```zsh
~/.omomeow/bin/omosense daemon status
```

Expect the pre-deployment shape from (c): both profiles' sources, no wrapper.

After a successful (i), clean up the rollback aids when the install has been stable:

```zsh
rm -f ~/.omomeow/bin/omosense.old /tmp/omosense.new
```

Keep `config.json.bak` and the skills tarball.
