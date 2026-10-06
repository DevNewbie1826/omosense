# omosense generalization — deployment runbook

One-time cutover of the 오우모드 install to the generalized omosense: profile-self-contained
`config.json` (new shape only), wrapper-installed binary, and the skill command swap
(plan `.omo/plans/omosense-generalize.md`, IS-9 / IS-10 / IS-11). Performed by 오우 MAIN on the
owner's account after the PR is merged to `main`. Expected downtime: a few minutes between
steps (d) and (h). Rollback: section (j), restores the old binary, the old config and
the old skill files; the live state files (reminders, sessions, replay journals,
Telegram offsets, google seen, tidy watermark) are deliberately left untouched — the
pre-deploy state snapshot from (b) is kept only for (j)'s emergency recovery.

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

# State a deploy must never lose — the emergency-recovery source for (j); ordinary
# rollback leaves live state in place. The globs expand INSIDE the state directory
# (the subshell cd), so they match regardless of the caller's cwd or null_glob.
sb=~/.omomeow/state-backup-$(date +%Y%m%d); mkdir -p $sb
( cd ~/.omomeow/state && setopt null_glob
  for f in reminders-main.json reminders-family.json sessions.json \
           omosense-journal-main.jsonl omosense-journal-family.jsonl \
           tg-offset-* google-seen-*.json memory-tidy.json; do
    [[ -e $f ]] && cp -p $f $sb/
  done; true )
# completeness gate — every state file this install must have, verified present and
# byte-identical; a missing or differing file fails the backup loudly
expected=(reminders-main.json reminders-family.json sessions.json
          omosense-journal-main.jsonl omosense-journal-family.jsonl
          tg-offset-owo_dm tg-offset-owo google-seen-main.json google-seen-family.json
          memory-tidy.json)
for f in $expected; do
  [[ -f $sb/$f ]] || { echo "BACKUP INCOMPLETE: missing $f"; exit 1; }
  cmp -s ~/.omomeow/state/$f $sb/$f || { echo "BACKUP CORRUPT: $f differs"; exit 1; }
done
echo "state backup verified: ${#expected} files byte-identical"
ls -l ~/.omomeow/config.json.bak ~/.omomeow/bin/omosense.old ~/.omomeow/skills-backup-*.tgz $sb
```

Expect `state backup verified: 10 files byte-identical` before the listing of the three
backup artifacts plus the state backup directory. Any `BACKUP INCOMPLETE`/`BACKUP CORRUPT`
line aborts the deploy before the shutdown — a silently partial backup is worse than
none. Keep the date suffixes for (j). What each state file protects:

| File | Why it must survive the deploy |
|---|---|
| `reminders-main.json`, `reminders-family.json` | The scheduled reminder queues. A truthy `sent`/`skipped`/`failed`/`cancelled` marker is terminal — the scheduler never resends such an entry — so a destructive rewrite is unrecoverable from live state. |
| `sessions.json` | Both roles' response-session registration (rpc deliver reads it; only the permanent full stop removes entries). |
| `omosense-journal-main.jsonl`, `omosense-journal-family.jsonl` | Queued always-on replay lines; a destructive profile stop discards pending entries. |
| `tg-offset-*` | Per-bot Telegram fetch offsets; losing them would re-fetch updates that were already processed. |
| `google-seen-*.json` | Calendar/mail seen-state; losing it would re-emit `CAL`/`MAIL` duplicates. |
| `memory-tidy.json` | Tidy watermark; losing it would force a redundant re-tidy. |

Source lock files (`listen-*.lock.json`, `watch-*.lock.json`, …) and the daemon's own
pid/log/lock metadata are deliberately **not** backed up or restored: locks belong to
running processes, and a restored stale lock would block workers.

## (c) Pre-checks

```zsh
~/.omomeow/bin/omosense daemon status
```

Expect one JSON line: `pid`, `version`, `features` (includes `profile-stop`), `sources` for both
`main` and `family`, and `clients` for both response sessions. Record this output; (i) and (j)
compare against it.

Then record the pending-reminder baseline — the same command runs again in (d), (h), (i)
and (j). Counts may move during the deploy window only when real work happened (a due
reminder gains a terminal marker; explicitly added entries appear) — the identity
snapshot below makes every such change provable record-by-record, and after (j)'s
rollback the state must equal the pre-rollback snapshot exactly. "Pending" uses the reminder scheduler's own
semantics (`internal/remind/sched.go`): an entry counts as pending unless `sent`,
`skipped`, `failed` or `cancelled` holds a truthy value:

```zsh
python3 -c '
import json, glob, os
def truthy(v):
    if v is None or isinstance(v, bool): return bool(v)
    if isinstance(v, (int, float)): return v == v and v != 0
    if isinstance(v, str): return v != ""
    return True
for f in sorted(glob.glob(os.path.expanduser("~/.omomeow/state/reminders-*.json"))):
    try: rs = json.load(open(f))
    except FileNotFoundError: rs = []
    print(os.path.basename(f), "pending",
          sum(1 for r in rs if not any(truthy(r.get(k)) for k in ("sent", "skipped", "failed", "cancelled"))))
'
```

Expect `reminders-main.json pending N` and `reminders-family.json pending M`; record N and M.

Alongside the count, record the identity snapshot — ids with their terminal markers,
offsets, seen keys, watermark — at every checkpoint ((d), (h), (i) and twice in (j));
it is the evidence that nothing was lost or rewound. The journal is deliberately not
in it: the daemon legitimately rewrites its frame file on every open, so (j) checks it
by content greps instead:

```zsh
cat > /tmp/omostate-id.py <<'PY'
import json, glob, os
state = os.path.expanduser("~/.omomeow/state")
def mark(r):
    for k in ("sent", "skipped", "failed", "cancelled"):
        if r.get(k): return k
    return "pending"
for f in sorted(glob.glob(state + "/reminders-*.json")):
    print(os.path.basename(f), sorted((str(r.get("id")), mark(r)) for r in json.load(open(f))))
for f in sorted(glob.glob(state + "/tg-offset-*")):
    print(os.path.basename(f), open(f).read().strip())
for f in sorted(glob.glob(state + "/google-seen-*.json")):
    print(os.path.basename(f), sorted(json.load(open(f))))
wm = json.load(open(state + "/memory-tidy.json"))
print("memory-tidy.json", sorted((k, str(v)) for k, v in (wm.get("repos") or {}).items()))
PY
python3 /tmp/omostate-id.py
```

Expect the seeded ids with their markers, both bots' offset values, the seen keys and
the watermark repos. Record this output at every checkpoint.

## (d) Shutdown (state-preserving — never `daemon stop --profile`)

> **WARNING — never run `daemon stop --profile family` (or `--profile main`) anywhere in
> this deployment or its rollback.** That subcommand is the *permanent* profile stop:
> it marks every still-pending reminder of the profile `cancelled` — a terminal marker;
> `internal/remind/sched.go` never resends an entry with a truthy
> `sent`/`skipped`/`failed`/`cancelled` — discards the profile's queued replay journal,
> and writes `state/omosense-profile-<name>.stopped`, which keeps the profile stopped
> across daemon restarts. A later attach re-enables the workers but does **not** revive
> the cancelled reminders or the discarded replay. The command exists for the owo-mode
> skill's permanent stop flows (independent FAMILY stop, full stop with tab close), where
> cancelling the queue is intended. A deployment is a *temporary* suspension: all of
> that state must survive in place.

This is the owo-mode skill's "Full stop from MAIN" order with exactly two deviations:
the skill's step-1 `daemon stop --profile family` is replaced by the plain stop in step 4
below, and the skill's step-4 tab close / `sessions.json` removal is skipped — both
Herdr panes and both registrations stay.

1. Ask the registered FAMILY pane to detach **every FAMILY monitor** via its saved
   `kill_bash` handles; send the instruction with `herdr pane send-text`, then Enter.
   Keep its pane and its `sessions.json` entry.
2. Stop **every MAIN monitor listed in the source matrix** using its saved `kill_bash`
   handles. Attach clients auto-spawn the daemon when its socket is missing, so every
   monitor of **both** roles must be detached before the next step, or the next attach
   dial brings the daemon straight back.
3. Verify `daemon status` has no clients for either role; resolve any remaining owner
   before proceeding. (Detaching does not stop always-on workers — Discord and the
   reminder schedulers keep running until step 4; that is fine.)
4. Run plain `~/.omomeow/bin/omosense daemon stop` (no `--profile`) through a bounded
   command monitor. A plain stop terminates every worker of both profiles without
   cancelling reminders, without discarding replay journals and without writing stop
   markers; the daemon sends each remaining attach client a shutdown frame on its way
   out.
5. Verify the daemon is gone:

```zsh
~/.omomeow/bin/omosense daemon stop                    # after ALL monitors of both roles are detached
~/.omomeow/bin/omosense daemon status                  # expect: dial unix ... omosense.sock: no such file or directory (exit 1)
```

Re-run the (c) count and identity commands: nothing was cancelled — pending ids keep
their markers, and any new terminal marker names a reminder that actually came due
while the daemon was still running. Keep both Herdr panes open and both `sessions.json` entries registered;
(h) re-arms the same monitors and the registrations stay valid. Do not close the FAMILY
tab and do not remove any registration — that happens only in the skill's permanent
full stop. Report that `오우모드` keeps both panes and will re-arm after (h).

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

Arm **every row for both profiles** — MAIN and FAMILY alike, including all three FAMILY
rows (`listen`, `google`, `remind`); FAMILY arms nothing else (no `herdr`, `rpc`, `tidy`).
No stop marker was written in (d), so both profiles start enabled and the attach clients
restore both roles' subscriptions directly. The first attach auto-starts the daemon,
which reads the new config. Then run the skill's subscription-armed check — and its
common precheck point about registration: confirm both roles' `sessions.json` entries
still name the open panes ((d) never removed them):

```zsh
~/.omomeow/bin/omosense daemon status
```

Expect every expected worker `running` (FAMILY's unsubscribed `herdr` worker may show
`paused`; FAMILY must have no `tidy` and no `rpc` source at all), and one `clients` entry
per armed matrix row for **both** roles — `listen-family`, `google-family`,
`remind-family` alongside the MAIN rows.

Then re-run the (c) count and identity commands (the swap itself must not touch the
queues — any change must name work that really happened while the new binary ran) and
confirm the registrations survived the binary swap:

```zsh
cat ~/.omomeow/state/sessions.json   # both main and family entries, panes unchanged
```

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

Finally, re-run the (c) count and identity commands and `daemon status` once more:
every difference from (c) names work that really happened in the window (a due reminder
now `sent`, an explicitly added reminder now pending), sources/clients match the (h)
armed state, and this identity output is the pre-rollback snapshot (j) compares against.

## (j) Rollback

Only if (i) failed. Stop exactly as in (d) — detach every FAMILY and MAIN monitor, then
plain `daemon stop` (**never** `daemon stop --profile family`; the (d) warning applies
unchanged) — and verify the daemon is gone. With the daemon down, freeze the identity
snapshot the rollback must preserve, then restore binary, config and skills — and
never touch state:

```zsh
python3 /tmp/omostate-id.py > /tmp/omostate-rb0.txt && wc -l < /tmp/omostate-rb0.txt   # frozen pre-rollback state
```

```zsh
mv ~/.omomeow/bin/omosense.old ~/.omomeow/bin/omosense
rm -f ~/.omomeow/bin/omosense.real
cp ~/.omomeow/config.json.bak ~/.omomeow/config.json
tar -xzf ~/.omomeow/skills-backup-<DATE>.tgz -C /Users/mirage/.omo/agent/skills   # same date suffix as (b)
# NOTE: nothing writes to ~/.omomeow/state here — live state survives the rollback
# exactly as the workers left it (the (b) snapshot is for emergencies only, below).

# restoration proofs
cmp ~/.omomeow/config.json ~/.omomeow/config.json.bak && echo "config identical to backup"
file ~/.omomeow/bin/omosense | cut -d: -f2                 # Mach-O 64-bit executable arm64 — not the #!/bin/sh wrapper
grep -c 'bun ~/.omomeow' /Users/mirage/.omo/agent/skills/owo-mode/SKILL.md   # old skill text is back (> 0)

# preservation proof — rb0 was frozen after the stop, rb1 is taken now: the rollback
# copied nothing over state, so the two identity snapshots must be identical
python3 /tmp/omostate-id.py > /tmp/omostate-rb1.txt
diff /tmp/omostate-rb0.txt /tmp/omostate-rb1.txt && echo "state untouched by rollback"
```

Nothing here rewrites state: reminders added while the new binary ran stay, terminal
markers (`sent`/`skipped`/`failed`/`cancelled`) taken in the window stay terminal — the
scheduler never resends them — Telegram offsets stay advanced so no update is fetched
twice, calendar/mail seen keys and the tidy watermark stay where the new binary left
them, and the replay journals keep every queued line. The old binary reads all of
these files unchanged (the state formats are identical across the two binaries), so
the restart below continues exactly where the deploy was abandoned. Lock files were
never backed up and must not exist for a stopped daemon; if a stale lock survived an
abnormal exit, resolve it per the skill's lock-conflict diagnosis — never delete a
live holder's lock.

### Emergency state recovery — rewind, not rollback (corrupted state dir only)

Only when the state directory itself was damaged by the failed attempt (an unparseable
reminders file, a truncated journal) — never as a routine step, never while a daemon
is running, and never to "undo" work that happened in the window. Restoring the (b)
snapshot rewinds everything processed since (b): reminders added in the window vanish,
terminal markers taken in the window are erased (those entries become pending again
and WILL be re-delivered), Telegram offsets regress (updates processed in the window
arrive again), seen keys regress (duplicate `CAL`/`MAIL`), and the tidy watermark
rewinds (a redundant re-tidy). Quarantine the damaged state first, diff it, then
rewind:

```zsh
q=~/.omomeow/state-rewind-$(date +%Y%m%d%H%M%S); mkdir -p $q
( cd ~/.omomeow/state && setopt null_glob
  for f in *.json *.jsonl tg-offset-*; do cp -p $f $q/; done )   # quarantine the damaged files
cp ~/.omomeow/state-backup-<DATE>/* ~/.omomeow/state/            # LAST RESORT — rewinds processed state
```

Restart as in (h): re-arm every source-matrix row for **both** profiles (the old binary
reads `~/.omomeow` by default — no wrapper, no `OMOSENSE_DIR` involved), then verify the
same armed state as (c)/(h):

```zsh
~/.omomeow/bin/omosense daemon status
```

Expect the pre-deployment shape from (c): both profiles' sources with the expected
workers `running`, `clients` for both roles, no wrapper. Re-run the (c) identity
command: the output equals the pre-rollback snapshot — pending entries (including any
added while the new binary ran) are scheduled again, terminal entries are not
re-delivered (no duplicate send in the bots' history), offsets continue from their
advanced values — and `sessions.json` still holds both roles' entries. Confirm the
window's journal lines survived the old binary's restart: `grep -c` a marker from an
event delivered while the new binary ran (e.g. its message text) in
`~/.omomeow/state/omosense-journal-family.jsonl` before and after the restart — same
count (the old binary replay-parses the journal the new binary wrote; a format drift
would have failed the daemon's start).

After a successful (i), clean up the rollback aids when the install has been stable:

```zsh
rm -f ~/.omomeow/bin/omosense.old /tmp/omosense.new
# state-backup-<DATE> is the emergency-recovery source — keep it for a few stable
# days before `rm -rf ~/.omomeow/state-backup-<DATE>`
```

Keep `config.json.bak` and the skills tarball.
