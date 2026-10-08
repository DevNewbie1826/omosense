#!/usr/bin/env bash
#
# qa11-live-audit.sh - read-only live-safety audit for omosense hosts (QA-11 / S5-8).
#
# Proves a QA run left the LIVE hosts untouched: same host pids, same process
# start times, same deployed binary hash. It also snapshots the live state
# dir into a sandbox; that snapshot is a RECEIPT ONLY - live state contents
# and mtimes are deliberately NOT a pass criterion, because a live host keeps
# writing its own state (owner decision: that criterion was rejected).
#
# Usage:
#   scripts/qa11-live-audit.sh before <live-folder> <receipt-dir>
#       Take the receipt into <receipt-dir> (must resolve under /tmp):
#         state-copy/     cp -p copy of <live-folder>/.omosense/state
#                         (regular files only; the source is never written)
#         state.sha256    sorted "sha256  ./relative/path" of the copy
#         pids.txt        sorted "<lock> <pid> <ps lstart>" for every
#                         <live-folder>/.omosense/state/*.lock.json
#                         ("-" when the pid is not alive)
#         binary.sha256   sorted unique "sha256  <argv0>" where argv0 comes
#                         from `ps -o comm= -p <pid>` of every live lock pid
#         meta.txt        live folder, UTC timestamp, hostname (never compared)
#   scripts/qa11-live-audit.sh after <live-folder> <receipt-dir>
#       Recompute pids + start times + binary hash and compare them with the
#       before receipt: identical -> "PASS" exit 0; different -> "FAIL <what>"
#       exit 1. The state snapshot is never compared here.
#   scripts/qa11-live-audit.sh selftest
#       Run the built-in control: a fake live folder under /tmp with a real
#       helper process, PASS while untouched, FAIL on a tampered start time,
#       a tampered binary hash and a missing receipt.
#   scripts/qa11-live-audit.sh --help
#       This help.
#
# Exit codes: 0 ok / 1 audit FAIL or unreadable input / 2 usage or refusal.
#
# Safety: the receipt dir is validated BEFORE anything is created - it must
# be lexically under /tmp or /private/tmp, contain no `..`, carry no symlink
# component below the /tmp prefix (on macOS /tmp itself IS a symlink to
# /private/tmp), and its deepest existing ancestor must resolve under /tmp.
# Refusal is exit 2 with nothing written. The receipt dir is created with
# mkdir and must be a real directory under /tmp; every receipt target
# (state-copy, state.sha256, pids.txt, binary.sha256, meta.txt) is written
# only after a [ -L ]/[ -e ] check with noclobber set, so an existing or
# dangling symlink target is refused before any write. The live tree is
# resolved with symlinks followed - the live folder, its .omosense dir and its
# state source - and a receipt dir that aliases it at any level (equal to,
# inside, or containing any of the three) is refused, so a symlinked .omosense
# or state, or a /tmp vs /private/tmp spelling, cannot redirect a write into
# the live state. The live folder is only ever read (find, cat, cp-from, ps,
# shasum) and `after` writes nothing at all. The script never deletes anything
# and never signals any process (the selftest kills only the helper process it
# started itself).

set -euo pipefail
export LC_ALL=C

usage() {
  cat <<'EOF'
usage: scripts/qa11-live-audit.sh <command> [args]

  before <live-folder> <receipt-dir>   take the audit receipt (receipt-dir under /tmp)
  after <live-folder> <receipt-dir>    recompute + compare with the before receipt
  selftest                             run the built-in self-test
  --help                               this help

Pass criteria are the host pids, their ps start times and the deployed
binary hash only; the copied state is a snapshot receipt and is never
compared. The script reads the live folder, writes only inside a receipt dir
confined to /tmp, never signals any process. Exit codes: 0 ok, 1 audit FAIL,
2 usage or an unsafe receipt dir. A receipt dir is refused when it is not
confined to /tmp, contains a symlink component, is itself a symlink or not a
directory, sits inside the live folder, or contains it, or resolves into or
contains the live tree at any level - the resolved live folder, its resolved
.omosense dir, or its resolved state source (symlinks followed, so a
symlinked .omosense/state and a /tmp vs /private/tmp spelling are both seen);
a receipt target that already exists or is a symlink (dangling included) is
refused before any write.
EOF
}

# resolve_receipt_dir DIR -> sets RECV to the absolute, symlink-resolved
# receipt dir path. Creates nothing. Exits 2 (before any write) unless DIR is
# lexically under /tmp or /private/tmp, contains no `..`, has no symlink
# component below the /tmp prefix (the prefix itself is exempt: on macOS /tmp
# IS a symlink to /private/tmp), and its deepest existing ancestor resolves
# (pwd -P) under /tmp or /private/tmp.
resolve_receipt_dir() {
  local dir="$1" prefix rest comp cur ancestor tail resolved
  while [ "${dir%/}" != "$dir" ]; do dir="${dir%/}"; done
  case "$dir" in
    /tmp/*) prefix="/tmp"; rest="${dir#/tmp/}" ;;
    /private/tmp/*) prefix="/private/tmp"; rest="${dir#/private/tmp/}" ;;
    *)
      printf 'FAIL receipt dir must be under /tmp: %s\n' "$1" >&2
      exit 2
      ;;
  esac
  case "$dir" in
    *..*)
      printf 'FAIL receipt dir must not contain ..: %s\n' "$dir" >&2
      exit 2
      ;;
  esac
  cur="$prefix"
  ancestor="$prefix"
  tail=""
  while [ -n "$rest" ]; do
    case "$rest" in
      */*) comp="${rest%%/*}"; rest="${rest#*/}" ;;
      *) comp="$rest"; rest="" ;;
    esac
    if [ -z "$comp" ]; then
      printf 'FAIL receipt dir has an empty component: %s\n' "$dir" >&2
      exit 2
    fi
    if [ -n "$tail" ]; then
      tail="$tail/$comp"
      continue
    fi
    cur="$cur/$comp"
    if [ -L "$cur" ]; then
      printf 'FAIL receipt dir path contains a symlink: %s\n' "$cur" >&2
      exit 2
    fi
    if [ ! -e "$cur" ]; then
      tail="$comp"
      continue
    fi
    if [ ! -d "$cur" ]; then
      printf 'FAIL receipt dir path component is not a directory: %s\n' "$cur" >&2
      exit 2
    fi
    ancestor="$cur"
  done
  if ! resolved="$(cd "$ancestor" && pwd -P)"; then
    printf 'FAIL cannot resolve receipt dir ancestor: %s\n' "$ancestor" >&2
    exit 2
  fi
  # The deepest existing ancestor may be the /tmp prefix itself when the
  # receipt dir is a not-yet-existing direct child of /tmp.
  case "$resolved" in
    /tmp | /tmp/* | /private/tmp | /private/tmp/*) ;;
    *)
      printf 'FAIL receipt dir resolves outside /tmp: %s -> %s\n' "$dir" "$resolved" >&2
      exit 2
      ;;
  esac
  if [ -n "$tail" ]; then
    RECV="$resolved/$tail"
  else
    RECV="$resolved"
  fi
}

# path_within A B -> success when A is B or A sits inside B. Both sides are
# already symlink-resolved and carry no trailing slash.
path_within() {
  local a="$1" b="$2"
  [ "$a" = "$b" ] && return 0
  case "$a/" in
    "$b/"*) return 0 ;;
  esac
  return 1
}

# resolve_omo_dir LIVE -> stdout the real (symlink-followed) LIVE/.omosense
# dir; exit 2 when it cannot be resolved.
resolve_omo_dir() {
  local out
  if ! out="$(cd "$1/.omosense" 2>/dev/null && pwd -P)"; then
    printf 'FAIL cannot resolve live .omosense dir: %s\n' "$1/.omosense" >&2
    exit 2
  fi
  printf '%s\n' "$out"
}

# resolve_state_src LIVE -> stdout the real (symlink-followed) state source
# LIVE/.omosense/state; exit 2 when it cannot be resolved. This is the path the
# snapshot is copied FROM and the path the receipt must never alias.
resolve_state_src() {
  local out
  if ! out="$(cd "$1/.omosense/state" 2>/dev/null && pwd -P)"; then
    printf 'FAIL cannot resolve live state dir: %s\n' "$1/.omosense/state" >&2
    exit 2
  fi
  printf '%s\n' "$out"
}

# refuse_alias LIVE_ABS OMO_ABS STATE_SRC -> exit 2 (nothing written) when the
# resolved receipt dir RECV aliases the live tree: equal to, inside, or
# containing the resolved live folder, the resolved .omosense dir, or the
# resolved state source. Every side is symlink-resolved, so a symlinked
# .omosense/state (a different real directory) and a /tmp vs /private/tmp
# spelling of the same location are both caught.
refuse_alias() {
  local live_abs="$1" omo_abs="$2" state_src="$3"
  if path_within "$RECV" "$live_abs"; then
    printf 'FAIL receipt dir is inside the live folder: %s\n' "$RECV" >&2
    exit 2
  fi
  if path_within "$live_abs" "$RECV"; then
    printf 'FAIL live folder is inside the receipt dir: %s\n' "$RECV" >&2
    exit 2
  fi
  if path_within "$RECV" "$omo_abs"; then
    printf 'FAIL receipt dir is inside the live .omosense dir: %s\n' "$RECV" >&2
    exit 2
  fi
  if path_within "$omo_abs" "$RECV"; then
    printf 'FAIL live .omosense dir is inside the receipt dir: %s\n' "$RECV" >&2
    exit 2
  fi
  if path_within "$RECV" "$state_src"; then
    printf 'FAIL receipt dir is inside the live state dir: %s\n' "$RECV" >&2
    exit 2
  fi
  if path_within "$state_src" "$RECV"; then
    printf 'FAIL live state dir is inside the receipt dir: %s\n' "$RECV" >&2
    exit 2
  fi
}

# resolve_live LIVE -> stdout the resolved live folder, or exit 1 when it
# cannot be resolved (the state dir was already checked by the caller).
resolve_live() {
  local out
  if ! out="$(cd "$1" && pwd -P)"; then
    printf 'FAIL cannot resolve live folder: %s\n' "$1" >&2
    exit 1
  fi
  printf '%s\n' "$out"
}

# extract_pid LOCK -> the pid field of a state lock file, or fail. Locks are
# {"pid":N,...} (the shape internal/stop reads); only the first match is used.
extract_pid() {
  local p=""
  p="$(sed -n 's/.*"pid"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$1" 2>/dev/null)" || p=""
  p="${p%%$'\n'*}"
  [ -n "$p" ] || return 1
  printf '%s\n' "$p"
}

# lock_pids LIVE -> stdout "<lock-basename> <pid> <lstart or ->" sorted.
lock_pids() {
  local state="$1/.omosense/state" f pid lstart
  [ -d "$state" ] || {
    printf 'FAIL no state dir: %s\n' "$state"
    exit 1
  }
  for f in "$state"/*.lock.json; do
    [ -e "$f" ] || continue
    if pid="$(extract_pid "$f")"; then
      if lstart="$(ps -o lstart= -p "$pid" 2>/dev/null)"; then
        :
      else
        lstart="-"
      fi
    else
      pid="-"
      lstart="-"
    fi
    printf '%s %s %s\n' "$(basename "$f")" "$pid" "$lstart"
  done | sort
}

# binary_hashes LIVE -> stdout "sha256  argv0" per unique live binary, sorted.
binary_hashes() {
  local state="$1/.omosense/state" f pid bin out
  [ -d "$state" ] || {
    printf 'FAIL no state dir: %s\n' "$state"
    exit 1
  }
  for f in "$state"/*.lock.json; do
    [ -e "$f" ] || continue
    if pid="$(extract_pid "$f")"; then
      if bin="$(ps -o comm= -p "$pid" 2>/dev/null)" && [ -n "$bin" ]; then
        printf '%s\n' "$bin"
      fi
    fi
  done | sort -u | while IFS= read -r bin; do
    if out="$(shasum -a 256 "$bin" 2>/dev/null)"; then
      printf '%s\n' "$out"
    else
      printf 'unreadable %s\n' "$bin"
    fi
  done | sort
}

cmd_before() {
  local live="$1" dir="$2" state="$1/.omosense/state" f live_abs omo_abs state_src resolved
  [ -d "$state" ] || {
    printf 'FAIL no state dir: %s\n' "$state" >&2
    exit 1
  }
  # Validate the receipt path (creates nothing) before any write, then resolve
  # the live tree (folder, .omosense dir, state source - symlinks followed) and
  # refuse any receipt location that aliases it, then create the receipt dir if
  # it does not exist. The dir may pre-exist, but every receipt target below
  # must be absent (see the loop).
  resolve_receipt_dir "$dir"
  live_abs="$(resolve_live "$live")"
  omo_abs="$(resolve_omo_dir "$live")"
  state_src="$(resolve_state_src "$live")"
  refuse_alias "$live_abs" "$omo_abs" "$state_src"
  if [ -L "$RECV" ]; then
    printf 'FAIL receipt dir is a symlink: %s\n' "$RECV" >&2
    exit 2
  fi
  if [ -e "$RECV" ] && [ ! -d "$RECV" ]; then
    printf 'FAIL receipt dir is not a directory: %s\n' "$RECV" >&2
    exit 2
  fi
  mkdir -p "$RECV"
  # Re-resolve what was actually created and re-check confinement.
  if ! resolved="$(cd "$RECV" && pwd -P)"; then
    printf 'FAIL cannot resolve created receipt dir: %s\n' "$RECV" >&2
    exit 2
  fi
  case "$resolved" in
    /tmp | /tmp/* | /private/tmp | /private/tmp/*) ;;
    *)
      printf 'FAIL receipt dir resolves outside /tmp: %s -> %s\n' "$RECV" "$resolved" >&2
      exit 2
      ;;
  esac
  RECV="$resolved"
  # Refuse every pre-existing receipt target, symlink or not: a dangling
  # symlink here is exactly the shape that redirected a write into the live
  # input before this fix. This runs before any write.
  for f in state-copy state.sha256 pids.txt binary.sha256 meta.txt; do
    if [ -L "$RECV/$f" ] || [ -e "$RECV/$f" ]; then
      printf 'FAIL receipt target already exists: %s/%s\n' "$RECV" "$f" >&2
      exit 1
    fi
  done
  # noclobber is the second line of defence behind the [ -L ]/[ -e ] loop.
  set -o noclobber

  # Snapshot the live state (receipt only; never compared, never written back).
  # Copy FROM the resolved state source, not the live-relative spelling.
  mkdir -p "$RECV/state-copy"
  (
    cd "$state_src" || exit 1
    find . -type f | sort | while IFS= read -r f; do
      mkdir -p "$RECV/state-copy/$(dirname "$f")"
      cp -p "$f" "$RECV/state-copy/$f"
    done
  )
  (
    cd "$RECV/state-copy" || exit 1
    find . -type f | sort | while IFS= read -r f; do
      shasum -a 256 "$f"
    done
  ) >"$RECV/state.sha256"
  lock_pids "$live" >"$RECV/pids.txt"
  binary_hashes "$live" >"$RECV/binary.sha256"
  {
    printf 'live_folder: %s\n' "$live"
    printf 'recorded_at: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'host: %s\n' "$(hostname)"
  } >"$RECV/meta.txt"
  set +o noclobber
  printf 'RECEIPT %s pids=%s state_files=%s binaries=%s\n' \
    "$RECV" \
    "$(wc -l <"$RECV/pids.txt" | tr -d '[:space:]')" \
    "$(wc -l <"$RECV/state.sha256" | tr -d '[:space:]')" \
    "$(wc -l <"$RECV/binary.sha256" | tr -d '[:space:]')"
}

cmd_after() {
  local live="$1" dir="$2" f live_abs omo_abs state_src rc=0
  # Same path validation and alias refusal as `before`, but read-only: this
  # mode writes nothing at all (no scratch dir), so no receipt-relative path
  # can escape.
  resolve_receipt_dir "$dir"
  live_abs="$(resolve_live "$live")"
  omo_abs="$(resolve_omo_dir "$live")"
  state_src="$(resolve_state_src "$live")"
  refuse_alias "$live_abs" "$omo_abs" "$state_src"
  if [ ! -d "$RECV" ]; then
    printf 'FAIL missing receipt dir: %s\n' "$RECV" >&2
    exit 1
  fi
  for f in pids.txt binary.sha256 state.sha256; do
    if [ -L "$RECV/$f" ]; then
      printf 'FAIL receipt file is a symlink: %s\n' "$RECV/$f" >&2
      exit 2
    fi
    if [ ! -f "$RECV/$f" ]; then
      printf 'FAIL missing receipt: %s\n' "$RECV/$f" >&2
      exit 1
    fi
  done
  if ! cmp -s "$RECV/pids.txt" <(lock_pids "$live"); then
    rc=1
    printf 'FAIL pids/start-times changed\n'
    diff "$RECV/pids.txt" <(lock_pids "$live") | sed 's/^/  /' || true
  fi
  if ! cmp -s "$RECV/binary.sha256" <(binary_hashes "$live"); then
    rc=1
    printf 'FAIL deployed binary changed\n'
    diff "$RECV/binary.sha256" <(binary_hashes "$live") | sed 's/^/  /' || true
  fi
  if [ "$rc" -eq 0 ]; then
    printf 'PASS pids+start-times+binary unchanged\n'
  fi
  exit "$rc"
}

cmd_selftest() {
  local T live SPID UNIQ REFUSE ESCAPE_TARGET DIRECT rc=0 out got
  local alias_live_a alias_target_a alias_live_b alias_ext_b alias_live_c alias_target_c
  T="$(mktemp -d /tmp/qa11-selftest.XXXXXX)"
  live="$T/live"
  UNIQ="${T##*/}"
  # Refusal inputs are HOME-independent on purpose: a receipt path built from
  # HOME is a legal /tmp receipt path whenever the mandated sandbox HOME itself
  # lives under /tmp. Both are unique to this run, so "created nothing" is a
  # real assertion rather than a stale-path accident.
  REFUSE="/var/empty/qa11-audit-refuse-$UNIQ"
  ESCAPE_TARGET="/var/empty/qa11-audit-escape-$UNIQ"
  # The helper runs under its own name (copying /bin/sleep elsewhere is
  # killed by macOS code signing); the audit accepts any binary. disown drops
  # it from the job table so bash never prints a Terminated notice.
  sleep 300 &
  SPID=$!
  disown
  # Kill only the helper this selftest started; nothing else is ever signaled.
  # The trap removes only the /tmp sandbox - never a path outside /tmp.
  trap 'kill "$SPID" 2>/dev/null || true; rm -rf "$T"' EXIT
  mkdir -p "$live/.omosense/state/inbox"
  printf '{"pid": %d, "session": "qa11-selftest"}\n' "$SPID" >"$live/.omosense/state/listen.lock.json"
  printf '{"pid": 999999999, "session": "dead"}\n' >"$live/.omosense/state/dead.lock.json"
  printf '{"v": 1}\n' >"$live/.omosense/state/reminders.json"
  printf 'msg-1\n' >"$live/.omosense/state/inbox/msg-1"

  # lane NAME WANT_EXIT WANT_SUBSTR -- CMD... : run CMD, expect WANT_EXIT and
  # (when non-empty) WANT_SUBSTR in the output; anything else fails the lane.
  lane() {
    local name="$1" want="$2" sub="$3"
    shift 3
    if [ "${1-}" = "--" ]; then shift; fi
    set +e
    out="$("$@" 2>&1)"
    got=$?
    set -e
    if [ "$got" -ne "$want" ]; then
      rc=1
      printf 'FAIL %s: exit %d want %d\n%s\n' "$name" "$got" "$want" "$out"
      return 0
    fi
    if [ -n "$sub" ] && ! printf '%s\n' "$out" | grep -qF -- "$sub"; then
      rc=1
      printf 'FAIL %s: output does not contain "%s"\n%s\n' "$name" "$sub" "$out"
      return 0
    fi
    printf 'ok   %s\n' "$name"
  }

  # absent WHAT PATH : the refusal must not have created PATH.
  absent() {
    local what="$1" p="$2"
    if [ -e "$p" ] || [ -L "$p" ]; then
      rc=1
      printf 'FAIL %s created %s\n' "$what" "$p"
    else
      printf 'ok   %s created nothing: %s\n' "$what" "$p"
    fi
  }

  # alias_lane NAME SRCDIR TARGETDIR -- CMD... : CMD must exit 2 and must not
  # change the entry set of SRCDIR or TARGETDIR (listed before and after), so a
  # refusal never writes into the state source it was aimed at.
  alias_lane() {
    local name="$1" srcdir="$2" targetdir="$3"
    shift 3
    if [ "${1-}" = "--" ]; then shift; fi
    local s_before t_before s_after t_after
    s_before="$(find "$srcdir" -mindepth 1 2>/dev/null | sort)"
    t_before="$(find "$targetdir" -mindepth 1 2>/dev/null | sort)"
    set +e
    out="$("$@" 2>&1)"
    got=$?
    set -e
    s_after="$(find "$srcdir" -mindepth 1 2>/dev/null | sort)"
    t_after="$(find "$targetdir" -mindepth 1 2>/dev/null | sort)"
    if [ "$got" -ne 2 ]; then
      rc=1
      printf 'FAIL %s: exit %d want 2\n%s\n' "$name" "$got" "$out"
      return 0
    fi
    if [ "$s_before" != "$s_after" ] || [ "$t_before" != "$t_after" ]; then
      rc=1
      printf 'FAIL %s: refusal changed the source or target\n-- source before\n%s\n-- source after\n%s\n-- target before\n%s\n-- target after\n%s\n' \
        "$name" "$s_before" "$s_after" "$t_before" "$t_after"
      return 0
    fi
    printf 'ok   %s (exit 2, source+target unchanged)\n' "$name"
  }

  lane "usage: no args exits 2" 2 "" -- "$0"
  lane "usage: unknown command exits 2" 2 "" -- "$0" frobnicate x y
  lane "usage: wrong arg count exits 2" 2 "" -- "$0" before "$live"

  # R1: the receipt path must be validated before anything is created, and no
  # refusal may leave a write behind - inside or outside /tmp.
  lane "refuse: receipt dir outside /tmp exits 2" 2 "FAIL" -- "$0" before "$live" "$REFUSE"
  absent "outside-/tmp refusal" "$REFUSE"

  ln -s "$T/never-created-target" "$T/dangling-receipt"
  lane "refuse: dangling symlink receipt dir exits 2" 2 "FAIL" -- "$0" before "$live" "$T/dangling-receipt"
  absent "dangling-symlink refusal" "$T/never-created-target"

  mkdir -p "$T/symlink-target"
  ln -s "$T/symlink-target" "$T/symlinked-receipt"
  lane "refuse: symlinked receipt dir exits 2" 2 "FAIL" -- "$0" before "$live" "$T/symlinked-receipt"
  if [ -n "$(ls -A "$T/symlink-target" 2>/dev/null)" ]; then
    rc=1
    printf 'FAIL symlinked-receipt refusal wrote into its target\n'
  else
    printf 'ok   symlinked-receipt refusal wrote nothing into its target\n'
  fi

  mkdir -p "$T/esc-parent"
  ln -s "$ESCAPE_TARGET" "$T/esc-parent/escape"
  lane "refuse: symlink component escaping /tmp exits 2" 2 "FAIL" \
    -- "$0" before "$live" "$T/esc-parent/escape/child"
  absent "escaping-symlink refusal" "$ESCAPE_TARGET"

  lane "refuse: receipt dir inside the live folder exits 2" 2 "FAIL" \
    -- "$0" before "$live" "$live/qa11-receipt-inside"
  absent "inside-live refusal" "$live/qa11-receipt-inside"

  lane "refuse: live folder inside the receipt dir exits 2" 2 "FAIL" -- "$0" before "$live" "$T"

  # Path-aliasing lanes: when <live>/.omosense or <live>/.omosense/state is a
  # symlink, the real state source is a directory the plain containment check
  # above never sees. Every side is resolved, so the receipt is refused (exit
  # 2, nothing created in the source or the target).
  alias_live_a="$T/alias-a-live"
  alias_target_a="$T/alias-a-target"
  mkdir -p "$alias_live_a/.omosense" "$alias_target_a"
  printf 'a\n' >"$alias_target_a/keep"
  ln -s "$alias_target_a" "$alias_live_a/.omosense/state"
  alias_lane "refuse: .omosense/state symlink to the receipt target" \
    "$alias_target_a" "$alias_target_a" -- "$0" before "$alias_live_a" "$alias_target_a"

  alias_live_b="$T/alias-b-live"
  alias_ext_b="$T/alias-b-ext"
  mkdir -p "$alias_live_b" "$alias_ext_b/state"
  printf 'b\n' >"$alias_ext_b/state/keep"
  ln -s "$alias_ext_b" "$alias_live_b/.omosense"
  alias_lane "refuse: .omosense symlink, receipt is its external state" \
    "$alias_ext_b/state" "$alias_ext_b/state" -- "$0" before "$alias_live_b" "$alias_ext_b/state"

  alias_live_c="$T/alias-c-live"
  alias_target_c="$T/alias-c-target"
  mkdir -p "$alias_live_c/.omosense" "$alias_target_c/inner"
  printf 'c\n' >"$alias_target_c/keep"
  ln -s "$alias_target_c" "$alias_live_c/.omosense/state"
  alias_lane "refuse: receipt inside state source via /tmp vs /private/tmp spelling" \
    "$alias_target_c" "$alias_target_c" -- "$0" before "$alias_live_c" "/tmp/$UNIQ/alias-c-target/inner"

  # The verifier's reported shape: an existing receipt dir whose meta.txt is a
  # dangling symlink into the live state. The target check refuses it and the
  # link target must stay absent.
  mkdir -p "$T/pre-receipt"
  ln -s "$live/.omosense/state/created-by-audit" "$T/pre-receipt/meta.txt"
  lane "refuse: dangling symlink receipt file exits 1" 1 "FAIL" \
    -- "$0" before "$live" "$T/pre-receipt"
  absent "dangling receipt-file refusal" "$live/.omosense/state/created-by-audit"

  # A symlink receipt file aimed at a real live state file must be refused too,
  # leaving the live file byte-identical.
  mkdir -p "$T/pre-receipt2"
  cp "$live/.omosense/state/reminders.json" "$T/reminders.keep"
  ln -s "$live/.omosense/state/reminders.json" "$T/pre-receipt2/meta.txt"
  lane "refuse: symlink receipt file aimed at live state exits 1" 1 "FAIL" \
    -- "$0" before "$live" "$T/pre-receipt2"
  if cmp -s "$T/reminders.keep" "$live/.omosense/state/reminders.json"; then
    printf 'ok   live state untouched through the symlinked receipt file\n'
  else
    rc=1
    printf 'FAIL live state was overwritten through a symlinked receipt file\n'
  fi

  lane "before: missing state dir fails" 1 "FAIL" -- "$0" before "$T/no-live" "$T/r0"
  # A not-yet-existing DIRECT child of /tmp is the shape the live audit uses
  # (/tmp/qa11-live-<name>); its deepest existing ancestor is /tmp itself.
  DIRECT="/tmp/qa11-selftest-direct-$UNIQ"
  if [ -e "$DIRECT" ] || [ -L "$DIRECT" ]; then
    rc=1
    printf 'FAIL pre-existing direct-child receipt dir %s\n' "$DIRECT"
  else
    lane "before: not-yet-existing direct /tmp child receipt dir" 0 "RECEIPT" \
      -- "$0" before "$live" "$DIRECT"
    rm -rf "$DIRECT"
  fi
  lane "before: takes receipt" 0 "RECEIPT" -- "$0" before "$live" "$T/receipt"
  lane "before: refuses to clobber receipt" 1 "FAIL" -- "$0" before "$live" "$T/receipt"

  if [ -s "$T/receipt/state.sha256" ] && [ -d "$T/receipt/state-copy" ] \
    && [ -f "$T/receipt/pids.txt" ] && [ -f "$T/receipt/binary.sha256" ] \
    && [ -f "$T/receipt/meta.txt" ] \
    && cmp -s "$live/.omosense/state/reminders.json" "$T/receipt/state-copy/reminders.json" \
    && cmp -s "$live/.omosense/state/inbox/msg-1" "$T/receipt/state-copy/inbox/msg-1"; then
    printf 'ok   receipt complete and snapshot copy identical\n'
  else
    rc=1
    printf 'FAIL receipt incomplete or snapshot copy differs\n'
  fi

  lane "after: PASS while untouched" 0 "PASS" -- "$0" after "$live" "$T/receipt"

  # after: a receipt file that is a symlink (dangling or not) is refused, so a
  # receipt dir cannot redirect the comparison at an arbitrary file.
  cp "$T/receipt/pids.txt" "$T/pids.real"
  rm "$T/receipt/pids.txt"
  ln -s "$T/pids.real" "$T/receipt/pids.txt"
  lane "after: refuses a symlinked receipt file" 2 "FAIL" -- "$0" after "$live" "$T/receipt"
  rm "$T/receipt/pids.txt"
  ln -s "$T/never-created-target" "$T/receipt/pids.txt"
  lane "after: refuses a dangling symlink receipt file" 2 "FAIL" -- "$0" after "$live" "$T/receipt"
  absent "dangling receipt-file refusal" "$T/never-created-target"
  rm "$T/receipt/pids.txt"
  mv "$T/pids.real" "$T/receipt/pids.txt"
  lane "after: PASS again once the symlink is gone" 0 "PASS" -- "$0" after "$live" "$T/receipt"

  # Tamper only the start-time field of the live lock line (lock + pid intact).
  cp "$T/receipt/pids.txt" "$T/pids.keep"
  awk '$2 == 999999999 { print; next } !done { print $1, $2, "TAMPERED-START"; done = 1; next } { print }' \
    "$T/pids.keep" >"$T/receipt/pids.txt"
  lane "after: FAIL on tampered start time" 1 "FAIL pids/start-times" -- "$0" after "$live" "$T/receipt"
  cp "$T/pids.keep" "$T/receipt/pids.txt"

  cp "$T/receipt/binary.sha256" "$T/binary.keep"
  awk 'NR == 1 { print "00" $0; next } { print }' "$T/binary.keep" >"$T/receipt/binary.sha256"
  lane "after: FAIL on tampered binary hash" 1 "FAIL deployed binary" -- "$0" after "$live" "$T/receipt"
  cp "$T/binary.keep" "$T/receipt/binary.sha256"

  mv "$T/receipt/pids.txt" "$T/pids.moved"
  lane "after: FAIL on missing receipt" 1 "missing receipt" -- "$0" after "$live" "$T/receipt"
  mv "$T/pids.moved" "$T/receipt/pids.txt"

  lane "after: PASS again after restore" 0 "PASS" -- "$0" after "$live" "$T/receipt"

  if [ "$rc" -eq 0 ]; then
    printf 'SELFTEST PASS\n'
  else
    printf 'SELFTEST FAIL\n'
  fi
  exit "$rc"
}

main() {
  if [ "$#" -lt 1 ]; then
    usage >&2
    exit 2
  fi
  case "$1" in
    before | after)
      if [ "$#" -ne 3 ]; then
        usage >&2
        exit 2
      fi
      ;;
    selftest)
      if [ "$#" -ne 1 ]; then
        usage >&2
        exit 2
      fi
      ;;
    -h | --help | help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
  case "$1" in
    before) cmd_before "$2" "$3" ;;
    after) cmd_after "$2" "$3" ;;
    selftest) cmd_selftest ;;
  esac
}

main "$@"
