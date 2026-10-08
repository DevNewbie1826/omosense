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
#         binary.sha256   sorted unique "sha256  <exe>" where <exe> is the
#                         cwd-independent absolute executable path of every
#                         live lock pid (lsof text entry, then Linux
#                         /proc/<pid>/exe, then an absolute `ps -o comm=`)
#         meta.txt        live folder, UTC timestamp, hostname (never compared)
#   scripts/qa11-live-audit.sh after <live-folder> <receipt-dir>
#       Recompute pids + start times + binary hash and compare them with the
#       before receipt: identical -> "PASS" exit 0; different -> "FAIL <what>"
#       exit 1. The state snapshot is never compared here.
#   scripts/qa11-live-audit.sh selftest
#       Run the built-in control: a fake live folder under /tmp with a real
#       helper process, PASS while untouched, FAIL on a tampered start time,
#       a tampered binary hash and a missing receipt, plus the fail-closed
#       lanes (unparsable lock, unreadable state dir, a relative-argv0 helper
#       whose on-disk binary is replaced, an unhashable executable).
#   scripts/qa11-live-audit.sh --help
#       This help.
#
# Measurement is fail-closed: every value that is compared (the lock pid
# list, the ps start times, the executable hash) must be really measured. A
# lock file that exists but carries no parsable pid, a live pid whose start
# time cannot be read, a live pid whose executable cannot be resolved to an
# absolute path or cannot be hashed, and a state dir that cannot be enumerated
# are all ERRORS: `before` exits 1 and writes no receipt at all (measurement
# runs before the receipt dir is created), and `after` exits 1 with a FAIL
# line. Two equal error markers are never compared as a PASS, and an empty
# measurement is accepted as "no live hosts" only when the enumeration itself
# succeeded - a pid that is simply not alive is a legitimate value, recorded
# as "-".
#
# Liveness never goes through an external tool: it comes from the shell's own
# `kill -0`, so no missing, broken or empty-output program can make a LIVE pid
# look dead. That shape - a `ps` that could not be executed reading as "no
# such process", the pid recorded as "-", its executable never hashed, and
# `after` comparing empty against empty for a PASS - is exactly the false PASS
# this file must not be able to produce. `kill -0` reports EPERM for a live
# pid that is not ours, which still counts as alive; only a clear "no such
# process" is "not alive", and any other outcome is an error. Every external
# tool whose output is compared (ps for the start time, the executable
# resolver, the hasher) has its exit status checked, and a tool that is
# present but fails is an error rather than a silent fall-through.
#
# Required tools are resolved once at startup (`command -v`): ps, a hasher
# (shasum or sha256sum), and an executable resolver - lsof on macOS, readlink
# for the Linux /proc/<pid>/exe link. A missing tool is exit 2 naming it,
# before anything is written. The executable is then resolved without an
# OS-name branch (lsof's text entry, then Linux /proc/<pid>/exe, then an
# absolute `ps -o comm=`), so a host launched as ./relative-host is still
# hashed by its real path.
#
# Exit codes: 0 ok / 1 audit FAIL or unreadable input / 2 usage, refusal or a
# missing required tool.
#
# Receipt dir rule: the dir itself may already exist (a fresh dir is the common
# case), but it must be a real, non-symlink directory under /tmp and every
# receipt target - state-copy, state.sha256, pids.txt, binary.sha256, meta.txt
# - must be absent, so an existing or dangling symlink target is refused before
# any write.
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
# the executable resolver, the hasher) and `after` writes nothing at all. The
# script never deletes anything
# and never signals any process (the selftest kills only the helper process it
# started itself).

set -euo pipefail
export LC_ALL=C

# Resolved once by require_tools(); every hash goes through hash_file() so the
# receipt and each re-measurement use the same program.
HASH_CMD=""

usage() {
  cat <<'EOF'
usage: scripts/qa11-live-audit.sh <command> [args]

  before <live-folder> <receipt-dir>   take the audit receipt (receipt-dir under /tmp)
  after <live-folder> <receipt-dir>    recompute + compare with the before receipt
  selftest                             run the built-in self-test
  --help                               this help

Pass criteria are the host pids, their ps start times and the deployed
binary hash only; the copied state is a snapshot receipt and is never
compared.

Measurement is fail-closed: a lock whose pid cannot be parsed, a live pid
whose start time cannot be read, a live pid whose executable cannot be
resolved to an absolute path or cannot be hashed, and a state dir that cannot
be enumerated each make `before` exit 1 and write no receipt at all, and make
`after` exit 1 with a FAIL line - an unmeasurable input never passes. An empty
lock set is a legitimate "no hosts" result only because the enumeration itself
succeeded; a pid that is not alive is a legitimate value recorded as "-".

Liveness comes from the shell's own `kill -0`, never from an external tool,
so no missing or broken program can make a live pid look dead (EPERM counts
as alive; only "no such process" is dead). Required tools are resolved at
startup - ps, a hasher (shasum or sha256sum), and lsof on macOS / readlink on
Linux - and a missing one is exit 2 naming it before anything is written.
Every tool whose output is compared has its exit status checked. The
executable is resolved without an OS-name branch (lsof text entry, then Linux
/proc/<pid>/exe, then an absolute `ps -o comm=`), so ./relative-host is hashed
by its real path.

The script reads the live folder, writes only inside a receipt dir confined
to /tmp, never signals any process. Exit codes: 0 ok, 1 audit FAIL or
unreadable input, 2 usage or an unsafe receipt dir. Receipt dir rule: the dir
itself may already exist (a fresh dir is the common case), but it must be a
real non-symlink directory under /tmp and every receipt target - state-copy,
state.sha256, pids.txt, binary.sha256, meta.txt - must be absent, so an
existing or dangling symlink target is refused before any write. A receipt
dir is refused when it is not confined to /tmp, contains a symlink component,
is itself a symlink or not a directory, sits inside the live folder, or
contains it, or resolves into or contains the live tree at any level - the
resolved live folder, its resolved .omosense dir, or its resolved state
source (symlinks followed, so a symlinked .omosense/state and a /tmp vs
/private/tmp spelling are both seen).
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
# A lock that exists but carries no parsable pid is an ERROR for every caller.
extract_pid() {
  local p=""
  p="$(sed -n 's/.*"pid"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$1" 2>/dev/null)" || p=""
  p="${p%%$'\n'*}"
  [ -n "$p" ] || return 1
  printf '%s\n' "$p"
}

# require_state_dir LIVE -> exit 1 when the state dir is missing or cannot be
# enumerated. An empty lock list is a legitimate "no hosts" result ONLY after
# this succeeds: an unreadable dir must never read as "no hosts".
require_state_dir() {
  local state="$1/.omosense/state"
  if [ ! -d "$state" ]; then
    printf 'FAIL no state dir: %s\n' "$state" >&2
    exit 1
  fi
  if [ ! -r "$state" ] || [ ! -x "$state" ]; then
    printf 'FAIL state dir is not enumerable: %s\n' "$state" >&2
    exit 1
  fi
}

# require_tools -> resolve the tools the audit needs and exit 2 (before any
# write) naming every one that is missing. ps supplies the start time, a
# hasher supplies the executable digest, and an executable resolver is needed
# per platform: lsof for the text entry on macOS, readlink for the Linux
# /proc/<pid>/exe link. Resolving here means a missing tool is a clear refusal
# instead of a mid-audit surprise whose output could be read as a value.
require_tools() {
  local miss="" os t
  if ! command -v ps >/dev/null 2>&1; then
    miss="$miss ps"
  fi
  if command -v shasum >/dev/null 2>&1; then
    HASH_CMD="shasum"
  elif command -v sha256sum >/dev/null 2>&1; then
    HASH_CMD="sha256sum"
  else
    miss="$miss shasum"
  fi
  os="$(uname -s 2>/dev/null || printf 'unknown')"
  if [ "$os" = "Linux" ]; then
    if ! command -v readlink >/dev/null 2>&1; then
      miss="$miss readlink"
    fi
  else
    if ! command -v lsof >/dev/null 2>&1; then
      miss="$miss lsof"
    fi
  fi
  if [ -n "$miss" ]; then
    for t in $miss; do
      printf 'FAIL missing required tool: %s\n' "$t" >&2
    done
    exit 2
  fi
}

# hash_file PATH -> stdout "<sha256>  <path>" in the shasum/sha256sum format,
# non-zero when the hasher fails. The hasher is resolved once at startup.
hash_file() {
  if [ "$HASH_CMD" = "shasum" ]; then
    shasum -a 256 "$1"
  else
    sha256sum "$1"
  fi
}

# pid_alive PID -> success when the pid exists. Liveness comes from the
# shell's own `kill -0`, never from an external program, so a missing or
# failing tool can no longer make a LIVE pid look dead. `kill -0` reports
# EPERM for a live pid that is not ours - still alive. Only a clear "no such
# process" is "not alive"; anything else is an ERROR the caller must not read
# as a value.
pid_alive() {
  local err
  if kill -0 "$1" 2>/dev/null; then
    return 0
  fi
  err="$(kill -0 "$1" 2>&1)" || true
  case "$err" in
    *"permitted"* | *"not owner"*) return 0 ;;
    *"No such process"*) return 1 ;;
  esac
  printf 'FAIL cannot determine liveness of pid %s: %s\n' "$1" "$err" >&2
  exit 1
}

# exe_path PID -> stdout the absolute, cwd-independent path of the process
# executable, or fail (non-zero, nothing printed). A resolver that is present
# is ALWAYS invoked and its exit status is ALWAYS checked: a present-but-failed
# tool is an error, never a silent fall-through to the next candidate and
# never a marker two receipts could compare as equal. Only a resolver that is
# not installed at all is skipped (that is absence, not failure). Candidates:
# lsof's text entry (macOS and Linux), the Linux /proc/<pid>/exe link, then an
# absolute `ps -o comm=` value that exists on disk. A RELATIVE `ps` value is
# never used - that was exactly the case that used to be stored as a marker
# and compared as equal.
exe_path() {
  local pid="$1" p
  if command -v lsof >/dev/null 2>&1; then
    if ! p="$(lsof -a -p "$pid" -d txt -Fn 2>/dev/null)"; then
      return 1
    fi
    p="$(printf '%s\n' "$p" | sed -n 's/^n//p')"
    p="${p%%$'\n'*}"
    if [ -n "$p" ]; then
      printf '%s\n' "$p"
      return 0
    fi
    return 1
  fi
  if [ -L "/proc/$pid/exe" ]; then
    if ! p="$(readlink "/proc/$pid/exe" 2>/dev/null)"; then
      return 1
    fi
    if [ -n "$p" ]; then
      printf '%s\n' "$p"
      return 0
    fi
    return 1
  fi
  if ! p="$(ps -o comm= -p "$pid" 2>/dev/null)"; then
    return 1
  fi
  case "$p" in
    /*)
      if [ -e "$p" ]; then
        printf '%s\n' "$p"
        return 0
      fi
      ;;
  esac
  return 1
}

# lock_pids LIVE -> stdout "<lock-basename> <pid> <lstart or ->" sorted.
# Fails closed: a lock without a parsable pid, and a live pid whose start time
# cannot be read, are errors. A pid that is not alive is a legitimate "-".
lock_pids() {
  local state="$1/.omosense/state" f pid lstart
  require_state_dir "$1"
  for f in "$state"/*.lock.json; do
    # The only entry to skip is the unexpanded pattern itself (no lock files).
    # A dangling symlink DOES match the pattern and is a lock we cannot parse,
    # so it must reach extract_pid and fail closed rather than be dropped.
    [ -e "$f" ] || [ -L "$f" ] || continue
    if ! pid="$(extract_pid "$f")"; then
      printf 'FAIL cannot parse pid from lock: %s\n' "$f" >&2
      exit 1
    fi
    if pid_alive "$pid"; then
      if ! lstart="$(ps -o lstart= -p "$pid" 2>/dev/null)" || [ -z "${lstart//[[:space:]]/}" ]; then
        printf 'FAIL cannot read start time of live pid %s: %s\n' "$pid" "$f" >&2
        exit 1
      fi
    else
      lstart="-"
    fi
    printf '%s %s %s\n' "$(basename "$f")" "$pid" "$lstart"
  done | sort
}

# binary_hashes LIVE -> stdout "sha256  <exe>" per unique live executable,
# sorted. Fails closed: a live pid whose executable cannot be resolved to an
# absolute path or cannot be hashed is an error - never an "unreadable <path>"
# marker that two receipts could compare as equal.
binary_hashes() {
  local state="$1/.omosense/state" f pid exe out
  require_state_dir "$1"
  for f in "$state"/*.lock.json; do
    [ -e "$f" ] || [ -L "$f" ] || continue
    if ! pid="$(extract_pid "$f")"; then
      printf 'FAIL cannot parse pid from lock: %s\n' "$f" >&2
      exit 1
    fi
    pid_alive "$pid" || continue
    if ! exe="$(exe_path "$pid")"; then
      printf 'FAIL cannot resolve executable of live pid %s: %s\n' "$pid" "$f" >&2
      exit 1
    fi
    if ! out="$(hash_file "$exe" 2>/dev/null)" || [ -z "$out" ]; then
      printf 'FAIL cannot hash executable %s of live pid %s: %s\n' "$exe" "$pid" "$f" >&2
      exit 1
    fi
    printf '%s\n' "$out"
  done | sort -u
}

# write_lines FILE TEXT -> write TEXT plus one trailing newline, or an empty
# file when TEXT is empty, so an empty measurement stays a 0-byte receipt.
write_lines() {
  local file="$1" text="${2-}"
  if [ -z "$text" ]; then
    : >"$file"
  else
    printf '%s\n' "$text" >"$file"
  fi
}

cmd_before() {
  local live="$1" dir="$2" f live_abs omo_abs state_src resolved pids_now bins_now
  require_state_dir "$live"
  # Validate the receipt path (creates nothing) before any write, then resolve
  # the live tree (folder, .omosense dir, state source - symlinks followed) and
  # refuse any receipt location that aliases it, then MEASURE. Measurement runs
  # before the receipt dir is created, so an input that cannot be measured
  # leaves no receipt behind at all. The dir may pre-exist, but every receipt
  # target below must be absent (see the loop).
  resolve_receipt_dir "$dir"
  live_abs="$(resolve_live "$live")"
  omo_abs="$(resolve_omo_dir "$live")"
  state_src="$(resolve_state_src "$live")"
  refuse_alias "$live_abs" "$omo_abs" "$state_src"
  pids_now="$(lock_pids "$live")"
  bins_now="$(binary_hashes "$live")"
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
      hash_file "$f" || exit 1
    done
  ) >"$RECV/state.sha256"
  write_lines "$RECV/pids.txt" "$pids_now"
  write_lines "$RECV/binary.sha256" "$bins_now"
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
  local pids_ref bins_ref pids_now bins_now
  require_state_dir "$live"
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
  # Measure first, then compare: a measurement that cannot be taken exits 1
  # here (via the functions' own fail-closed exits) instead of being compared
  # as an empty or marker value against the receipt.
  pids_ref="$(cat "$RECV/pids.txt")"
  bins_ref="$(cat "$RECV/binary.sha256")"
  pids_now="$(lock_pids "$live")"
  bins_now="$(binary_hashes "$live")"
  if [ "$pids_ref" != "$pids_now" ]; then
    rc=1
    printf 'FAIL pids/start-times changed\n'
    diff <(printf '%s\n' "$pids_ref") <(printf '%s\n' "$pids_now") | sed 's/^/  /' || true
  fi
  if [ "$bins_ref" != "$bins_now" ]; then
    rc=1
    printf 'FAIL deployed binary changed\n'
    diff <(printf '%s\n' "$bins_ref") <(printf '%s\n' "$bins_now") | sed 's/^/  /' || true
  fi
  if [ "$rc" -eq 0 ]; then
    printf 'PASS pids+start-times+binary unchanged\n'
  fi
  exit "$rc"
}

cmd_selftest() {
  local T live SPID UNIQ REFUSE ESCAPE_TARGET DIRECT rc=0 out got
  local alias_live_a alias_target_a alias_live_b alias_ext_b alias_live_c alias_target_c
  local SCRIPT REL_PID HELPER_DIR CC_BIN rel_ready rel_live rel_receipt want_sha got_line
  local MINBIN FAKEPS_FAIL FAKEPS_EMPTY deadonly_live t p
  local empty_live noread_live malformed_live mal2_live gone_live dangling_lock_live c
  T="$(mktemp -d /tmp/qa11-selftest.XXXXXX)"
  # Absolute script path: the new lanes run the audit from a DIFFERENT cwd, so
  # a relative "$0" would break.
  SCRIPT="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
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
  # Every helper this selftest starts is tracked here - global on purpose, since
  # the EXIT trap runs after this function has already returned.
  HELPER_PIDS="$SPID"
  # Kill only the helpers this selftest started; nothing else is ever signaled.
  # The trap removes only the /tmp sandbox - never a path outside /tmp.
  trap 'for p in $HELPER_PIDS; do kill "$p" 2>/dev/null || true; done; rm -rf "$T"' EXIT
  mkdir -p "$live/.omosense/state/inbox"
  printf '{"pid": %d, "session": "qa11-selftest"}\n' "$SPID" >"$live/.omosense/state/listen.lock.json"
  printf '{"pid": 999999999, "session": "dead"}\n' >"$live/.omosense/state/dead.lock.json"
  printf '{"v": 1}\n' >"$live/.omosense/state/reminders.json"
  printf 'msg-1\n' >"$live/.omosense/state/inbox/msg-1"

  # lane NAME WANT_EXIT WANT_SUBSTR [--cwd DIR] -- CMD... : run CMD, expect
  # WANT_EXIT and (when non-empty) WANT_SUBSTR in the output; anything else
  # fails the lane. With --cwd the command runs from DIR, which proves the
  # audit does not depend on the caller's working directory.
  lane() {
    local name="$1" want="$2" sub="$3" cwd=""
    shift 3
    if [ "${1-}" = "--cwd" ]; then
      cwd="$2"
      shift 2
    fi
    if [ "${1-}" = "--" ]; then shift; fi
    set +e
    if [ -n "$cwd" ]; then
      out="$( cd "$cwd" && "$@" 2>&1 )"
    else
      out="$("$@" 2>&1)"
    fi
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

  # --- S5-8 R1 class: every compared measurement is fail-closed. ---
  # A pid that is not alive is a legitimate value ("-"), and an empty lock set
  # is a legitimate "no hosts" result only because enumeration itself
  # succeeded - but a lock that cannot be parsed, a live pid whose start time
  # or executable cannot be read, and a state dir that cannot be enumerated
  # must fail the audit instead of being compared as an equal error marker.
  if grep -qF -- "dead.lock.json 999999999 -" "$T/receipt/pids.txt"; then
    printf 'ok   dead pid recorded as "-" (a legitimate not-alive value)\n'
  else
    rc=1
    printf 'FAIL dead pid is not recorded as a legitimate "-" value\n%s\n' "$(cat "$T/receipt/pids.txt")"
  fi

  empty_live="$T/empty-live"
  mkdir -p "$empty_live/.omosense/state"
  lane "empty state dir: before is a legitimate no-hosts receipt" 0 "RECEIPT" \
    -- "$SCRIPT" before "$empty_live" "$T/empty-receipt"
  lane "empty state dir: after PASSes (no hosts)" 0 "PASS" \
    -- "$SCRIPT" after "$empty_live" "$T/empty-receipt"

  noread_live="$T/noread-live"
  mkdir -p "$noread_live/.omosense/state"
  printf '{"pid": %d, "session": "noread"}\n' "$SPID" >"$noread_live/.omosense/state/listen.lock.json"
  chmod 000 "$noread_live/.omosense/state"
  lane "unreadable state dir: before fails closed" 1 "FAIL" \
    -- "$SCRIPT" before "$noread_live" "$T/noread-receipt"
  absent "unreadable-state-dir refusal" "$T/noread-receipt"
  chmod 755 "$noread_live/.omosense/state"

  malformed_live="$T/malformed-live"
  mkdir -p "$malformed_live/.omosense/state"
  printf 'not-json\n' >"$malformed_live/.omosense/state/listen.lock.json"
  lane "malformed lock: before fails closed" 1 "FAIL cannot parse pid" \
    -- "$SCRIPT" before "$malformed_live" "$T/malformed-receipt"
  absent "malformed-lock refusal" "$T/malformed-receipt"

  mal2_live="$T/mal2-live"
  mkdir -p "$mal2_live/.omosense/state"
  printf '{"pid": %d, "session": "mal2"}\n' "$SPID" >"$mal2_live/.omosense/state/listen.lock.json"
  lane "malformed lock: receipt taken while the lock is valid" 0 "RECEIPT" \
    -- "$SCRIPT" before "$mal2_live" "$T/mal2-receipt"
  printf 'not-json\n' >"$mal2_live/.omosense/state/listen.lock.json"
  lane "malformed lock: after fails closed" 1 "FAIL cannot parse pid" \
    -- "$SCRIPT" after "$mal2_live" "$T/mal2-receipt"

  # A dangling *.lock.json symlink matches the lock pattern, so it is an entry
  # the audit cannot parse - not something to drop silently.
  dangling_lock_live="$T/dangling-lock-live"
  mkdir -p "$dangling_lock_live/.omosense/state"
  ln -s "$dangling_lock_live/.omosense/state/missing-target" \
    "$dangling_lock_live/.omosense/state/listen.lock.json"
  lane "dangling symlink lock: before fails closed" 1 "FAIL cannot parse pid" \
    -- "$SCRIPT" before "$dangling_lock_live" "$T/dangling-lock-receipt"
  absent "dangling-lock refusal" "$T/dangling-lock-receipt"

  # (a) a process launched as ./relative-host from its own dir, audited from
  # another cwd: the receipt must hold the REAL digest of its resolved path,
  # and replacing the on-disk binary (pid and start time unchanged) must FAIL.
  CC_BIN=""
  for c in cc clang gcc; do
    if command -v "$c" >/dev/null 2>&1; then
      CC_BIN="$c"
      break
    fi
  done
  if [ -z "$CC_BIN" ]; then
    rc=1
    printf 'FAIL relative-argv0 lane: no C compiler (cc/clang/gcc) to build the fixture\n'
  else
    HELPER_DIR="$T/rel-bin"
    mkdir -p "$HELPER_DIR"
    cat >"$HELPER_DIR/helper.c" <<'CEOF'
#include <stdio.h>
#include <unistd.h>
int main(void) {
  printf("HELPER_READY %s\n", BUILD_LABEL);
  fflush(stdout);
  for (;;) pause();
  return 0;
}
CEOF
    "$CC_BIN" '-DBUILD_LABEL="A"' -o "$HELPER_DIR/relative-host" "$HELPER_DIR/helper.c"
    "$CC_BIN" '-DBUILD_LABEL="B"' -o "$HELPER_DIR/relative-host.next" "$HELPER_DIR/helper.c"
    # Subscribe to the helper's own readiness line before triggering the audit:
    # the FIFO open is the synchronisation and `-t 10` only bounds the wait, so
    # nothing is paced by a sleep and the exec is known to have happened.
    mkfifo "$T/rel.ready"
    exec 9<>"$T/rel.ready"
    ( cd "$HELPER_DIR" && exec ./relative-host ) >"$T/rel.ready" 2>&1 &
    REL_PID=$!
    disown
    HELPER_PIDS="$HELPER_PIDS $REL_PID"
    if ! IFS= read -r -t 10 -u 9 rel_ready; then
      rc=1
      printf 'FAIL relative-argv0 lane: helper never reported readiness\n'
    elif [ "${rel_ready#HELPER_READY}" = "$rel_ready" ]; then
      rc=1
      printf 'FAIL relative-argv0 lane: unexpected readiness line: %s\n' "$rel_ready"
    fi
    exec 9<&-
    rel_live="$T/rel-live"
    mkdir -p "$rel_live/.omosense/state"
    printf '{"pid": %d, "session": "relative"}\n' "$REL_PID" >"$rel_live/.omosense/state/listen.lock.json"
    rel_receipt="$T/rel-receipt"

    lane "relative-argv0: before measures a real digest from another cwd" 0 "RECEIPT" \
      --cwd "$T" -- "$SCRIPT" before "$rel_live" "$rel_receipt"

    want_sha="$(shasum -a 256 "$HELPER_DIR/relative-host" | awk '{print $1}')"
    got_line="$(cat "$rel_receipt/binary.sha256" 2>/dev/null || true)"
    case "$got_line" in
      "$want_sha  "/*)
        printf 'ok   relative-argv0: receipt holds the real sha256 of the resolved executable\n' ;;
      *)
        rc=1
        printf 'FAIL relative-argv0: receipt does not hold the real digest %s\n%s\n' "$want_sha" "$got_line" ;;
    esac
    case "$got_line" in
      *unreadable* | *./relative-host*)
        rc=1
        printf 'FAIL relative-argv0: receipt still carries an unreadable/relative marker\n%s\n' "$got_line" ;;
    esac

    mv "$HELPER_DIR/relative-host" "$HELPER_DIR/relative-host.running"
    mv "$HELPER_DIR/relative-host.next" "$HELPER_DIR/relative-host"
    lane "relative-argv0: after FAILs on a replaced binary" 1 "FAIL deployed binary" \
      --cwd "$T" -- "$SCRIPT" after "$rel_live" "$rel_receipt"
    mv "$HELPER_DIR/relative-host" "$HELPER_DIR/relative-host.next"
    mv "$HELPER_DIR/relative-host.running" "$HELPER_DIR/relative-host"
    lane "relative-argv0: after PASSes once the binary is restored" 0 "PASS" \
      --cwd "$T" -- "$SCRIPT" after "$rel_live" "$rel_receipt"

    # A content-only change at the SAME path must fail too: the receipt holds a
    # real digest of the resolved executable, not just its pathname.
    cp "$HELPER_DIR/relative-host" "$HELPER_DIR/relative-host.a"
    if ! cat "$HELPER_DIR/relative-host.next" >"$HELPER_DIR/relative-host"; then
      rc=1
      printf 'FAIL relative-argv0: could not overwrite the running binary in place\n'
    fi
    lane "relative-argv0: after FAILs on an in-place content change" 1 "FAIL deployed binary" \
      --cwd "$T" -- "$SCRIPT" after "$rel_live" "$rel_receipt"
    if ! cat "$HELPER_DIR/relative-host.a" >"$HELPER_DIR/relative-host"; then
      rc=1
      printf 'FAIL relative-argv0: could not restore the running binary in place\n'
    fi
    lane "relative-argv0: after PASSes once the content is restored" 0 "PASS" \
      --cwd "$T" -- "$SCRIPT" after "$rel_live" "$rel_receipt"

    # (b) a live pid whose executable cannot be measured: deleting the running
    # binary leaves the process alive but unhashable - never a PASS.
    gone_live="$T/gone-live"
    mkdir -p "$gone_live/.omosense/state"
    printf '{"pid": %d, "session": "gone"}\n' "$REL_PID" >"$gone_live/.omosense/state/listen.lock.json"
    rm "$HELPER_DIR/relative-host"
    lane "unhashable executable: before fails closed" 1 "FAIL" \
      -- "$SCRIPT" before "$gone_live" "$T/gone-receipt"
    absent "unhashable-executable refusal" "$T/gone-receipt"
  fi

  # --- S5-8 R1 class, round 2: liveness must not be able to fail silently. ---
  # The v-pr2d residual blocker: with a `ps` that could not be executed the
  # liveness probe read as "not alive", a LIVE pid was recorded with the
  # dead-pid marker "-", its executable was never hashed, and `after` compared
  # empty==empty and "-"=="-" for a PASS. Liveness now comes from the shell's
  # own `kill -0`, the required tools are resolved before anything is written,
  # and every tool whose output is compared has its exit status checked.
  MINBIN="$T/minbin"
  mkdir -p "$MINBIN"
  # Everything the audit needs EXCEPT ps, so `ps` is the only missing tool and
  # the refusal message can be pinned exactly.
  for t in bash sed sort shasum lsof uname basename cat cp date diff find hostname mkdir tr wc; do
    p="$(command -v "$t" 2>/dev/null || true)"
    if [ -n "$p" ]; then ln -s "$p" "$MINBIN/$t"; fi
  done
  if [ -e "$MINBIN/ps" ] || [ -L "$MINBIN/ps" ]; then
    rc=1
    printf 'FAIL tools lane: the minimal PATH unexpectedly contains ps\n'
  fi
  lane "tools: PATH without ps refuses (exit 2) before any write" 2 "FAIL missing required tool: ps" \
    -- env PATH="$MINBIN" "$SCRIPT" before "$live" "$T/nops-receipt"
  absent "no-ps refusal" "$T/nops-receipt"
  lane "tools: PATH without ps never PASSes on after" 2 "FAIL missing required tool: ps" \
    -- env PATH="$MINBIN" "$SCRIPT" after "$live" "$T/receipt"

  # A `ps` that is present but non-functional: exit 1 for every call.
  FAKEPS_FAIL="$T/fakebin-fail"
  mkdir -p "$FAKEPS_FAIL"
  printf '#!/bin/sh\nexit 1\n' >"$FAKEPS_FAIL/ps"
  chmod +x "$FAKEPS_FAIL/ps"
  lane "tools: a ps that exits 1 never PASSes (before)" 1 "FAIL cannot read start time" \
    -- env PATH="$FAKEPS_FAIL:$PATH" "$SCRIPT" before "$live" "$T/failps-receipt"
  absent "failing-ps refusal" "$T/failps-receipt"
  lane "tools: a ps that exits 1 never PASSes (after)" 1 "FAIL cannot read start time" \
    -- env PATH="$FAKEPS_FAIL:$PATH" "$SCRIPT" after "$live" "$T/receipt"

  # A `ps` that succeeds and prints nothing: empty output is not a value.
  FAKEPS_EMPTY="$T/fakebin-empty"
  mkdir -p "$FAKEPS_EMPTY"
  printf '#!/bin/sh\nexit 0\n' >"$FAKEPS_EMPTY/ps"
  chmod +x "$FAKEPS_EMPTY/ps"
  lane "tools: a ps that prints nothing never PASSes (before)" 1 "FAIL cannot read start time" \
    -- env PATH="$FAKEPS_EMPTY:$PATH" "$SCRIPT" before "$live" "$T/emptyps-receipt"
  absent "empty-ps refusal" "$T/emptyps-receipt"
  lane "tools: a ps that prints nothing never PASSes (after)" 1 "FAIL cannot read start time" \
    -- env PATH="$FAKEPS_EMPTY:$PATH" "$SCRIPT" after "$live" "$T/receipt"

  # Control: liveness is decided by kill -0, so a dead pid needs no ps at all.
  # With a ps that always fails, a dead-only lock set is still a legitimate
  # no-hosts receipt - ps is not on the liveness path.
  deadonly_live="$T/deadonly-live"
  mkdir -p "$deadonly_live/.omosense/state"
  printf '{"pid": 999999999, "session": "dead"}\n' >"$deadonly_live/.omosense/state/dead.lock.json"
  lane "liveness: a dead pid needs no ps (kill -0 decides)" 0 "RECEIPT" \
    -- env PATH="$FAKEPS_FAIL:$PATH" "$SCRIPT" before "$deadonly_live" "$T/deadonly-receipt"
  lane "liveness: dead-only after PASSes with a broken ps" 0 "PASS" \
    -- env PATH="$FAKEPS_FAIL:$PATH" "$SCRIPT" after "$deadonly_live" "$T/deadonly-receipt"

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
  # Resolve the required tools before any command runs: a missing tool is a
  # clear exit-2 refusal here, never a mid-audit failure whose output could be
  # read as a measured value.
  case "$1" in
    before | after | selftest) require_tools ;;
  esac
  case "$1" in
    before) cmd_before "$2" "$3" ;;
    after) cmd_after "$2" "$3" ;;
    selftest) cmd_selftest ;;
  esac
}

main "$@"
