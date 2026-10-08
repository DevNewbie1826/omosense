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
#         state.sha256    count=N, then "<sha256><TAB>./relative/path" of the copy
#         pids.txt        count=N, then "<lock> <pid> <ps lstart>" for every
#                         <live-folder>/.omosense/state/*.lock.json
#                         ("-" when the pid is not alive)
#         binary.sha256   count=N, then "<pid><TAB><sha256><TAB><exe>" per
#                         distinct live pid, where <exe> is the
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
# Every receipt has an explicit count=N header; every data record is validated
# for shape, identity and count both on creation and on readback. Zero hosts is
# count=0, never an empty file. Compared data and snapshot enumeration use
# shell builtins, not sort/awk pipelines. Older uncounted receipts are rejected;
# take a fresh before receipt with this version.
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

# The receipt grammar is also used on readback. Counts are shell integers,
# never values derived from a lossy external pipeline.
valid_pid() { [[ "$1" =~ ^[1-9][0-9]*$ ]]; }
valid_digest() { [[ "$1" =~ ^[0-9a-f]{64}$ ]]; }
valid_text() { [[ -n "$1" && "$1" != *$'\n'* && "$1" != *$'\r'* && "$1" != *$'\t'* ]]; }
valid_exe() { valid_text "$1" && [[ "$1" = /* ]] && [ -f "$1" ]; }
valid_start() {
  local re='^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ( [1-9]|[12][0-9]|3[01]) ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9] [1-9][0-9]{3}$'
  [[ "$1" =~ $re ]]
}
valid_timestamp() {
  local re='^[1-9][0-9]{3}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z$'
  [[ "$1" =~ $re ]]
}

fail_measure() {
  printf 'FAIL %s\n' "$*" >&2
  exit 1
}

# Keep the terminal newline until its count is checked. Plain command
# substitution would silently erase trailing blank records.
one_line() {
  local out
  out="$("$@" && printf '\001')" || return 1
  out="${out%$'\001'}"
  [[ "$out" = *$'\n' ]] || return 1
  out="${out%$'\n'}"
  valid_text "$out" || return 1
  printf '%s\n' "$out"
}

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

Each receipt starts with count=N. Digests are exactly 64 lowercase hex digits,
pids are positive decimal integers, start times have the ps lstart shape, and
executable paths are absolute existing files. Counts and live-pid coverage are
checked before storing or comparing. Blank, malformed, truncated, reordered or
uncounted receipts fail. A genuinely empty measurement is count=0.

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
# {"pid":N,...} (the shape internal/stop reads); duplicate pid fields are invalid.
# A lock that exists but carries no parsable pid is an ERROR for every caller.
extract_pid() {
  local text line p rest re='"pid"[[:space:]]*:[[:space:]]*([1-9][0-9]*)[[:space:]]*[,}]'
  [ -f "$1" ] && [ -r "$1" ] || return 1
  text=""
  while IFS= read -r line || [ -n "$line" ]; do text="$text$line"; done <"$1"
  [[ "$text" =~ ^[[:space:]]*\{.*\}[[:space:]]*$ && "$text" =~ $re ]] || return 1
  p="${BASH_REMATCH[1]}"
  rest="${text#*\"pid\"}"
  [[ "$rest" != *'"pid"'* ]] || return 1
  valid_pid "$p" || return 1
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
  os="$(one_line uname -s 2>/dev/null)" || fail_measure "cannot measure platform"
  [[ "$os" = Darwin || "$os" = Linux ]] || fail_measure "invalid platform: $os"
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

# hash_file PATH -> exactly one validated lowercase sha256, non-zero when the
# hasher fails or returns anything except one correctly shaped digest/path row.
hash_file() {
  local out digest re='^([0-9a-f]{64})  (.*)$'
  valid_text "$1" || return 1
  if [ "$HASH_CMD" = "shasum" ]; then
    out="$(one_line shasum -a 256 "$1")" || return 1
  else
    out="$(one_line sha256sum "$1")" || return 1
  fi
  [[ "$out" =~ $re ]] || return 1
  digest="${BASH_REMATCH[1]}"
  [[ "${BASH_REMATCH[2]}" = "$1" ]] && valid_digest "$digest" || return 1
  printf '%s\n' "$digest"
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
  local pid="$1" p line first="" comm seen=0 want=file
  if command -v lsof >/dev/null 2>&1; then
    if ! p="$(lsof -a -p "$pid" -d txt -Fn 2>/dev/null && printf '\001')"; then
      return 1
    fi
    p="${p%$'\001'}"
    [[ "$p" = *$'\n' ]] || return 1
    p="${p%$'\n'}"
    # Exactly one matching process header followed by ftxt/n-path pairs.
    while IFS= read -r line; do
      if [ "$seen" -eq 0 ]; then
        [[ "$line" = "p$pid" ]] || return 1
        seen=1
      elif [ "$want" = file ]; then
        [[ "$line" = ftxt ]] || return 1
        want=path
      else
        [[ "$line" = n/* ]] || return 1
        # lsof also lists mapped data/library files which may already be
        # unlinked. They are not executable measurements; validate framing
        # here, and require existence only for the selected executable below.
        valid_text "${line#n}" || return 1
        if [ -z "$first" ]; then first="${line#n}"; fi
        want="file"
      fi
    done <<<"$p"
    if [[ -z "$first" || "$want" != file ]] || ! valid_exe "$first"; then return 1; fi
    comm="$(one_line ps -o comm= -p "$pid" 2>/dev/null)" || return 1
    valid_text "$comm" || return 1
    # A reordered library entry must not be mistaken for the executable.
    [[ "${first##*/}" = "${comm##*/}" ]] || return 1
    printf '%s\n' "$first"
    return 0
  fi
  if [ -L "/proc/$pid/exe" ]; then
    if ! p="$(one_line readlink "/proc/$pid/exe" 2>/dev/null)"; then
      return 1
    fi
    if valid_exe "$p"; then
      printf '%s\n' "$p"
      return 0
    fi
    return 1
  fi
  if ! p="$(one_line ps -o comm= -p "$pid" 2>/dev/null)"; then
    return 1
  fi
  case "$p" in
    /*)
      if valid_exe "$p"; then
        printf '%s\n' "$p"
        return 0
      fi
      ;;
  esac
  return 1
}

# One lock enumeration supplies BOTH receipts. Bash glob order is stable under
# LC_ALL=C; no sort/awk/sed can erase or reorder the compared measurement set.
# Each lock has one pid row; each distinct live pid has one binary row.
measure() {
  local state="$1/.omosense/state" f name pid lstart exe digest lock_count=0 live_count=0
  local seen=" " pid_rows="" bin_rows="" locks=()
  require_state_dir "$1"
  for f in "$state"/*.lock.json; do
    [ -e "$f" ] || [ -L "$f" ] || continue
    locks[${#locks[@]}]="$f"
  done
  for f in ${locks[@]+"${locks[@]}"}; do
    name="${f##*/}"
    valid_text "$name" && [[ "$name" != *' '* ]] || fail_measure "invalid lock name: $f"
    pid="$(extract_pid "$f")" || fail_measure "cannot parse pid from lock: $f"
    if pid_alive "$pid"; then
      lstart="$(one_line ps -o lstart= -p "$pid" 2>/dev/null)" || fail_measure "cannot read start time of live pid $pid: $f"
      # ps pads its single value; trim only the exterior, then validate shape.
      lstart="${lstart#"${lstart%%[![:space:]]*}"}"
      lstart="${lstart%"${lstart##*[![:space:]]}"}"
      valid_start "$lstart" || fail_measure "cannot read start time of live pid $pid: $f"
      if [[ "$seen" != *" $pid "* ]]; then
        if ! exe="$(exe_path "$pid")" || ! valid_exe "$exe"; then
          fail_measure "deployed binary cannot resolve executable of live pid $pid: $f"
        fi
        if ! digest="$(hash_file "$exe" 2>/dev/null)" || ! valid_digest "$digest"; then
          fail_measure "cannot hash executable $exe of live pid $pid: $f"
        fi
        bin_rows="$bin_rows$pid"$'\t'"$digest"$'\t'"$exe"$'\n'
        seen="$seen$pid "
        live_count=$((live_count + 1))
      fi
    else
      lstart="-"
    fi
    pid_rows="$pid_rows$name $pid $lstart"$'\n'
    lock_count=$((lock_count + 1))
  done
  [ "$lock_count" -eq "${#locks[@]}" ] || fail_measure "lock measurement count mismatch"
  PIDS_NOW="count=$lock_count"$'\n'"$pid_rows"
  BINS_NOW="count=$live_count"$'\n'"$bin_rows"
  read_receipt pids/start-times <(printf '%s' "$PIDS_NOW")
  read_receipt "deployed binary" <(printf '%s' "$BINS_NOW")
}

# Recursive builtin enumeration replaces find|sort for the snapshot too.
# Symlinks are not followed, just as with find -type f.
state_files() {
  local dir="$1" f
  [ -r "$dir" ] && [ -x "$dir" ] || fail_measure "cannot enumerate snapshot directory: $dir"
  for f in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do
    [ -e "$f" ] || [ -L "$f" ] || continue
    valid_text "$f" || fail_measure "invalid snapshot path: $f"
    [ ! -L "$f" ] || continue
    if [ -d "$f" ]; then
      state_files "$f"
    elif [ -f "$f" ]; then
      STATE_FILES[${#STATE_FILES[@]}]="$f"
    fi
  done
}

# Read the original bytes line by line (not cat in a substitution, which loses
# trailing blank lines). Rebuild only after every record and count validates.
# For pids, also derive the distinct live-pid set used by the binary validator.
read_receipt() {
  local kind="$1" file="$2" line count n=0 key pid value prev="" seen=" "
  local re text="" expected_live="${LIVE_PIDS- }" expected_paths="" f
  if [ "$kind" = "pids/start-times" ]; then LIVE_PIDS=" "; fi
  {
    IFS= read -r line || fail_measure "$kind malformed receipt header"
    re='^count=(0|[1-9][0-9]*)$'
    [[ "$line" =~ $re ]] || fail_measure "$kind malformed receipt header"
    count="${BASH_REMATCH[1]}"
    # Avoid arithmetic on unbounded/untrusted numbers. Compare decimal strings.
    text="$line"$'\n'
    while IFS= read -r line; do
      [ -n "$line" ] || fail_measure "$kind blank receipt record"
      case "$kind" in
        pids/start-times)
          re='^([^[:space:]]+\.lock\.json) ([1-9][0-9]*) (.*)$'
          [[ "$line" =~ $re ]] || fail_measure "$kind malformed receipt record"
          key="${BASH_REMATCH[1]}"; pid="${BASH_REMATCH[2]}"; value="${BASH_REMATCH[3]}"
          valid_pid "$pid" || fail_measure "$kind invalid pid"
          [[ -z "$prev" || "$prev" < "$key" ]] || fail_measure "$kind reordered or duplicate lock"
          prev="$key"
          if [ "$value" != "-" ]; then
            valid_start "$value" || fail_measure "$kind invalid start time"
            if [[ "$LIVE_PIDS" != *" $pid "* ]]; then LIVE_PIDS="$LIVE_PIDS$pid "; fi
          fi
          ;;
        "deployed binary")
          re=$'^([1-9][0-9]*)\t([0-9a-f]{64})\t(.+)$'
          [[ "$line" =~ $re ]] || fail_measure "$kind malformed receipt record"
          key="${BASH_REMATCH[1]}"; value="${BASH_REMATCH[2]}"; pid="${BASH_REMATCH[3]}"
          if ! valid_pid "$key" || ! valid_digest "$value" || ! valid_exe "$pid"; then
            fail_measure "$kind invalid field"
          fi
          [[ "$seen" != *" $key "* && "$expected_live" = *" $key "* ]] || fail_measure "$kind duplicate or unexpected pid"
          seen="$seen$key "
          ;;
        snapshot)
          re=$'^([0-9a-f]{64})\t(\\./.+)$'
          [[ "$line" =~ $re ]] || fail_measure "$kind malformed receipt record"
          value="${BASH_REMATCH[1]}"; key="${BASH_REMATCH[2]}"
          if ! valid_digest "$value" || ! valid_text "$key"; then fail_measure "$kind invalid field"; fi
          [[ "$key" != *'/../'* && "$key" != *'/./'* && "$seen" != *$'\t'"$key"$'\t'* ]] || fail_measure "$kind invalid or duplicate path"
          [ -f "$RECV/state-copy/$key" ] || fail_measure "$kind missing copied file"
          seen="$seen"$'\t'"$key"$'\t'
          ;;
        metadata)
          case "$n" in
            0)
              if [[ "$line" != 'live_folder: '* ]] || ! valid_text "${line#live_folder: }"; then
                fail_measure "$kind invalid live folder"
              fi
              ;;
            1)
              if [[ "$line" != 'recorded_at: '* ]] || ! valid_timestamp "${line#recorded_at: }"; then
                fail_measure "$kind invalid timestamp"
              fi
              ;;
            2)
              re='^host: [A-Za-z0-9][A-Za-z0-9._-]*$'
              [[ "$line" =~ $re ]] || fail_measure "$kind invalid hostname"
              ;;
            *) fail_measure "$kind excess record" ;;
          esac
          ;;
      esac
      text="$text$line"$'\n'
      n=$((n + 1))
    done
    [ -z "$line" ] || fail_measure "$kind truncated receipt record"
  } <"$file"
  [ "$n" = "$count" ] || fail_measure "$kind receipt count mismatch"
  if [ "$kind" = "deployed binary" ]; then
    [ "$seen" = "$expected_live" ] || fail_measure "$kind live pid coverage or order mismatch"
  elif [ "$kind" = snapshot ]; then
    STATE_FILES=()
    state_files "$RECV/state-copy"
    [ "$n" -eq "${#STATE_FILES[@]}" ] || fail_measure "$kind file coverage count mismatch"
    expected_paths=" "
    for f in ${STATE_FILES[@]+"${STATE_FILES[@]}"}; do
      expected_paths="$expected_paths"$'\t'"./${f#"$RECV/state-copy/"}"$'\t'
    done
    [ "$seen" = "$expected_paths" ] || fail_measure "$kind path coverage or order mismatch"
  elif [ "$kind" = metadata ]; then
    [ "$count" = 3 ] || fail_measure "$kind record count mismatch"
  fi
  RECEIPT_TEXT="$text"
}

cmd_before() {
  local live="$1" dir="$2" f live_abs omo_abs state_src resolved rel digest snapshot_rows=""
  local recorded_at host pid_count bin_count
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
  measure "$live"
  STATE_FILES=()
  state_files "$state_src"
  valid_text "$live" || fail_measure "invalid live folder"
  recorded_at="$(one_line date -u +%Y-%m-%dT%H:%M:%SZ)" || fail_measure "cannot measure receipt time"
  valid_timestamp "$recorded_at" || fail_measure "invalid receipt time"
  host="$(one_line hostname)" || fail_measure "cannot measure hostname"
  [[ "$host" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || fail_measure "invalid hostname"
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
  for f in ${STATE_FILES[@]+"${STATE_FILES[@]}"}; do
    rel="./${f#"$state_src"/}"
    mkdir -p "${RECV}/state-copy/${rel%/*}"
    cp -p "$f" "$RECV/state-copy/$rel"
    digest="$(hash_file "$RECV/state-copy/$rel")" || fail_measure "cannot hash snapshot: $rel"
    snapshot_rows="$snapshot_rows$digest"$'\t'"$rel"$'\n'
  done
  printf 'count=%s\n%s' "${#STATE_FILES[@]}" "$snapshot_rows" >"$RECV/state.sha256"
  printf '%s' "$PIDS_NOW" >"$RECV/pids.txt"
  printf '%s' "$BINS_NOW" >"$RECV/binary.sha256"
  {
    printf 'count=3\n'
    printf 'live_folder: %s\n' "$live"
    printf 'recorded_at: %s\n' "$recorded_at"
    printf 'host: %s\n' "$host"
  } >"$RECV/meta.txt"
  set +o noclobber
  # Readback uses the very same grammar and coverage checks as `after`.
  read_receipt pids/start-times "$RECV/pids.txt"
  read_receipt "deployed binary" "$RECV/binary.sha256"
  read_receipt snapshot "$RECV/state.sha256"
  read_receipt metadata "$RECV/meta.txt"
  pid_count="${PIDS_NOW%%$'\n'*}"
  bin_count="${BINS_NOW%%$'\n'*}"
  printf 'RECEIPT %s pids=%s state_files=%s binaries=%s\n' \
    "$RECV" \
    "${pid_count#count=}" \
    "${#STATE_FILES[@]}" \
    "${bin_count#count=}"
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
  for f in pids.txt binary.sha256 state.sha256 meta.txt; do
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
  read_receipt pids/start-times "$RECV/pids.txt"
  pids_ref="$RECEIPT_TEXT"
  read_receipt "deployed binary" "$RECV/binary.sha256"
  bins_ref="$RECEIPT_TEXT"
  read_receipt snapshot "$RECV/state.sha256"
  read_receipt metadata "$RECV/meta.txt"
  measure "$live"
  pids_now="$PIDS_NOW"
  bins_now="$BINS_NOW"
  if [ "$pids_ref" != "$pids_now" ]; then
    rc=1
    printf 'FAIL pids/start-times changed\n'
    diff <(printf '%s' "$pids_ref") <(printf '%s' "$pids_now") || true
  fi
  if [ "$bins_ref" != "$bins_now" ]; then
    rc=1
    printf 'FAIL deployed binary changed\n'
    diff <(printf '%s' "$bins_ref") <(printf '%s' "$bins_now") || true
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
  local MINBIN FAKEPS_FAIL FAKEPS_EMPTY deadonly_live t p shim tag field header first rest LSOF_BIN
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
  # killed by macOS code signing); the audit accepts any binary.
  sleep 300 &
  SPID=$!
  # Track every helper so the EXIT trap kills AND reaps it before removing
  # the sandbox. wait is the completion signal, not a sleep/poll loop.
  HELPER_PIDS="$SPID"
  # Kill only the helpers this selftest started; nothing else is ever signaled.
  # The trap removes only the /tmp sandbox - never a path outside /tmp.
  trap 'for p in $HELPER_PIDS; do kill "$p" 2>/dev/null || true; wait "$p" 2>/dev/null || true; if kill -0 "$p" 2>/dev/null; then printf "FAIL cleanup: helper %s remains\n" "$p" >&2; exit 1; fi; done; rm -rf "$T"; printf "cleanup: helpers reaped; removed %s\n" "$T"' EXIT
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
  awk 'NR == 1 || $2 == 999999999 { print; next } !done { print $1, $2, "TAMPERED-START"; done = 1; next } { print }' \
    "$T/pids.keep" >"$T/receipt/pids.txt"
  lane "after: FAIL on tampered start time" 1 "FAIL pids/start-times" -- "$0" after "$live" "$T/receipt"
  cp "$T/pids.keep" "$T/receipt/pids.txt"

  cp "$T/receipt/binary.sha256" "$T/binary.keep"
  awk 'NR == 2 { print "00" $0; next } { print }' "$T/binary.keep" >"$T/receipt/binary.sha256"
  lane "after: FAIL on tampered binary hash" 1 "FAIL deployed binary" -- "$0" after "$live" "$T/receipt"
  cp "$T/binary.keep" "$T/receipt/binary.sha256"

  mv "$T/receipt/pids.txt" "$T/pids.moved"
  lane "after: FAIL on missing receipt" 1 "missing receipt" -- "$0" after "$live" "$T/receipt"
  mv "$T/pids.moved" "$T/receipt/pids.txt"

  lane "after: PASS again after restore" 0 "PASS" -- "$0" after "$live" "$T/receipt"

  # Stored receipts are untrusted input: line count, framing, field shape,
  # identity coverage and ordering all run through the real after comparator.
  for field in pids.txt binary.sha256 state.sha256 meta.txt; do
    cp "$T/receipt/$field" "$T/field.keep"
    printf '\n' >>"$T/receipt/$field"
    lane "receipt: $field trailing blank line is FAIL" 1 "FAIL" \
      -- "$SCRIPT" after "$live" "$T/receipt"
    cp "$T/field.keep" "$T/receipt/$field"
    # A dropped final record must not survive even if the other lines are valid.
    awk 'NR > 1 { if (last != "") print last } { last = $0 }' "$T/field.keep" >"$T/receipt/$field"
    lane "receipt: $field dropped record is FAIL" 1 "FAIL" \
      -- "$SCRIPT" after "$live" "$T/receipt"
    cp "$T/field.keep" "$T/receipt/$field"
  done
  {
    IFS= read -r header
    IFS= read -r first
    IFS= read -r rest
    printf '%s\n%s\n%s\n' "$header" "$rest" "$first"
  } <"$T/pids.keep" >"$T/receipt/pids.txt"
  lane "receipt: reordered pid records are FAIL" 1 "FAIL" -- "$SCRIPT" after "$live" "$T/receipt"
  cp "$T/pids.keep" "$T/receipt/pids.txt"
  awk 'NR == 1 { print "count=0"; next } { print }' "$T/binary.keep" >"$T/receipt/binary.sha256"
  lane "receipt: binary header cannot hide live records" 1 "FAIL" -- "$SCRIPT" after "$live" "$T/receipt"
  cp "$T/binary.keep" "$T/receipt/binary.sha256"
  awk -F '\t' 'BEGIN { OFS = "\t" } NR == 2 { $1 = 999999998 } { print }' \
    "$T/binary.keep" >"$T/receipt/binary.sha256"
  lane "receipt: binary pid must cover the live lock pid" 1 "FAIL" -- "$SCRIPT" after "$live" "$T/receipt"
  cp "$T/binary.keep" "$T/receipt/binary.sha256"
  lane "receipt: intact measured fields still PASS" 0 "PASS" -- "$SCRIPT" after "$live" "$T/receipt"

  # Successful but wrong output is a measurement failure, not a tool failure.
  for tag in whitespace short garbage multiline truncated wrongpath; do
    shim="$T/hash-$tag"
    mkdir -p "$shim"
    case "$tag" in
      whitespace) printf '#!/bin/sh\nprintf "   \\n"\n' >"$shim/shasum" ;;
      short) printf "#!/bin/sh\nprintf '%%063d  %%s\\n' 0 \"\$3\"\n" >"$shim/shasum" ;;
      garbage) printf "#!/bin/sh\nprintf 'not-a-digest  %%s\\n' \"\$3\"\n" >"$shim/shasum" ;;
      multiline) printf '#!/bin/sh\n/usr/bin/shasum "$@"\nprintf "\\n"\n' >"$shim/shasum" ;;
      truncated) printf "#!/bin/sh\nout=\$(/usr/bin/shasum \"\$@\"); printf '%%s' \"\$out\"\n" >"$shim/shasum" ;;
      wrongpath) printf '#!/bin/sh\nprintf "%%064d  /not-the-input\\n" 0\n' >"$shim/shasum" ;;
    esac
    chmod +x "$shim/shasum"
    lane "hash shape: $tag before fails closed" 1 "FAIL" \
      -- env PATH="$shim:$PATH" "$SCRIPT" before "$live" "$T/hash-receipt-$tag"
    absent "hash-$tag refusal" "$T/hash-receipt-$tag"
    lane "hash shape: $tag after never PASSes" 1 "FAIL" \
      -- env PATH="$shim:$PATH" "$SCRIPT" after "$live" "$T/receipt"
  done
  for tag in whitespace garbage multiline; do
    shim="$T/ps-$tag"
    mkdir -p "$shim"
    case "$tag" in
      whitespace) printf '#!/bin/sh\nprintf "   \\n"\n' >"$shim/ps" ;;
      garbage) printf '#!/bin/sh\nprintf "not-a-start-time\\n"\n' >"$shim/ps" ;;
      multiline) printf '#!/bin/sh\n/bin/ps "$@"\nprintf "\\n"\n' >"$shim/ps" ;;
    esac
    chmod +x "$shim/ps"
    lane "start shape: $tag before fails closed" 1 "FAIL" \
      -- env PATH="$shim:$PATH" "$SCRIPT" before "$live" "$T/ps-receipt-$tag"
    absent "ps-$tag refusal" "$T/ps-receipt-$tag"
    lane "start shape: $tag after never PASSes" 1 "FAIL" \
      -- env PATH="$shim:$PATH" "$SCRIPT" after "$live" "$T/receipt"
  done
  # A valid executable may have an unlinked non-executable mapping (observed
  # on the real host's Logging/.plist-cache). Only the executable must exist.
  LSOF_BIN="$(command -v lsof)"
  shim="$T/lsof-mapped"
  mkdir -p "$shim"
  printf "#!/bin/sh\n\"%s\" \"\$@\" || exit 1\nprintf 'ftxt\\nn/tmp/qa11-unlinked-mapping\\n'\n" \
    "$LSOF_BIN" >"$shim/lsof"
  chmod +x "$shim/lsof"
  lane "resolver: unlinked auxiliary mapping is not the executable" 0 "RECEIPT" \
    -- env PATH="$shim:$PATH" "$SCRIPT" before "$live" "$T/mapped-receipt"
  lane "resolver: counted real executable still PASSes with an unlinked mapping" 0 "PASS" \
    -- env PATH="$shim:$PATH" "$SCRIPT" after "$live" "$T/mapped-receipt"

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
  for field in pids.txt binary.sha256 state.sha256; do
    if [ "$(cat "$T/empty-receipt/$field")" = "count=0" ]; then
      printf 'ok   empty state: %s explicitly records count=0\n' "$field"
    else
      rc=1
      printf 'FAIL empty state: %s has no count=0 header\n' "$field"
    fi
  done
  : >"$T/empty-receipt/pids.txt"
  lane "empty state: zero-byte pid receipt is not a valid count=0" 1 "FAIL" \
    -- "$SCRIPT" after "$empty_live" "$T/empty-receipt"
  printf 'count=0\n' >"$T/empty-receipt/pids.txt"
  lane "empty state: explicit zero header restores PASS" 0 "PASS" \
    -- "$SCRIPT" after "$empty_live" "$T/empty-receipt"

  mkdir -p "$T/same-pid-live/.omosense/state"
  printf '{"pid":%d}\n' "$SPID" >"$T/same-pid-live/.omosense/state/a.lock.json"
  printf '{"pid":%d}\n' "$SPID" >"$T/same-pid-live/.omosense/state/b.lock.json"
  lane "counts: two locks for one live pid take a receipt" 0 "RECEIPT" \
    -- "$SCRIPT" before "$T/same-pid-live" "$T/same-pid-receipt"
  IFS= read -r header <"$T/same-pid-receipt/pids.txt"
  IFS= read -r first <"$T/same-pid-receipt/binary.sha256"
  if [[ "$header" = count=2 && "$first" = count=1 ]]; then
    printf 'ok   counts: all two locks and exactly one distinct live pid\n'
  else
    rc=1
    printf 'FAIL counts: lock/live headers are %s / %s\n' "$header" "$first"
  fi
  lane "counts: repeated live pid stays a legitimate PASS" 0 "PASS" \
    -- "$SCRIPT" after "$T/same-pid-live" "$T/same-pid-receipt"

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
    if [[ "$got_line" = "count=1"$'\n'"$REL_PID"$'\t'"$want_sha"$'\t'/* ]]; then
      printf 'ok   relative-argv0: receipt holds the real sha256 of the resolved executable\n'
    else
      rc=1
      printf 'FAIL relative-argv0: receipt does not hold the real digest %s\n%s\n' "$want_sha" "$got_line"
    fi
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

    # sort/awk no longer process measurements. Hostile versions therefore
    # cannot lose coverage: the receipt still holds the real pid+digest, and
    # a SAME-PATH content replacement must FAIL through the real comparator.
    for tag in sort-empty sort-drop awk-empty; do
      shim="$T/$tag"
      mkdir -p "$shim"
      case "$tag" in
        sort-empty) printf '#!/bin/sh\n/bin/cat >/dev/null\n' >"$shim/sort" ;;
        sort-drop) printf '#!/bin/sh\n/usr/bin/sed "1d"\n' >"$shim/sort" ;;
        awk-empty) printf '#!/bin/sh\n/bin/cat >/dev/null\n' >"$shim/awk" ;;
      esac
      chmod +x "$shim/"*
      lane "processing: $tag cannot erase measured records before" 0 "RECEIPT" \
        -- env PATH="$shim:$PATH" "$SCRIPT" before "$rel_live" "$T/$tag-receipt"
      got_line="$(cat "$T/$tag-receipt/binary.sha256")"
      if [[ "$got_line" = "count=1"$'\n'"$REL_PID"$'\t'"$want_sha"$'\t'/* ]]; then
        printf 'ok   processing: %s preserves a real counted pid+digest\n' "$tag"
      else
        rc=1
        printf 'FAIL processing: %s erased or corrupted measurements\n%s\n' "$tag" "$got_line"
      fi
      cat "$HELPER_DIR/relative-host.next" >"$HELPER_DIR/relative-host"
      lane "processing: $tag never hides replaced binary bytes" 1 "FAIL deployed binary" \
        -- env PATH="$shim:$PATH" "$SCRIPT" after "$rel_live" "$T/$tag-receipt"
      cat "$HELPER_DIR/relative-host.a" >"$HELPER_DIR/relative-host"
      lane "processing: $tag restored bytes legitimately PASS" 0 "PASS" \
        -- env PATH="$shim:$PATH" "$SCRIPT" after "$rel_live" "$T/$tag-receipt"
    done

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
