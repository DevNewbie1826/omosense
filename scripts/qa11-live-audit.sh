#!/usr/bin/env bash
# qa11-live-audit.sh - read-only liveness audit for omosense hosts.
# Simplified owner contract (Discord msg 1557773996785795073): measuring that
# the running hosts' pid and start time are unchanged is sufficient. The ONLY
# measured facts are the pid and `ps -o lstart=` start time of every host lock
# ("pid and start time were obtained" = the single fail-closed rule); binary
# hashing, lsof, /proc, ps -o comm, argv0 lanes, snapshots and shape/count
# validators are removed entirely.

set -u -o pipefail
export LC_ALL=C

RECEIPT_NAME=receipt.txt

usage() {
  cat <<'EOF'
usage: qa11-live-audit.sh before <live-folder> <receipt-dir>
       qa11-live-audit.sh after  <live-folder> <receipt-dir>
       qa11-live-audit.sh selftest | --help

before: measure pid + ps start time of every <live>/.omosense/state/*.lock.json
        into ONE receipt <receipt-dir>/receipt.txt (count=N header; count=0 is
        a valid empty set from a readable state dir). Receipt dir must resolve
        under /tmp (macOS /private/tmp) and must not exist yet or be empty -
        anything else is refused, exit 2, before anything is written. If ANY
        pid or start time cannot be obtained: exit 1, no receipt.
after:  re-measure and compare; identical -> "PASS" exit 0; any difference,
        missing/empty receipt, or unobtainable measurement -> "FAIL" exit 1.
        The live folder is only ever read.
EOF
}

fail() { printf 'FAIL: %s\n' "$*"; exit 1; }
refuse() { printf 'refuse: %s\n' "$*" >&2; exit 2; }

# Print the pid (integer > 1) named by a lock file, or fail (unparsable).
lock_pid() {
  local pid
  pid=$(grep -Eo '"pid"[[:space:]]*:[[:space:]]*[0-9]+' "$1" 2>/dev/null | head -n 1) || return 1
  pid=${pid//[!0-9]/}
  case $pid in '' | *[!0-9]*) return 1 ;; esac
  [ "$pid" -gt 1 ] 2>/dev/null || return 1
  printf '%s\n' "$pid"
}

# Print the ps start time of a pid, or fail (ps missing or failing,
# empty output, dead pid - every path fails closed).
pid_lstart() {
  local out
  out=$(ps -o lstart= -p "$1" 2>/dev/null) || return 1
  [ -n "$out" ] || return 1
  printf '%s\n' "$out"
}

# Print "count=N" plus one "lock pid start" line per lock (glob order),
# or fail with the reason on stderr.
measure() {
  local state="$1/.omosense/state" lock name pid start n=0 out=""
  [ -d "$state" ] || { printf 'state dir missing: %s\n' "$state" >&2; return 1; }
  ls "$state" >/dev/null 2>&1 || { printf 'state dir unreadable: %s\n' "$state" >&2; return 1; }
  for lock in "$state"/*.lock.json; do
    if [ ! -f "$lock" ]; then
      { [ -e "$lock" ] || [ -L "$lock" ]; } && { printf 'unparsable lock: %s\n' "${lock##*/}" >&2; return 1; }
      continue # glob matched nothing
    fi
    name=${lock##*/}
    pid=$(lock_pid "$lock") || { printf 'unparsable lock: %s\n' "$name" >&2; return 1; }
    start=$(pid_lstart "$pid") || { printf 'start time unobtainable for pid %s (%s)\n' "$pid" "$name" >&2; return 1; }
    out="$out$name $pid $start
"
    n=$((n + 1))
  done
  printf 'count=%d\n%s' "$n" "$out"
}

# Print a path with its existing prefix resolved (cd + pwd -P follows
# symlinks); tail components must be plain names (no . / .. / empty).
resolve_dir_path() {
  local p="$1" tail=""
  while [ ! -e "$p" ] && [ "$p" != "/" ]; do
    tail="${p##*/}${tail:+/$tail}"
    p=$(dirname "$p")
  done
  local base
  base=$(cd "$p" 2>/dev/null && pwd -P) || return 1
  if [ -n "$tail" ]; then
    case "/$tail/" in */./* | */../* | *//*) return 1 ;; esac
    printf '%s/%s\n' "${base%/}" "$tail"
  else
    printf '%s\n' "$base"
  fi
}

# Succeed when a dir resolves under /tmp (macOS /private/tmp).
under_tmp() {
  local r
  r=$(resolve_dir_path "$1") || return 1
  case $r in /tmp | /tmp/* | /private/tmp | /private/tmp/*) return 0 ;; *) return 1 ;; esac
}

# Refuse (exit 2, nothing written) unless the receipt dir resolves under
# /tmp, does not overlap the resolved live folder, .omosense or state dir,
# and is either absent or an empty real directory.
check_receipt_dir() {
  local live=$1 rdir=$2 entries r t tr
  under_tmp "$rdir" || refuse "receipt dir must resolve under /tmp: $rdir"
  r=$(resolve_dir_path "$rdir") || refuse "receipt dir unresolvable: $rdir"
  for t in "$live" "$live/.omosense" "$live/.omosense/state"; do
    [ -d "$t" ] || continue
    # unenterable: measure fails on the state dir before anything is written
    tr=$(cd "$t" 2>/dev/null && pwd -P) || continue
    case "$r/" in "$tr/"*) refuse "receipt dir is inside the live folder: $rdir" ;; esac
    case "$tr/" in "$r/"*) refuse "receipt dir contains the live folder: $rdir" ;; esac
  done
  if [ -e "$rdir" ]; then
    { [ ! -L "$rdir" ] && [ -d "$rdir" ]; } || refuse "receipt dir is not a real directory: $rdir"
    entries=$(ls -A "$rdir" 2>/dev/null) || refuse "receipt dir unreadable: $rdir"
    [ -z "$entries" ] || refuse "receipt dir exists and is not empty: $rdir"
  fi
}

cmd_before() {
  local live=$1 rdir=$2 receipt measurement
  [ -d "$live" ] || fail "live folder missing: $live"
  check_receipt_dir "$live" "$rdir"
  receipt=$rdir/$RECEIPT_NAME
  measurement=$(measure "$live" 2>&1) || fail "$measurement"
  mkdir -p "$rdir" || fail "cannot create receipt dir: $rdir"
  printf '%s\n' "$measurement" >"$receipt" 2>/dev/null || fail "cannot write receipt: $receipt"
  printf 'receipt: %s (%s)\n' "$receipt" "$(printf '%s\n' "$measurement" | head -n 1)"
}

cmd_after() {
  local live=$1 rdir=$2 receipt want got
  receipt=$rdir/$RECEIPT_NAME
  [ -f "$receipt" ] || fail "missing receipt: $receipt"
  [ -s "$receipt" ] || fail "empty receipt: $receipt"
  want=$(cat "$receipt" 2>/dev/null) || fail "unreadable receipt: $receipt"
  got=$(measure "$live" 2>&1) || fail "$got"
  if [ "$want" = "$got" ]; then
    printf 'PASS\n'
    exit 0
  fi
  printf 'FAIL: measured state differs from receipt\n'
  diff <(printf '%s\n' "$want") <(printf '%s\n' "$got")
  exit 1
}

# Selftest: fixtures under /tmp; the live pid is a plain sleep under its
# own name; every lane asserts a binary observable (exit code, files, content).
cmd_selftest() {
  local root live1 live2 live2b live3 minbin outside helper="" passes=0 fails=0 out rc t
  root=$(mktemp -d /tmp/qa11-selftest.XXXXXX) || { printf 'selftest: mktemp failed\n' >&2; return 1; }
  trap '[ -n "$helper" ] && kill "$helper" >/dev/null 2>&1; rm -rf -- "$root"' EXIT
  ok() { printf 'ok   %s\n' "$1"; passes=$((passes + 1)); }
  bad() { printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); }
  rc_is() { # rc_is DESC WANT_RC CMD... - pass when CMD exits WANT_RC
    local d=$1 w=$2 o r
    shift 2
    o=$("$@" 2>&1)
    r=$?
    if [ "$r" -eq "$w" ]; then ok "$d"; else bad "$d (want rc=$w got rc=$r: $o)"; return 1; fi
  }
  is_true() { local d=$1; shift; if "$@"; then ok "$d"; else bad "$d"; fi; }

  # Fixture: live1 with one live host (plain sleep under its own name).
  live1=$root/live1
  mkdir -p "$live1/.omosense/state"
  /bin/sleep 300 >/dev/null 2>&1 &
  helper=$!
  printf '{"pid":%d}\n' "$helper" >"$live1/.omosense/state/listen.lock.json"

  rc_is "before writes a receipt" 0 "$0" before "$live1" "$root/r1" &&
    is_true "receipt is non-empty" test -s "$root/r1/$RECEIPT_NAME"
  rc_is "after passes an untouched receipt" 0 "$0" after "$live1" "$root/r1"

  sed -E 's/^(listen\.lock\.json [0-9]+) .*/\1 Mon Jan  1 00:00:01 2020/' \
    "$root/r1/$RECEIPT_NAME" >"$root/r1/.t" && mv "$root/r1/.t" "$root/r1/$RECEIPT_NAME"
  rc_is "after fails a tampered start time" 1 "$0" after "$live1" "$root/r1"

  rc_is "after fails a missing receipt" 1 "$0" after "$live1" "$root/r-missing"

  minbin=$root/minbin # every external tool before needs, except ps
  mkdir -p "$minbin"
  for t in bash grep head dirname ls mkdir; do ln -s "$(command -v "$t")" "$minbin/$t"; done
  out=$(PATH="$minbin" "$0" before "$live1" "$root/r4" 2>&1)
  rc=$?
  if [ "$rc" -eq 1 ] && [ ! -e "$root/r4" ]; then
    ok "ps unavailable: before fails closed, nothing written"
  else
    bad "ps unavailable (rc=$rc, r4 created: $([ -e "$root/r4" ] && echo yes || echo no): $out)"
  fi

  outside=/var/empty/qa11-refuse-$$.d # exists-checkable, never under /tmp
  out=$("$0" before "$live1" "$outside" 2>&1)
  rc=$?
  if [ "$rc" -eq 2 ] && [ ! -e "$outside" ]; then
    ok "receipt dir outside /tmp refused, nothing created"
  else
    bad "receipt dir outside /tmp refused (rc=$rc: $out)"
  fi

  mkdir -p "$root/r6" && : >"$root/r6/occupant"
  rc_is "existing non-empty receipt dir refused" 2 "$0" before "$live1" "$root/r6"

  live2=$root/live2 # empty but readable state dir: a valid empty set
  mkdir -p "$live2/.omosense/state"
  rc_is "before on empty state records count=0" 0 "$0" before "$live2" "$root/r7" &&
    is_true "receipt says exactly count=0" test "$(cat "$root/r7/$RECEIPT_NAME")" = "count=0"
  rc_is "after passes the empty set" 0 "$0" after "$live2" "$root/r7"

  live2b=$root/live2b # unreadable state dir must not read as count=0
  mkdir -p "$live2b/.omosense/state"
  chmod 000 "$live2b/.omosense/state"
  out=$("$0" before "$live2b" "$root/r7b" 2>&1)
  rc=$?
  chmod 755 "$live2b/.omosense/state"
  if [ "$rc" -eq 1 ] && [ ! -e "$root/r7b" ]; then
    ok "unreadable state dir: before fails closed"
  else
    bad "unreadable state dir (rc=$rc: $out)"
  fi

  live3=$root/live3 # lock whose pid is not > 1: unparsable, fail closed
  mkdir -p "$live3/.omosense/state"
  printf '{"pid":1}\n' >"$live3/.omosense/state/bad.lock.json"
  out=$("$0" before "$live3" "$root/r8" 2>&1)
  rc=$?
  if [ "$rc" -eq 1 ] && [ ! -e "$root/r8" ]; then
    ok "unparsable lock: before fails closed, nothing written"
  else
    bad "unparsable lock (rc=$rc: $out)"
  fi

  out=$("$0" before "$live1" "$live1/.omosense/state/receipt" 2>&1)
  rc=$?
  if [ "$rc" -eq 2 ] && [ ! -e "$live1/.omosense/state/receipt" ]; then
    ok "receipt dir inside the live folder refused, nothing created"
  else
    bad "receipt dir inside the live folder (rc=$rc: $out)"
  fi

  cp -R "$live1" "$root/live4" # dangling lock beside a measurable one
  ln -s "$root/nowhere" "$root/live4/.omosense/state/broken.lock.json"
  out=$("$0" before "$root/live4" "$root/r10" 2>&1)
  rc=$?
  if [ "$rc" -eq 1 ] && [ ! -e "$root/r10" ]; then
    ok "dangling lock: before fails closed, nothing written"
  else
    bad "dangling lock (rc=$rc: $out)"
  fi

  rc_is "before for the dead-pid lane" 0 "$0" before "$live1" "$root/r9"
  kill "$helper" >/dev/null 2>&1
  wait "$helper" 2>/dev/null
  helper=""
  rc_is "after fails when a host pid died" 1 "$0" after "$live1" "$root/r9"

  if [ "$fails" -eq 0 ]; then
    printf 'SELFTEST PASS (%d checks)\n' "$passes"
    exit 0
  fi
  printf 'SELFTEST FAIL (%d failed, %d passed)\n' "$fails" "$passes"
  exit 1
}

main() {
  case ${1:-} in
    before) shift; [ $# -eq 2 ] || { usage >&2; exit 2; }; cmd_before "$@" ;;
    after) shift; [ $# -eq 2 ] || { usage >&2; exit 2; }; cmd_after "$@" ;;
    selftest) shift; cmd_selftest "$@" ;;
    -h | --help) usage ;;
    *) usage >&2; exit 2 ;;
  esac
}
main "$@"
