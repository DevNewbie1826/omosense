#!/usr/bin/env bash
# qa11-live-audit.test.sh - runs the qa11-live-audit.sh self-test.
# Kept as a separate file so `scripts/qa11-live-audit.test.sh` is the obvious
# test entry point; the selftest logic lives in qa11-live-audit.sh.
set -u
unset CDPATH # cd must resolve relative paths against the cwd only
dir="$(cd "$(dirname "$0")" && pwd)" || exit 1
exec "$dir/qa11-live-audit.sh" selftest
