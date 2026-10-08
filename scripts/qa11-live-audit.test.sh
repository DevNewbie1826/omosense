#!/usr/bin/env bash
# qa11-live-audit.test.sh - runs the qa11-live-audit.sh self-test.
# Kept as a separate file so `scripts/qa11-live-audit.test.sh` is the obvious
# test entry point; all logic lives in qa11-live-audit.sh.
set -euo pipefail
dir="$(cd "$(dirname "$0")" && pwd)"
exec "$dir/qa11-live-audit.sh" selftest
