#!/usr/bin/env node
// omosense npm shim: pick the prebuilt Go binary for this platform and run it.
"use strict";

const { spawn } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const TARGETS = {
  "darwin-arm64": "omosense-darwin-arm64",
  "darwin-x64": "omosense-darwin-amd64",
  "linux-arm64": "omosense-linux-arm64",
  "linux-x64": "omosense-linux-amd64",
};

const key = `${process.platform}-${process.arch}`;
const name = TARGETS[key];
if (!name) {
  process.stderr.write(
    `omosense: unsupported platform ${key} (supported: darwin-arm64, darwin-x64, linux-arm64, linux-x64)\n` +
      "Windows is not supported in this version.\n"
  );
  process.exit(1);
}

const bin = path.join(__dirname, "dist", name);
if (!fs.existsSync(bin)) {
  process.stderr.write(
    `omosense: missing binary ${bin}; the package install is incomplete - reinstall omosense\n`
  );
  process.exit(1);
}

try {
  fs.accessSync(bin, fs.constants.X_OK);
} catch (_err) {
  // npm may normalize file modes (the exec bit is only forced on bin entries),
  // so restore it before spawning.
  try {
    fs.chmodSync(bin, 0o755);
  } catch (err) {
    process.stderr.write(`omosense: cannot make binary executable ${bin}: ${err.message}\n`);
    process.exit(1);
  }
}

const child = spawn(bin, process.argv.slice(2), { stdio: "inherit" });

child.on("error", (err) => {
  process.stderr.write(`omosense: failed to start ${bin}: ${err.message}\n`);
  process.exit(1);
});

function forward(sig) {
  child.kill(sig);
}

function swallow() {}

// SIGTERM/SIGHUP are forwarded. SIGINT is deliberately NOT forwarded:
// terminal Ctrl-C already reaches the child through the foreground process
// group, and the Go side uses signal.NotifyContext - forwarding would
// double-deliver. The no-op handler keeps the shim alive until the child
// exits. Supervisors should send SIGTERM.
process.on("SIGTERM", forward);
process.on("SIGHUP", forward);
process.on("SIGINT", swallow);

child.on("exit", (code, signal) => {
  if (signal) {
    process.removeListener("SIGTERM", forward);
    process.removeListener("SIGHUP", forward);
    process.removeListener("SIGINT", swallow);
    process.kill(process.pid, signal);
    process.exit(128 + os.constants.signals[signal]);
  }
  process.exit(code);
});
