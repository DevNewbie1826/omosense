# omosense

omosense is the Go rewrite of a set of Bun monitors (listen, watch-google, remind, watch-herdr, memory-tidy, say). Each subcommand runs one source in-process and prints the same stdout grammar the TypeScript tools used. For long-running use, a resident daemon plus attach clients is the default mode.

## Quick start

Run it without installing:

```sh
npx omosense --help
bunx omosense --help
```

Or install it globally:

```sh
npm i -g omosense
omosense --help
```

The help output starts with:

```
Usage: omosense <subcommand> [--profile main|family] [flags]
```

Subcommands: `listen`, `google`, `remind`, `herdr`, `rpc`, `tidy`, `say`, `daemon`, `attach`. Run `omosense <subcommand> --help` for details on each.

## Supported platforms

The package ships four static (`CGO_ENABLED=0`) prebuilt binaries, and a small Node shim picks the right one at run time:

- darwin-arm64
- darwin-x64
- linux-arm64
- linux-x64

Windows isn't supported in this version. npm and npx refuse to install with `EBADPLATFORM`, because `package.json` limits `os` and `cpu`. If the shim does end up running on another platform, it stops with:

```
omosense: unsupported platform <key> (supported: darwin-arm64, darwin-x64, linux-arm64, linux-x64)
```

## Signals

The shim forwards SIGTERM and SIGHUP to the binary. Ctrl-C in a terminal reaches the binary directly through the terminal's process group. If you run omosense under a supervisor, send SIGTERM to the shim. A programmatic SIGINT sent only to the shim's pid isn't forwarded.

## Daemons and the npx cache

When you start omosense through `npx`, an auto-spawned daemon runs the binary from the npx cache. Clearing that cache while the daemon is running deletes the binary out from under it. For long-running daemons, install globally with `npm i -g omosense` instead.

## Building from source

You need Go (see `go.mod` for the version).

```sh
git clone https://github.com/DevNewbie1826/omosense.git
cd omosense
go build ./cmd/omosense
./omosense --help
```

Maintainers: the npm release process is described in [docs/npm-release.md](docs/npm-release.md).

## License

MIT. Copyright (c) 2026 DevNewbie1826.
