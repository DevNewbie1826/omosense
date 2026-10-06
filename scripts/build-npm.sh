#!/bin/sh
# prepack runs this: rebuild all four binaries so a pack/publish can never ship stale ones.
set -eu

cd "$(dirname "$0")/.."

if ! command -v go >/dev/null 2>&1; then
  echo "error: go not found in PATH; install Go from https://go.dev/dl/ to build the npm binaries" >&2
  exit 1
fi

rm -rf npm/dist
mkdir -p npm/dist

for target in darwin/arm64 darwin/amd64 linux/arm64 linux/amd64; do
  os=${target%/*}
  arch=${target#*/}
  out="npm/dist/omosense-$os-$arch"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags "-s -w" -o "$out" ./cmd/omosense
  chmod 0755 "$out"
  echo "built $out ($os/$arch)"
done
