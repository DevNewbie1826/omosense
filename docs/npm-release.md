# Releasing omosense to npm

Releases go out from GitHub Actions through npm Trusted Publishing (OIDC). No npm token is stored in the repository or in its secrets.

## Release procedure

1. Open a PR that bumps `"version"` in `package.json` (for example `0.0.2`). Merge it.
2. Tag the merge commit with the matching version and push the tag:

   ```sh
   git tag v0.0.2 <merge-commit>
   git push origin v0.0.2
   ```

3. The tag push starts `.github/workflows/release.yml`. It:
   - checks that the tag matches the `package.json` version,
   - runs `npm pack`, whose `prepack` step calls `scripts/build-npm.sh` to build the four binaries (darwin-arm64, darwin-x64, linux-arm64, linux-x64),
   - installs the packed tarball in a temp dir and smoke-tests `omosense --help`,
   - publishes from the `npm-release` environment with `--provenance`, authenticating through OIDC.

If that version is already on the registry, the publish job skips instead of failing. So rerunning a finished tag is harmless.

CI needs npm CLI 11.5.1 or newer and Node 22.14 or newer. The workflow installs a pinned npm for that reason.

## Trusted publishing setup

This is a one-time step, done by an owner who's logged in to npm. It may ask for browser 2FA.

```sh
npm trust github omosense --file release.yml --repo DevNewbie1826/omosense --allow-publish
npm trust list omosense
```

Keep the workflow file named `release.yml`. npm matches the filename exactly, and a rename breaks publishing until you register again.

Provenance only works while the repository is public. If the repo goes private, an explicit `--provenance` publish fails.

## Bootstrap history

npm only lets you attach a trusted publisher to a package that already exists. Because of that, `omosense@0.0.1` was published once by hand from a verified tarball. The trusted publisher was registered after that, and every later version comes from CI.

## Verifying a release

```sh
npm view omosense@X.Y.Z version dist.attestations
npx --yes omosense@X.Y.Z --help
```

The first command should print the version and an attestations entry (the provenance). The second should print usage starting with `Usage: omosense [<subcommand> [flags]]` and exit 0.

## Troubleshooting

**`ENEEDAUTH` in the publish step.** The OIDC exchange didn't match a trusted publisher. Check, in this order:

- the workflow file is still `.github/workflows/release.yml`,
- the repository is `DevNewbie1826/omosense` and matches `repository.url` in `package.json`,
- `npm trust list omosense` shows the registration at all.

If the registration exists but publishing still fails, register again with the environment set explicitly:

```sh
npm trust github omosense --file release.yml --repo DevNewbie1826/omosense --env npm-release --allow-publish
```

No workflow change is needed for that.

**Version mismatch.** The build job stops when the tag (minus the `v`) differs from `package.json`. Fix the tag, or bump the version through a PR first.
