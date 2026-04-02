# Releasing Mailfrost

Mailfrost releases are built automatically by GitHub Actions.

The release workflow runs when you push a git tag that starts with `v`.

## Release Steps

1. Make sure your changes are committed.

2. Push the current `main` branch:

```bash
git push origin main
```

3. Create the next version tag:
```bash
(Check with git describe --tags --abbrev=0)
git tag -a v0.1.1 -m "v0.1.1"
```

4. Push the tag:

```bash
git push origin v0.1.1
```

That tag push triggers the GitHub Actions release workflow.

## What The Workflow Builds

The workflow currently publishes this release asset:

- `mailfrost_vX.Y.Z_macos_apple_silicon.zip`

Each zip contains the `mailfrost` binary.

The version string inside the binary is set from the git tag, for example `v0.1.1`.

After the release asset is uploaded, the workflow also updates the shared Homebrew tap repo `nivoc/homebrew-tap` so `brew upgrade mailfrost` points at the new macOS asset and checksum automatically.
After that, the workflow deletes `.zip` assets from older releases. The release/tag history stays visible on GitHub, but only the newest binary zip remains directly downloadable.

## Notes

- Use a new tag for every release.
- Cross-repo Homebrew updates use the `HOMEBREW_TAP_TOKEN` repository secret in the main repo.
- After the tag workflow finishes, `brew update && brew upgrade mailfrost` should pick up the new version automatically.
- Older binary zips are cleaned up automatically. If someone needs an older version later, they should build it from source from the tagged commit.
- If a tag already exists locally, inspect it before reusing or moving it.
- If `git push` fails with a DNS or network error, just retry when connectivity is back.
