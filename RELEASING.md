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
git tag -a v0.1.1 -m "v0.1.1"
```

4. Push the tag:

```bash
git push origin v0.1.1
```

That tag push triggers the GitHub Actions release workflow.

## What The Workflow Builds

The workflow currently publishes these release assets:

- `mailfrost_vX.Y.Z_macos_apple_silicon.zip`
- `mailfrost_vX.Y.Z_linux_x86_64.zip`
- `mailfrost_vX.Y.Z_linux_arm64.zip`

Each zip contains the `mailfrost` binary.

The version string inside the binary is set from the git tag, for example `v0.1.1`.

## Notes

- Use a new tag for every release.
- If a tag already exists locally, inspect it before reusing or moving it.
- If `git push` fails with a DNS or network error, just retry when connectivity is back.
