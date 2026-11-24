# Release Process for HIE

This document describes the release process for the Helios Integration Engine (HIE).

## Overview

HIE is a single Rust crate (`helios-hie`, binary `hie`). It is not published to crates.io (`publish = false` in [Cargo.toml](Cargo.toml)): a release is a version tag, and the tag drives everything else through [release.yml](.github/workflows/release.yml). See [Release and deploy](README.md#release-and-deploy) for what the workflow builds and where it deploys.

## Prerequisites

1. Install `cargo-release`:
   ```bash
   cargo install cargo-release
   ```

2. Ensure you can push to `main` and push tags on [HeliosSoftware/hie](https://github.com/HeliosSoftware/hie).

3. Ensure the secrets are set on the repository (Settings → Secrets and variables → Actions):
   - `DOCKER_HOST` (required): the shared Docker server, the same value hfs uses. Every image build and `docker run` in CI and the release runs there, not on the runner.
   - `HIE_DEPLOY_HOST`, `HIE_DEPLOY_USER` (required)
   - `HIE_DEPLOY_PORT`, `HIE_FHIR_BASE_URL`, `HIE_PUBLIC_URL` (optional; defaults in the workflow)
   - `HIE_AUTH_JWKS_URL`, `HIE_AUTH_ISSUER`, `HIE_AUTH_AUDIENCE` (only for a secure environment; without them the API is deployed open, like the public sandbox)
   - `HIE_FHIR_TOKEN_URL`, `HIE_FHIR_CLIENT_ID`, `HIE_FHIR_CLIENT_SECRET`, `HIE_FHIR_SCOPE` (only for a secure environment whose HFS requires a token; without them the engine calls HFS anonymously, as the sandbox does against the public HFS)
   - `HIE_DATABASE_URL` (optional: a `postgres://` URL for a shared store; without it the engine keeps its SQLite file under `/opt/hie/data`)
   - `HIE_TERMINOLOGY_SERVER`, `HIE_TERMINOLOGY_TOKEN` (optional: the HTS transforms may ask; the public HTS by default, the token only where that HTS requires one)

4. Ensure the self-hosted runners are online. Every job runs on `self-hosted`; the deploy job needs a runner labelled `Deploy` whose user can SSH to `HIE_DEPLOY_USER@HIE_DEPLOY_HOST` with a key in its `~/.ssh`. That user needs passwordless `sudo` on the host.

## Release Steps

### 1. Prepare for Release

- Bring the built-in time zone data up to date: `cargo update -p jiff-tzdb` takes the newest IANA database that `jiff-tzdb` ships. The engine never reads the host's zone files, so a zone rule changed by a government reaches it only through a release.
- Run the same checks CI runs:
```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
python3 -m unittest tests/test_lab_results.py tests/test_hl7.py tests/test_terminology.py tests/test_worker.py
HIE_TEST_PYTHON=python3 cargo test
docker build -t hie:local . && docker run --rm hie:local check
```
- Check in any changes.
- Ensure that your build in GitHub Actions has succeeded fully. These are found in [ci.yml](.github/workflows/ci.yml).

### 2. Create a Release

To create a new release, use cargo-release with the appropriate version bump:

```bash
# For a patch release (0.1.0 -> 0.1.1)
cargo release patch --dry-run

# For a minor release (0.1.0 -> 0.2.0)
cargo release minor --dry-run

# For a major release (0.1.0 -> 1.0.0)
cargo release major --dry-run

# For a pre-release (0.1.0 -> 0.2.0-rc.1)
cargo release minor --dry-run --pre-release rc
```

Review the dry-run output, then execute without `--dry-run`:

```bash
cargo release patch --execute
```

This will:
- Update the version in Cargo.toml and Cargo.lock
- Create a git commit with the version bump
- Create a `v{version}` git tag
- Push these changes to GitHub

After the tag is pushed, GitHub Actions will automatically:
- Build the Linux x86_64 release archive (`hie-{version}-x86_64-unknown-linux-gnu.tar.gz` and its SHA-256)
- Create a [GitHub Release](https://github.com/HeliosSoftware/hie/releases) with the archive; a version with a `-` (such as `0.2.0-rc.1`) is marked as a pre-release
- Build and push the container image to `ghcr.io/heliossoftware/hie` (`:{version}`, `:{major.minor}`, `:sha-{sha}`, and `:latest` for a non-pre-release)
- Deploy to [hie.heliossoftware.com](https://hie.heliossoftware.com) and fail unless `/health` answers

### 3. Verify the Release

- Check the [Release workflow run](https://github.com/HeliosSoftware/hie/actions/workflows/release.yml) finished green.
- Confirm the deployed server is up:
```bash
curl -sf https://hie.heliossoftware.com/health
```
- If the deploy failed, the job prints the last 40 lines of `journalctl -u hie` from the host.
