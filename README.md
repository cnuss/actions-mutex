# actions-mutex

[![CodeQL](https://github.com/cnuss/actions-mutex/actions/workflows/codeql.yml/badge.svg)](https://github.com/cnuss/actions-mutex/actions/workflows/codeql.yml)
[![Latest release](https://img.shields.io/github/v/release/cnuss/actions-mutex?sort=semver&logo=github)](https://github.com/cnuss/actions-mutex/releases/latest)
[![Dependabot](https://img.shields.io/badge/dependabot-enabled-025E8C?logo=dependabot)](https://github.com/cnuss/actions-mutex/security/dependabot)
[![Security policy](https://img.shields.io/badge/security-policy-brightgreen)](./SECURITY.md)

A distributed mutex for GitHub Actions, built on the Actions cache service.
Jobs that share a key take turns: one holds the lock, the rest wait, and
nobody is cancelled.

## Why

`concurrency:` serializes whole jobs or workflows, and keeps at most one
pending run per group; anything else waiting is cancelled. This action locks
just the steps that need it, and every waiter gets its turn.

The cache service's `CreateCacheEntry` is an atomic reservation: given N
concurrent callers on the same key+version, exactly one gets a write URL and the
rest get `already_exists`. Deleting the entry through the REST API frees the
key+version, and the next `CreateCacheEntry` wins at once. That's a lock.

## Inputs

| input | required | default | description |
|---|---|---|---|
| `key` | yes | | Lock name. Every job using the same key takes turns. |
| `run` | no | | Bash script run while holding the lock, released when it exits. Without it, the lock is held until the job ends. |
| `timeout-seconds` | no | `3600` | How long to wait for the lock before failing. |
| `github-token` | no | `${{ github.token }}` | Token for the REST calls. Needs `actions: write`. |

## Outputs

| output | description |
|---|---|
| `wait-seconds` | Seconds spent waiting for the lock. |

## Usage

Hold the lock for the rest of the job:

```yaml
permissions:
  actions: write
  contents: read

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: cnuss/actions-mutex@v1
        with:
          key: deploy-production

      - run: ./deploy.sh # every step from here on runs under the lock
```

Or lock a single script:

```yaml
      - uses: cnuss/actions-mutex@v1
        with:
          key: tofu-state
          run: tofu apply -input=false plan.tfplan
```

## How it works

1. **Acquire**: `CreateCacheEntry { key: "mutex/<key>", version }`. The winner
   uploads a small record naming its job and finalizes it immediately, so the
   entry exists and can be deleted. It also publishes
   `mutex/<key>/holder/<entry id>` with the same record.
2. **Wait**: losers poll `GetCacheEntryDownloadURL` every 2–3 s and race
   `CreateCacheEntry` again only when the lock looks free, or every 15 s in
   case a lagging replica still shows a released lock. On a 429 they wait out
   `Retry-After` and spread their retries across the next window.
3. **Release**: `DELETE /repos/{repo}/actions/caches/{entry id}`, from the post
   step (or right after `run` exits). Post runs on failure and cancellation
   too.
4. **Abandoned locks**: if the holder's runner dies before post can run, a
   waiter notices within about a minute of the job ending: it reads the holder
   record for the current lock entry, sees that job has completed, and deletes
   the lock.

## Notes

- Dependency-free: no `node_modules`, no bundling, no build step. Node 24.
- Needs `permissions: actions: write` in the calling job.
- **Locks are per ref.** Cache entries are scoped to the branch or PR ref that
  wrote them, so a run on `main` and a run on a feature branch can hold the
  same key at once.
- **Not FIFO.** After a release, whichever waiter retries first wins.
- **Cache-service budget.** The service allows about 200 `CreateCacheEntry`
  and 1,500 `GetCacheEntryDownloadURL` calls per minute, shared across jobs
  (and most likely across the repository, `actions/cache` included). A waiter
  makes about 25 reads and 4 creates a minute.
- **REST budget.** Each acquire and release costs a few REST calls, and each
  waiter makes two more per minute while it waits. `GITHUB_TOKEN` gets 1,000
  REST requests per hour per repository.
- A stuck lock can always be cleared by hand: `gh cache delete 'mutex/<key>'`.
- Set `ACTIONS_STEP_DEBUG=true` (or re-run with debug logging) to log every
  request's method, redacted URL, status, headers, and body.
