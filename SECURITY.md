# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| `v1.x` (tag `v1`) | :white_check_mark: |
| `< v1` | :x: |

Always pin to a released tag — `uses: cnuss/actions-mutex@v1` (moving major)
or a full commit SHA for maximum supply-chain safety.

## Reporting a vulnerability

**Do not open a public issue for security problems.**

Report privately via GitHub's **Private Vulnerability Reporting**:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability** (Advisories → Report).
3. Describe the issue, affected versions, and reproduction steps.

You will get an acknowledgement within **3 business days**. Once confirmed, a
fix and a GitHub Security Advisory (with CVE if warranted) will be published,
and the moving `v1` tag updated.

## Scope

This action takes a lock by reserving an entry in the Actions cache service,
and releases it by deleting that entry through the REST API. When `run` is set,
it runs that caller-supplied bash script while holding the lock. It reads
runner-injected environment (`ACTIONS_RUNTIME_TOKEN`, `ACTIONS_RESULTS_URL`) and
uses the caller's own short-lived job tokens, which expire with the job. It
holds no secrets of its own and exfiltrates nothing off-runner.

In scope: code execution beyond the supplied `run` script, token leakage to
logs, releasing or stealing a lock held by a live job, cache-entry tampering
across keys, or supply-chain tampering with releases/tags.

Out of scope: the action faithfully running the `run` script a workflow author
provided (including one that prints secrets it was given), and the latency /
eventual-consistency characteristics of the underlying cache service.
