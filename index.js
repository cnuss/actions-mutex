'use strict';

// Distributed mutex on the Actions cache service.
//
// Acquire: CreateCacheEntry is an atomic reservation on key+version. Of N
// concurrent callers exactly one gets a signed upload URL; the rest get
// `already_exists`. The winner uploads and finalizes at once, which makes the
// entry visible to the REST API and therefore deletable.
//
// Release: REST DELETE /actions/caches/{id}. The next CreateCacheEntry on the
// same key+version succeeds immediately, so waiters simply keep retrying it.
//
// Abandoned locks: every holder also publishes `mutex/<key>/holder/<entry id>`
// naming its job. A waiter that finds the lock entry looks up that record and
// deletes the lock only once the holder's job has completed. The record is
// keyed by the lock's entry id, so a stale replica read can never attribute one
// holder's lock to another holder's job.
//
// Implementation notes:
//   - Dependency-free: no @actions/* packages, no node_modules, no build step.
//     Runs as a node24 action so the runner injects ACTIONS_RUNTIME_TOKEN /
//     ACTIONS_RESULTS_URL (it withholds them from `run:` shells).
//   - Every request uses a FRESH TCP socket (agent: false). The cache service
//     sits behind eventually-consistent read replicas; a keep-alive connection
//     pins a client to ONE replica.
//   - main and post are the same file; main saves STATE_post so the post run
//     knows which it is.

const fs = require('fs');
const https = require('https');
const crypto = require('crypto');
const { URL } = require('url');
const { spawnSync } = require('child_process');

const ENVELOPE_VERSION = 'mutex-v1';
// The cache service allows about 200 CreateCacheEntry and 1,500
// GetCacheEntryDownloadURL calls per minute, shared with every other job (and
// actions/cache) in the repository. Waiters therefore poll with the cheaper
// read and only race CreateCacheEntry when the lock looks free.
const POLL_DELAY_MS = 2000;
const POLL_JITTER_MS = 1000;
// A stale replica can keep showing a released lock; race anyway this often.
const CREATE_FALLBACK_MS = 15_000;
const MAX_POLL_DELAY_MS = 30_000;
const BACKOFF_DELAY_MS = 3000; // 429/5xx without Retry-After
const HOLDER_PUBLISH_ATTEMPTS = 4;
// Abandonment checks cost two REST calls, and GITHUB_TOKEN gets 1,000 REST
// requests per hour per repository.
const RECLAIM_CHECK_INTERVAL_MS = 60_000;
const SELF_LOOKUP_ATTEMPTS = 5;
const SELF_LOOKUP_DELAY_MS = 2000;

let DEBUG = false;

function log(msg) { process.stdout.write(`${msg}\n`); }
function warn(msg) { log(`::warning::${msg}`); }
function fail(msg) { log(`::error::${msg}`); process.exitCode = 1; }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// Strip Azure SAS secrets (and any token-ish query params) before logging URLs.
function redactUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    for (const p of ['sig', 'skoid', 'sktid']) {
      if (u.searchParams.has(p)) u.searchParams.set(p, 'REDACTED');
    }
    return `${u.origin}${u.pathname}${u.search}`;
  } catch { return urlStr; }
}

function logRequest(method, urlStr, res) {
  if (!DEBUG) return;
  const body = (res.text || '').replace(/\s+/g, ' ').trim().slice(0, 300);
  log(`::group::[req] ${method} ${res.status} ${redactUrl(urlStr)}`);
  log(`[req] headers: ${JSON.stringify(res.headers)}`);
  log(`[req] body: ${body}${(res.text || '').length > 300 ? ' …(truncated)' : ''}`);
  log('::endgroup::');
}

function getInput(name) {
  const v = process.env[`INPUT_${name.toUpperCase().replace(/ /g, '_')}`];
  return v === undefined ? '' : v.trim();
}

function appendCommandFile(file, name, value) {
  if (!file) return;
  const delimiter = 'ghadelimiter_' + crypto.randomBytes(16).toString('hex');
  fs.appendFileSync(file, `${name}<<${delimiter}\n${value ?? ''}\n${delimiter}\n`);
}
function setOutput(name, value) { appendCommandFile(process.env.GITHUB_OUTPUT, name, value); }
function saveState(name, value) { appendCommandFile(process.env.GITHUB_STATE, name, value); }

function versionFor(key) {
  return crypto.createHash('sha256').update(`${ENVELOPE_VERSION}:${key}`).digest('hex');
}
function lockKey(name) { return `mutex/${name}`; }
function holderKey(name, entryId) { return `mutex/${name}/holder/${entryId}`; }

// One request, one fresh socket. No keep-alive -> no replica stickiness.
function request(method, urlStr, headers, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const data = body == null ? null : (Buffer.isBuffer(body) ? body : Buffer.from(body));
    const opts = {
      method,
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      headers: { 'Connection': 'close', ...headers },
      agent: false,
    };
    if (data) opts.headers['Content-Length'] = data.length;
    const req = https.request(opts, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        const out = { status: res.statusCode, text: buf.toString('utf8'), headers: res.headers };
        logRequest(method, urlStr, out);
        resolve(out);
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

function parseJson(text) {
  try { return text ? JSON.parse(text) : {}; } catch { return {}; }
}

function cacheClient() {
  const token = process.env.ACTIONS_RUNTIME_TOKEN;
  const resultsUrl = process.env.ACTIONS_RESULTS_URL;
  if (!token || !resultsUrl) {
    throw new Error('ACTIONS_RUNTIME_TOKEN / ACTIONS_RESULTS_URL not present — is this running as an action in GitHub Actions?');
  }
  const base = `${resultsUrl.replace(/\/$/, '')}/twirp/github.actions.results.api.v1.CacheService`;
  return async (method, body) => {
    const res = await request('POST', `${base}/${method}`, {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    }, JSON.stringify(body));
    return { status: res.status, json: parseJson(res.text), text: res.text, headers: res.headers };
  };
}

function isThrottled(res) { return res.status === 429 || res.status >= 500; }

// Retry-After in seconds or as an HTTP date; BACKOFF_DELAY_MS when absent.
function retryAfterMs(headers) {
  const v = headers && headers['retry-after'];
  if (!v) return BACKOFF_DELAY_MS;
  const secs = Number(v);
  const ms = Number.isFinite(secs) ? secs * 1000 : Date.parse(v) - Date.now();
  return Math.min(Math.max(ms, 0), 60_000);
}

function restClient(token) {
  const api = (process.env.GITHUB_API_URL || 'https://api.github.com').replace(/\/$/, '');
  return async (method, path) => {
    const res = await request(method, `${api}${path}`, {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'actions-mutex',
    }, null);
    return { status: res.status, json: parseJson(res.text), text: res.text };
  };
}

// Uploads `body` to a won reservation and finalizes it. Returns the entry id,
// which is also the REST cache id.
async function publish(twirp, key, uploadUrl, body) {
  const bytes = Buffer.from(JSON.stringify(body), 'utf8');
  const put = await request('PUT', uploadUrl, {
    'x-ms-blob-type': 'BlockBlob',
    'Content-Type': 'application/octet-stream',
  }, bytes);
  if (put.status < 200 || put.status >= 300) throw new Error(`blob upload failed: HTTP ${put.status}`);
  const final = await twirp('FinalizeCacheEntryUpload', { key, version: versionFor(key), size_bytes: bytes.length });
  if (final.json.ok !== true) throw new Error(`FinalizeCacheEntryUpload not ok: ${final.text}`);
  const id = String(final.json.entry_id ?? final.json.entryId ?? '');
  if (!id) throw new Error(`FinalizeCacheEntryUpload returned no entry_id: ${final.text}`);
  return id;
}

// Downloads and parses a finalized entry, or null when it isn't visible (yet).
async function readEntry(twirp, key) {
  const dl = await twirp('GetCacheEntryDownloadURL', { key, version: versionFor(key), restore_keys: [] });
  const url = dl.json.ok === true && (dl.json.signed_download_url || dl.json.signedDownloadUrl);
  if (!url) return null;
  const res = await request('GET', url, {}, null);
  if (res.status !== 200) return null;
  const body = parseJson(res.text);
  return body.v === ENVELOPE_VERSION ? body : null;
}

// 204 and 404 both leave the entry gone; anything else is an error.
async function deleteEntry(rest, id) {
  const r = await rest('DELETE', `/repos/${process.env.GITHUB_REPOSITORY}/actions/caches/${id}`);
  if (r.status === 204 || r.status === 404) return r.status === 204;
  throw new Error(`deleting cache entry ${id}: HTTP ${r.status}: ${r.text}`);
}

// This job, found by runner name among the run attempt's in-progress jobs. The
// jobs API can lag the job's start by a few seconds.
async function findSelfJob(rest) {
  const { GITHUB_REPOSITORY: repo, GITHUB_RUN_ID: run, GITHUB_RUN_ATTEMPT: attempt, RUNNER_NAME: runner } = process.env;
  for (let tries = 1; tries <= SELF_LOOKUP_ATTEMPTS; tries += 1) {
    for (let page = 1; ; page += 1) {
      const r = await rest('GET', `/repos/${repo}/actions/runs/${run}/attempts/${attempt}/jobs?per_page=100&page=${page}`);
      if (r.status !== 200) throw new Error(`listing this run's jobs: HTTP ${r.status}: ${r.text}`);
      const jobs = r.json.jobs || [];
      const job = jobs.find((j) => j.runner_name === runner && j.status === 'in_progress');
      if (job) return { id: job.id, name: job.name, url: job.html_url };
      if (jobs.length < 100) break;
    }
    await sleep(SELF_LOOKUP_DELAY_MS);
  }
  throw new Error(`could not find this job (runner "${runner}") in run ${run} attempt ${attempt}`);
}

// Deletes the lock if its holder's job has completed. Never reclaims a lock
// whose holder it cannot identify.
async function reclaimIfAbandoned({ twirp, rest, name }) {
  const key = lockKey(name);
  const repo = process.env.GITHUB_REPOSITORY;
  const query = `key=${encodeURIComponent(key)}&ref=${encodeURIComponent(process.env.GITHUB_REF)}&sort=created_at&direction=desc&per_page=100`;
  const list = await rest('GET', `/repos/${repo}/actions/caches?${query}`);
  if (list.status !== 200) { warn(`listing cache entries: HTTP ${list.status}`); return false; }
  const entry = (list.json.actions_caches || []).find((c) => c.key === key && c.version === versionFor(key));
  if (!entry) return false;

  const holder = await readEntry(twirp, holderKey(name, entry.id));
  if (!holder || !holder.job_id) return false;
  const job = await rest('GET', `/repos/${repo}/actions/jobs/${holder.job_id}`);
  if (job.status !== 200 || job.json.status !== 'completed') return false;

  log(`[mutex] holder "${holder.job_name}" (run ${holder.run_id}) ended without releasing "${name}" — reclaiming`);
  await deleteEntry(rest, entry.id);
  return true;
}

// Waits for the lock: reads until it looks free, then races CreateCacheEntry.
// `holder` is published alongside the lock so waiters can tell when it was
// abandoned.
async function acquire(ctx, holder) {
  const { twirp, name, timeoutMs } = ctx;
  const key = lockKey(name);
  const version = versionFor(key);
  const start = Date.now();
  const replies = {};
  const count = (reply) => { replies[reply] = (replies[reply] || 0) + 1; return replies[reply]; };
  let lastReclaimCheck = start;
  let lastCreate = 0;
  let looksFree = true;
  let delay = POLL_DELAY_MS;
  let creates = 0;
  let loggedThrottle = false;

  for (;;) {
    let res;
    if (looksFree || Date.now() - lastCreate >= CREATE_FALLBACK_MS) {
      lastCreate = Date.now();
      creates += 1;
      res = await twirp('CreateCacheEntry', { key, version });
      const uploadUrl = res.json.signed_upload_url || res.json.signedUploadUrl;
      if (uploadUrl) {
        // If this process dies before release, post finalizes and deletes from here.
        saveState('upload_url', uploadUrl);
        const record = { v: ENVELOPE_VERSION, ...holder, acquired_at: new Date().toISOString() };
        const entryId = await publish(twirp, key, uploadUrl, record);
        saveState('entry_id', entryId);
        const holderId = await publishHolder(twirp, name, entryId, record);
        saveState('holder_id', holderId);
        return { entryId, holderId, creates, replies, waitMs: Date.now() - start };
      }
      if (!isThrottled(res) && res.json.code !== 'already_exists') {
        throw new Error(`unexpected CreateCacheEntry response (status ${res.status}): ${res.text}`);
      }
      count(`create:${res.json.code || `http_${res.status}`}`);
      looksFree = false;
      if (creates === 1 && !isThrottled(res)) log(`[mutex] "${name}" is held — waiting`);
    } else {
      res = await twirp('GetCacheEntryDownloadURL', { key, version, restore_keys: [] });
      if (!isThrottled(res)) {
        looksFree = res.json.ok !== true;
        count(looksFree ? 'read:free' : 'read:held');
        if (looksFree) continue;
      } else {
        count(`read:http_${res.status}`);
      }
    }

    if (isThrottled(res)) {
      if (!loggedThrottle) {
        loggedThrottle = true;
        log(`[mutex] cache service throttled (HTTP ${res.status}, retry-after ${res.headers['retry-after'] || 'unset'}): ${res.text.slice(0, 200)}`);
      }
      delay = Math.min(delay * 2, MAX_POLL_DELAY_MS);
    } else {
      delay = Math.max(delay * 0.9, POLL_DELAY_MS);
    }

    if (Date.now() - lastReclaimCheck >= RECLAIM_CHECK_INTERVAL_MS) {
      lastReclaimCheck = Date.now();
      if (await reclaimIfAbandoned(ctx)) { looksFree = true; continue; }
    }
    if (Date.now() - start >= timeoutMs) {
      throw new Error(`timed out after ${Math.round(timeoutMs / 1000)}s waiting for "${name}"`);
    }
    // After a 429, spread waiters across the next window instead of all
    // retrying the moment it opens.
    const wait = isThrottled(res) ? retryAfterMs(res.headers) + Math.random() * delay : delay + Math.random() * POLL_JITTER_MS;
    await sleep(wait);
  }
}

// The holder record makes an abandoned lock reclaimable, so it is worth
// waiting out a throttled window for.
async function publishHolder(twirp, name, entryId, record) {
  const key = holderKey(name, entryId);
  let create;
  for (let tries = 1; tries <= HOLDER_PUBLISH_ATTEMPTS; tries += 1) {
    create = await twirp('CreateCacheEntry', { key, version: versionFor(key) });
    const uploadUrl = create.json.signed_upload_url || create.json.signedUploadUrl;
    if (uploadUrl) return publish(twirp, key, uploadUrl, record);
    if (!isThrottled(create)) break;
    await sleep(retryAfterMs(create.headers) + Math.random() * POLL_JITTER_MS);
  }
  warn(`could not publish holder record ${key}; if this job dies holding the lock, delete it by hand: ${create.text}`);
  return '';
}

async function release({ rest, name }, entryId, holderId) {
  await deleteEntry(rest, entryId);
  if (holderId) {
    try { await deleteEntry(rest, holderId); } catch (err) { warn(`leaving holder record ${holderId}: ${err.message}`); }
  }
  saveState('released', 'true');
  log(`[mutex] released "${name}"`);
}

function loadContext() {
  DEBUG = process.env.ACTIONS_STEP_DEBUG === 'true';
  const name = getInput('key');
  const token = getInput('github-token');
  if (!name) throw new Error('input `key` is required');
  if (!token) throw new Error('input `github-token` is required');
  const timeoutSeconds = parseInt(getInput('timeout-seconds') || '3600', 10);
  return { twirp: cacheClient(), rest: restClient(token), name, timeoutMs: timeoutSeconds * 1000 };
}

async function main() {
  saveState('post', 'true');
  const ctx = loadContext();
  const script = getInput('run');

  const self = await findSelfJob(ctx.rest);
  const holder = {
    repository: process.env.GITHUB_REPOSITORY,
    ref: process.env.GITHUB_REF,
    run_id: process.env.GITHUB_RUN_ID,
    run_attempt: process.env.GITHUB_RUN_ATTEMPT,
    job_id: self.id,
    job_name: self.name,
    job_url: self.url,
  };

  const got = await acquire(ctx, holder);
  const waitSeconds = Math.round(got.waitMs / 1000);
  log(`[mutex] acquired "${ctx.name}" after ${waitSeconds}s (entry ${got.entryId}); replies while waiting: ${JSON.stringify(got.replies)}`);
  setOutput('wait-seconds', String(waitSeconds));

  if (!script) {
    log(`[mutex] holding "${ctx.name}" until the end of the job`);
    return;
  }

  let exit;
  try {
    const child = spawnSync('bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', script], { stdio: 'inherit' });
    if (child.error) log(`[mutex] script spawn error: ${child.error.message}`);
    exit = child.status === null ? 1 : child.status;
  } finally {
    await release(ctx, got.entryId, got.holderId);
  }
  if (exit !== 0) fail(`script exited ${exit}`);
}

async function post() {
  if (process.env.STATE_released === 'true') return;
  const entryId = process.env.STATE_entry_id;
  const uploadUrl = process.env.STATE_upload_url;
  if (!entryId && !uploadUrl) return;

  const ctx = loadContext();
  // Won but never finalized: finalize so the entry becomes deletable.
  const id = entryId || await publish(ctx.twirp, lockKey(ctx.name), uploadUrl, { v: ENVELOPE_VERSION });
  await release(ctx, id, process.env.STATE_holder_id);
}

module.exports = {
  ENVELOPE_VERSION, versionFor, lockKey, holderKey,
  cacheClient, restClient, publish, readEntry, deleteEntry, findSelfJob, reclaimIfAbandoned,
};

if (require.main === module) {
  const run = process.env.STATE_post === 'true' ? post : main;
  run().catch((err) => fail(err && err.stack ? err.stack : String(err)));
}
