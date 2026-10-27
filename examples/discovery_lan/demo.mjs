#!/usr/bin/env node
// No dependencies, remote management, shell evaluation, or automatic deletion.
import { randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { open, mkdir, readFile, writeFile } from 'node:fs/promises';
import { networkInterfaces } from 'node:os';
import { resolve, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { setTimeout as pollDelay } from 'node:timers/promises';

const here = dirname(fileURLToPath(import.meta.url));
const SNAPSHOT = Buffer.from('discovery-lan').toString('base64url');
const RETENTION = 128; // Operation count, NOT a time window.
const { values: options, positionals } = parseArgs({
  allowPositionals: true,
  options: Object.fromEntries([
    'state', 'lan-ip', 'cluster', 'instance', 'network-port', 'management-port',
    'nx', 'value', 'peers', 'timeout',
  ].map(name => [name, { type: 'string' }])),
});

function check(condition, message) {
  if (!condition) throw new Error(message);
}

function numberOption(name, fallback, max) {
  const text = options[name] ?? String(fallback);
  check(/^\d+$/.test(text), `--${name} must be an integer`);
  const value = Number(text);
  check(Number.isSafeInteger(value) && value >= 1 && value <= max, `invalid --${name}`);
  return value;
}

async function initialize(state) {
  for (const name of ['lan-ip', 'cluster', 'instance']) {
    check(options[name], `init requires --${name}`);
  }
  const ip = options['lan-ip'];
  const local = Object.values(networkInterfaces()).flat().some(
    address => address?.family === 'IPv4' && !address.internal && address.address === ip,
  );
  check(local, '--lan-ip must be an actual non-loopback local IPv4 interface');
  for (const name of ['cluster', 'instance']) {
    check(/^[a-zA-Z0-9-]{1,50}$/.test(options[name]), `--${name}: use 1–50 letters, digits or hyphens`);
  }
  const network = numberOption('network-port', 9000, 65535);
  const management = numberOption('management-port', 9102, 65535);
  check(network !== management, 'network and management ports must differ');
  // Exclusive creation prevents overwriting an existing datastore/configuration.
  // The parent directory must exist. Partial initialization is preserved on errors.
  await mkdir(state, { mode: 0o700 });
  const token = randomBytes(32).toString('hex');
  await writeFile(join(state, 'management.token'), `${token}\n`, { flag: 'wx', mode: 0o600 });
  const q = JSON.stringify;
  const config = `[network]
listen = ${q(`${ip}:${network}`)}
peers = []

[storage]
datastore_path = ${q(join(state, 'data'))}

[management]
listen = "127.0.0.1:${management}"
token_file = ${q(join(state, 'management.token'))}
allow_non_loopback = false

[discovery]
mode = "mdns"
cluster_id = ${q(options.cluster)}
instance_name = ${q(options.instance)}
advertised_endpoint = ${q(`${ip}:${network}`)}
max_candidates = 8
max_instances = 8

[limits]
max_peers = 4
queued_ops_limit = 128
op_log_limit = ${RETENTION}
seen_ops_limit = ${RETENTION}
anti_entropy_interval = "200ms"
reconnect_initial_delay = "100ms"
reconnect_max_delay = "1s"
`;
  await writeFile(join(state, 'node.toml'), config, { flag: 'wx', mode: 0o600 });
  await writeFile(join(state, 'control.json'), JSON.stringify({ management }), { flag: 'wx', mode: 0o600 });
  console.log(`Initialized ${state}; management stays on loopback; retention=${RETENTION} operations.`);
}

async function client(state) {
  const { management } = JSON.parse(await readFile(join(state, 'control.json'), 'utf8'));
  check(Number.isInteger(management) && management > 0 && management <= 65535, 'invalid management port');
  const token = (await readFile(join(state, 'management.token'), 'utf8')).trim();
  check(/^[0-9a-f]{64}$/.test(token), 'invalid token file');
  return async function request(path, { method = 'GET', body, type, allowed = [200] } = {}) {
    const response = await fetch(`http://127.0.0.1:${management}/api/v1/${path}`, {
      method, body, redirect: 'error', signal: AbortSignal.timeout(5000),
      headers: { Authorization: `Bearer ${token}`, ...(type ? { 'Content-Type': type } : {}) },
    });
    // Do not echo headers, tokens, or arbitrary response bodies in errors.
    check(allowed.includes(response.status), `${method} ${path}: HTTP ${response.status}`);
    return response;
  };
}

async function register(request, mode) {
  const wasm = await readFile(join(here, 'target', mode, 'wasm32-unknown-unknown', 'release', 'discovery_lan.wasm'));
  const response = await request('modules', { method: 'POST', body: wasm, type: 'application/wasm', allowed: [200, 201] });
  const { id } = await response.json();
  check(/^[0-9a-f]{64}$/.test(id), 'invalid module id');
  return id;
}

async function run(request, module) {
  await request(`modules/${module}/runs`, { method: 'POST', allowed: [204] });
}

async function snapshot(request, reader, state) {
  await run(request, reader); // Read-only CRDT operation; refreshes LOCAL KV projection.
  const response = await request(`keys/${SNAPSHOT}`);
  const [nodeId, value, extra] = (await response.text()).split('\n');
  check(nodeId && /^\d+$/.test(value) && extra === undefined, 'invalid guest snapshot');
  const identityPath = join(state, 'identity');
  try {
    await writeFile(identityPath, nodeId, { flag: 'wx', mode: 0o600 });
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
    check(await readFile(identityPath, 'utf8') === nodeId, 'node identity changed; do not replace the datastore');
  }
  const peers = await (await request('peers?limit=10')).json();
  check(peers.next_cursor === null && Array.isArray(peers.items), 'unexpected peer page');
  const ids = peers.items.map(peer => peer.node_id);
  check(!ids.includes(nodeId), 'unexpected self connection');
  // Management lists connections; inbound/outbound links may share an identity.
  return { node_id: nodeId, value, peer_ids: [...new Set(ids)].sort(), connections: peers.items };
}

async function waitFor(label, timeout, condition, isAlive = () => true) {
  const deadline = performance.now() + timeout;
  let last = 'condition not met';
  do {
    check(isAlive(), `${label}: daemon exited`);
    try {
      const result = await condition();
      if (result) return result;
    } catch (error) {
      last = error.message;
    }
    if (performance.now() >= deadline) break;
    await pollDelay(200); // Condition polling only; no fixed startup/settling sleep.
  } while (performance.now() < deadline);
  throw new Error(`${label}: timeout (${last})`);
}

async function start(state, timeout) {
  const request = await client(state);
  // Check configuration exists before spawning. nx remains the configuration validator.
  await readFile(join(state, 'node.toml'));
  const log = await open(join(state, 'daemon.log'), 'a', 0o600);
  const env = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('NX_')));
  let child;
  let ended = false;
  let exit;
  try {
    child = spawn(resolve(options.nx ?? join(here, '..', '..', 'target', 'debug', 'nx')),
      ['serve', '--config', join(state, 'node.toml')],
      { env, stdio: ['ignore', log.fd, log.fd] });
    // Attach before the first await: spawn errors can arrive on the next tick.
    exit = new Promise(resolveExit => {
      child.once('error', () => { ended = true; resolveExit({ error: 'cannot start nx; check --nx' }); });
      child.once('exit', (code, signal) => { ended = true; resolveExit({ code, signal }); });
    });
  } finally {
    await log.close();
  }
  let stopping;
  function stop() {
    stopping ??= (async () => {
      if (ended) return;
      child.kill('SIGTERM');
      const escalation = setTimeout(() => child.kill('SIGKILL'), 15000);
      try { await exit; } finally { clearTimeout(escalation); }
    })();
    return stopping;
  }
  const signal = () => { void stop(); };
  process.on('SIGINT', signal);
  process.on('SIGTERM', signal);
  try {
    await waitFor('daemon readiness', timeout, async () => {
      await request('ready');
      return true;
    }, () => !ended);
    check(!ended, 'daemon exited during readiness');
    console.log(`Daemon ready. Use another local terminal for status/increment/wait. Ctrl-C stops it; ${state} is preserved.`);
    const result = await exit;
    check(result.code === 0, result.error ?? `daemon exited (code=${result.code}, signal=${result.signal}); inspect private daemon.log`);
  } finally {
    await stop();
    process.off('SIGINT', signal);
    process.off('SIGTERM', signal);
  }
}

async function main() {
  check(positionals.length === 1 && ['init', 'start', 'increment', 'status', 'wait'].includes(positionals[0]),
    'Usage: node demo.mjs init|start|increment|status|wait --state PATH (see README)');
  check(options.state, '--state is required');
  const state = resolve(options.state);
  const action = positionals[0];
  const timeout = numberOption('timeout', 60, 600) * 1000;
  if (action === 'init') return initialize(state);
  if (action === 'start') return start(state, timeout);
  if (action === 'wait') {
    check(options.value !== undefined || options.peers !== undefined, 'wait requires --value and/or --peers');
    if (options.value !== undefined) check(/^\d+$/.test(options.value), '--value must be a nonnegative integer');
    if (options.peers !== undefined) check(/^[0-2]$/.test(options.peers), '--peers must be 0, 1 or 2');
  }
  const request = await client(state);
  if (action === 'increment') {
    // Never retry a mutation: a lost HTTP response has an ambiguous outcome.
    await run(request, await register(request, 'writer'));
  }
  const reader = await register(request, 'reader');
  const observe = () => snapshot(request, reader, state);
  const result = action === 'wait'
    ? await waitFor('convergence', timeout, async () => {
      const current = await observe();
      const valueMatches = options.value === undefined || BigInt(current.value) === BigInt(options.value);
      const peersMatch = options.peers === undefined || current.peer_ids.length === Number(options.peers);
      return valueMatches && peersMatch ? current : false;
    })
    : await observe();
  console.log(JSON.stringify(result, null, 2));
}

main().catch(error => {
  console.error(`discovery_lan: ${error.message}`);
  process.exitCode = 1;
});