import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { createServer } from 'node:net';
import { mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs';
import { networkInterfaces, tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const script = fileURLToPath(new URL('./demo.mjs', import.meta.url));
const execute = (...args) => spawnSync(process.execPath, [script, ...args], { encoding: 'utf8', timeout: 10000 });

test('rejects missing arguments and loopback advertisement', () => {
  assert.notEqual(execute().status, 0);
  const result = execute('init', '--state', join(tmpdir(), 'unused-numax-demo'), '--lan-ip', '127.0.0.1', '--cluster', 'test', '--instance', 'a');
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /non-loopback local IPv4/);
});

const lan = Object.values(networkInterfaces()).flat().find(address => address?.family === 'IPv4' && !address.internal)?.address;
test('creates private loopback management config and refuses to overwrite it', { skip: !lan }, () => {
  const root = mkdtempSync(join(tmpdir(), 'numax-demo-test-'));
  try {
    const state = join(root, 'node');
    const args = ['init', '--state', state, '--lan-ip', lan, '--cluster', 'test-private', '--instance', 'a'];
    const first = execute(...args);
    assert.equal(first.status, 0, first.stderr);
    const token = readFileSync(join(state, 'management.token'), 'utf8').trim();
    assert.match(token, /^[0-9a-f]{64}$/);
    assert.ok(!first.stdout.includes(token) && !first.stderr.includes(token));
    const config = readFileSync(join(state, 'node.toml'), 'utf8');
    assert.match(config, /listen = "127\.0\.0\.1:9102"/);
    assert.match(config, /allow_non_loopback = false/);
    assert.match(config, /peers = \[\]/);
    assert.match(config, /op_log_limit = 128/);
    assert.match(config, /seen_ops_limit = 128/);
    assert.ok(!config.includes(token));
    if (process.platform !== 'win32') {
      assert.equal(statSync(state).mode & 0o777, 0o700);
      assert.equal(statSync(join(state, 'management.token')).mode & 0o777, 0o600);
    }
    assert.notEqual(execute(...args).status, 0);
    assert.equal(readFileSync(join(state, 'management.token'), 'utf8').trim(), token);
    const failure = execute('start', '--state', state, '--nx', join(root, 'missing-nx'), '--timeout', '1');
    assert.notEqual(failure.status, 0);
    assert.ok(!failure.stderr.includes(token));
  } finally {
    // Only resources exclusively created by this test are removed.
    rmSync(root, { recursive: true, force: true });
  }
});

test('real script lifecycle: SDK write, HTTP observation, stop and durable restart', {
  skip: process.env.NUMAX_DEMO_E2E !== '1', timeout: 90000,
}, async () => {
  assert.ok(lan, 'a real LAN interface is required');
  const root = mkdtempSync(join(tmpdir(), 'numax-demo-live-'));
  const state = join(root, 'node');
  const network = createServer();
  const management = createServer();
  let child;
  let childExit;
  let output = '';
  async function stop() {
    if (!child) return;
    child.kill('SIGTERM');
    const timer = setTimeout(() => child.kill('SIGKILL'), 20000);
    try {
      const [code, signal] = await childExit;
      assert.equal(signal, null, output);
      assert.equal(code, 0, output);
    } finally {
      clearTimeout(timer);
      child = undefined;
    }
  }
  function start() {
    output = '';
    child = spawn(process.execPath, [script, 'start', '--state', state], { stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.on('data', data => { output += data; });
    child.stderr.on('data', data => { output += data; });
    childExit = once(child, 'exit');
  }
  function command(...args) {
    const result = execute(...args, '--state', state);
    assert.equal(result.status, 0, `${result.stderr}\n${output}`);
    return result.stdout;
  }
  try {
    network.listen(0, lan);
    await once(network, 'listening');
    management.listen(0, '127.0.0.1');
    await once(management, 'listening');
    command('init', '--lan-ip', lan, '--cluster', `script-${process.pid}-${Date.now()}`,
      '--instance', 'script-node', '--network-port', String(network.address().port),
      '--management-port', String(management.address().port));
    await Promise.all([new Promise(resolve => network.close(resolve)), new Promise(resolve => management.close(resolve))]);
    start();
    // Observe readiness in the wrapper output; no fixed startup delay.
    async function ready() {
      const deadline = Date.now() + 15000;
      while (!output.includes('Daemon ready.')) {
        assert.equal(child.exitCode, null, output);
        assert.ok(Date.now() < deadline, `script readiness timeout: ${output}`);
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    }
    await ready();
    const initial = JSON.parse(command('wait', '--peers', '0', '--value', '0'));
    assert.equal(JSON.parse(command('increment')).value, '1');
    await stop();
    start();
    await ready();
    const recovered = JSON.parse(command('wait', '--peers', '0', '--value', '1'));
    assert.equal(recovered.node_id, initial.node_id);
    assert.equal(JSON.parse(command('increment')).value, '2');
    await stop();
  } finally {
    try { await stop(); } finally {
      network.close();
      management.close();
      rmSync(root, { recursive: true, force: true });
    }
  }
});