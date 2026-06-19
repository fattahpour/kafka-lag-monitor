import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import test from 'node:test';
import Module from 'node:module';

type ModuleLoader = (request: string, parent: unknown, isMain: boolean) => unknown;

const moduleWithLoad = Module as unknown as { _load: ModuleLoader };
const originalLoad = moduleWithLoad._load;

function loadProfileStore(workspacePath: string | null) {
  moduleWithLoad._load = ((request: string, parent: unknown, isMain: boolean) => {
    if (request === 'vscode') {
      return {
        workspace: {
          workspaceFolders: workspacePath ? [{ uri: { fsPath: workspacePath } }] : undefined,
          getConfiguration: () => ({ get: (_key: string, fallback: unknown) => fallback }),
        },
      };
    }
    return originalLoad(request, parent, isMain);
  }) as ModuleLoader;

  delete require.cache[require.resolve('../connection/profileStore')];
  return require('../connection/profileStore') as typeof import('../connection/profileStore');
}

function restoreProfileStore() {
  delete require.cache[require.resolve('../connection/profileStore')];
  moduleWithLoad._load = originalLoad;
}

function withTempWorkspace(fn: (workspacePath: string) => void) {
  const workspacePath = fs.mkdtempSync(path.join(os.tmpdir(), 'kafka-lag-monitor-'));
  try {
    fn(workspacePath);
  } finally {
    fs.rmSync(workspacePath, { recursive: true, force: true });
    restoreProfileStore();
  }
}

function writeConnectionsFile(workspacePath: string, contents: string) {
  const vscodeDir = path.join(workspacePath, '.vscode');
  fs.mkdirSync(vscodeDir, { recursive: true });
  fs.writeFileSync(path.join(vscodeDir, 'kafka-lag-monitor.connections.json'), contents, 'utf8');
}

test('getConnectionProfiles returns the default profile and reports an error without a workspace folder', () => {
  const store = loadProfileStore(null);
  const errors: string[] = [];

  assert.deepEqual(store.getConnectionProfiles((message) => errors.push(message)), [store.DEFAULT_PROFILE]);
  assert.deepEqual(errors, ['Open a workspace folder to manage Kafka connections']);

  restoreProfileStore();
});

test('getConnectionProfiles returns the default profile without an error when the file is missing', () => {
  withTempWorkspace((workspacePath) => {
    const store = loadProfileStore(workspacePath);
    const errors: string[] = [];

    assert.deepEqual(store.getConnectionProfiles((message) => errors.push(message)), [store.DEFAULT_PROFILE]);
    assert.deepEqual(errors, []);
  });
});

test('getConnectionProfiles returns no profiles and reports an error for invalid JSON', () => {
  withTempWorkspace((workspacePath) => {
    writeConnectionsFile(workspacePath, '{ nope');
    const store = loadProfileStore(workspacePath);
    const errors: string[] = [];

    assert.deepEqual(store.getConnectionProfiles((message) => errors.push(message)), []);
    assert.equal(errors.length, 1);
    assert.match(errors[0], /Failed to parse/);
  });
});

test('getConnectionProfiles accepts an empty connections array', () => {
  withTempWorkspace((workspacePath) => {
    writeConnectionsFile(workspacePath, JSON.stringify({ connections: [] }));
    const store = loadProfileStore(workspacePath);
    const errors: string[] = [];

    assert.deepEqual(store.getConnectionProfiles((message) => errors.push(message)), []);
    assert.deepEqual(errors, []);
  });
});

test('getConnectionProfiles keeps valid profiles and reports index-qualified errors for invalid entries', () => {
  withTempWorkspace((workspacePath) => {
    writeConnectionsFile(
      workspacePath,
      JSON.stringify({
        connections: [{ name: 'good', brokers: ['localhost:9092'] }, { name: 'bad' }],
      })
    );
    const store = loadProfileStore(workspacePath);
    const errors: string[] = [];

    assert.deepEqual(store.getConnectionProfiles((message) => errors.push(message)), [
      {
        name: 'good',
        brokers: ['localhost:9092'],
        sasl: null,
        ssl: false,
        clientId: 'kafka-lag-monitor',
      },
    ]);
    assert.equal(errors.length, 1);
    assert.match(errors[0], /^kafkaLagMonitor\.connections\[1\]:/);
    assert.match(errors[0], /brokers/);
  });
});

test('getConnectionProfiles returns valid profiles as-is', () => {
  withTempWorkspace((workspacePath) => {
    const profiles = [
      {
        name: 'local-cluster',
        brokers: ['localhost:9091', 'localhost:9092'],
        sasl: null,
        ssl: false,
        clientId: 'custom-client',
      },
    ];
    writeConnectionsFile(workspacePath, JSON.stringify({ connections: profiles }));
    const store = loadProfileStore(workspacePath);
    const errors: string[] = [];

    assert.deepEqual(store.getConnectionProfiles((message) => errors.push(message)), profiles);
    assert.deepEqual(errors, []);
  });
});

