import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import test from 'node:test';
import Module from 'node:module';

type ModuleLoader = (request: string, parent: unknown, isMain: boolean) => unknown;

const moduleWithLoad = Module as unknown as { _load: ModuleLoader };
const originalLoad = moduleWithLoad._load;

function loadConnectionStore(workspacePath: string | null) {
  moduleWithLoad._load = ((request: string, parent: unknown, isMain: boolean) => {
    if (request === 'vscode') {
      return {
        workspace: {
          workspaceFolders: workspacePath ? [{ uri: { fsPath: workspacePath } }] : undefined,
        },
      };
    }
    return originalLoad(request, parent, isMain);
  }) as ModuleLoader;

  delete require.cache[require.resolve('../connection/profileStore')];
  delete require.cache[require.resolve('../connection/connectionStore')];
  return require('../connection/connectionStore') as typeof import('../connection/connectionStore');
}

function restoreConnectionStore() {
  delete require.cache[require.resolve('../connection/profileStore')];
  delete require.cache[require.resolve('../connection/connectionStore')];
  moduleWithLoad._load = originalLoad;
}

async function withTempWorkspace(fn: (workspacePath: string) => Promise<void>) {
  const workspacePath = fs.mkdtempSync(path.join(os.tmpdir(), 'kafka-lag-monitor-'));
  try {
    await fn(workspacePath);
  } finally {
    fs.rmSync(workspacePath, { recursive: true, force: true });
    restoreConnectionStore();
  }
}

function readConnectionsFile(workspacePath: string): unknown {
  return JSON.parse(
    fs.readFileSync(path.join(workspacePath, '.vscode', 'kafka-lag-monitor.connections.json'), 'utf8')
  );
}

const profiles = [
  {
    name: 'local-cluster',
    brokers: ['localhost:9092'],
    sasl: null,
    ssl: false,
    clientId: 'kafka-lag-monitor',
  },
];

test('saveConnectionProfiles rejects without a workspace folder', async () => {
  const store = loadConnectionStore(null);

  await assert.rejects(() => store.saveConnectionProfiles(profiles), {
    message: 'Open a workspace folder to manage Kafka connections',
  });

  restoreConnectionStore();
});

test('saveConnectionProfiles creates .vscode and writes the connections file', async () => {
  await withTempWorkspace(async (workspacePath) => {
    const store = loadConnectionStore(workspacePath);

    await store.saveConnectionProfiles(profiles);

    assert.deepEqual(readConnectionsFile(workspacePath), { connections: profiles });
  });
});

test('saveConnectionProfiles overwrites an existing connections file', async () => {
  await withTempWorkspace(async (workspacePath) => {
    const vscodeDir = path.join(workspacePath, '.vscode');
    fs.mkdirSync(vscodeDir);
    fs.writeFileSync(path.join(vscodeDir, 'kafka-lag-monitor.connections.json'), '{"connections":[]}', 'utf8');
    const store = loadConnectionStore(workspacePath);

    await store.saveConnectionProfiles(profiles);

    assert.deepEqual(readConnectionsFile(workspacePath), { connections: profiles });
  });
});

