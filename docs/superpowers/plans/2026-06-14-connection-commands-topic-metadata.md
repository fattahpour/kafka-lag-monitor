# Connection Commands + Topic Metadata Webview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish roadmap Phase 1 — Add/Edit/Remove/Reconnect connection commands (with SASL credential wiring) and a Topic Metadata webview reachable by clicking a topic in the sidebar.

**Architecture:** A new `connectionWizard.ts` holds pure, unit-tested input validators plus a vscode QuickInput-based wizard; `connectionStore.ts` persists profiles to Global settings; `connectionCommands.ts` wires the wizard + `ConnectionManager` + `SecretStorage` into four commands. `ConnectionManager` gains a generation-counter guard and `reconnect()`. `extension.ts`'s admin-client factory becomes async and reads SASL credentials from `SecretStorage`. `webviews/topicMetadataPanel.ts` holds pure HTML-render functions plus a singleton `TopicMetadataPanel` glue class.

**Tech Stack:** TypeScript, vscode Extension API (QuickInput, WebviewPanel, SecretStorage, workspace configuration), kafkajs, node:test.

**Reference spec:** `docs/superpowers/specs/2026-06-14-connection-commands-topic-metadata-design.md`

---

## Task 1: Connection-name and broker-list validators

**Files:**
- Modify: `src/connection/profileValidation.ts:4`
- Create: `src/connection/connectionWizard.ts`
- Test: `src/test/connectionWizard.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `src/test/connectionWizard.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { parseBrokerList, validateProfileName } from '../connection/connectionWizard';

test('validateProfileName rejects an empty name', () => {
  assert.match(validateProfileName('', []) ?? '', /must not be empty/);
});

test('validateProfileName rejects a name containing a dot', () => {
  assert.match(validateProfileName('my.cluster', []) ?? '', /must not contain "\."/);
});

test('validateProfileName rejects a duplicate name', () => {
  assert.match(validateProfileName('local-cluster', ['local-cluster']) ?? '', /already exists/);
});

test('validateProfileName accepts a valid, unique name', () => {
  assert.equal(validateProfileName('local-cluster', ['other-cluster']), null);
});

test('parseBrokerList parses comma-separated host:port entries', () => {
  const result = parseBrokerList('localhost:9091, localhost:9092 ,localhost:9095');
  assert.deepEqual(result, {
    brokers: ['localhost:9091', 'localhost:9092', 'localhost:9095'],
    errors: [],
  });
});

test('parseBrokerList reports malformed entries but keeps valid ones', () => {
  const result = parseBrokerList('localhost:9091, not-a-broker');
  assert.deepEqual(result.brokers, ['localhost:9091']);
  assert.match(result.errors[0], /host:port/);
});

test('parseBrokerList reports an error for empty input', () => {
  const result = parseBrokerList('   ');
  assert.deepEqual(result.brokers, []);
  assert.match(result.errors[0], /At least one broker/);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error `Cannot find module '../connection/connectionWizard'`.

- [ ] **Step 3: Export `BROKER_PATTERN` from `profileValidation.ts`**

In `src/connection/profileValidation.ts`, change line 4 from:

```typescript
const BROKER_PATTERN = /^[\w.-]+:\d+$/;
```

to:

```typescript
export const BROKER_PATTERN = /^[\w.-]+:\d+$/;
```

- [ ] **Step 4: Create `src/connection/connectionWizard.ts`**

```typescript
import { BROKER_PATTERN } from './profileValidation';

export function validateProfileName(name: string, existingNames: string[]): string | null {
  const trimmed = name.trim();
  if (trimmed === '') {
    return '"name" must not be empty';
  }
  if (trimmed.includes('.')) {
    return '"name" must not contain "." (used as a separator in stored credential keys)';
  }
  if (existingNames.includes(trimmed)) {
    return `A connection named "${trimmed}" already exists`;
  }
  return null;
}

export interface ParsedBrokerList {
  brokers: string[];
  errors: string[];
}

export function parseBrokerList(input: string): ParsedBrokerList {
  const brokers: string[] = [];
  const errors: string[] = [];
  for (const raw of input.split(',')) {
    const broker = raw.trim();
    if (broker === '') continue;
    if (!BROKER_PATTERN.test(broker)) {
      errors.push(`"${broker}" must look like "host:port"`);
    } else {
      brokers.push(broker);
    }
  }
  if (brokers.length === 0 && errors.length === 0) {
    errors.push('At least one broker is required');
  }
  return { brokers, errors };
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `npm test`
Expected: PASS — 42 tests (35 existing + 7 new).

- [ ] **Step 6: Commit**

```bash
git add src/connection/profileValidation.ts src/connection/connectionWizard.ts src/test/connectionWizard.test.ts
git commit -m "feat: add connection name and broker-list validators"
```

---

## Task 2: ConnectionManager — async factory, generation guard, and reconnect()

**Files:**
- Modify: `src/connection/connectionManager.ts`
- Modify: `src/test/connectionManager.test.ts`

- [ ] **Step 1: Write the failing tests**

Replace the full contents of `src/test/connectionManager.test.ts` with:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { ConnectionManager } from '../connection/connectionManager';
import { KafkaAdminClient } from '../kafka/adminClient';
import { ConnectionProfile } from '../connection/types';

function createFakeAdminClient(overrides: Partial<KafkaAdminClient> = {}): KafkaAdminClient {
  const notImplemented = () => {
    throw new Error('not implemented in fake');
  };
  return {
    connect: async () => {},
    disconnect: async () => {},
    listTopics: notImplemented,
    fetchTopicMetadata: notImplemented,
    describeConfigs: notImplemented,
    listGroups: notImplemented,
    fetchOffsets: notImplemented,
    fetchTopicOffsets: notImplemented,
    ...overrides,
  } as KafkaAdminClient;
}

const profile: ConnectionProfile = {
  name: 'local-cluster',
  brokers: ['localhost:9091'],
  sasl: null,
  ssl: false,
  clientId: 'kafka-lag-monitor',
};

test('connect transitions idle -> connected and exposes an AdminService', async () => {
  const client = createFakeAdminClient();
  const manager = new ConnectionManager(async () => client);

  assert.equal(manager.getState(profile.name).status, 'idle');

  await manager.connect(profile);

  assert.equal(manager.getState(profile.name).status, 'connected');
  assert.ok(manager.getAdminService(profile.name));
});

test('connect sets status to error with the failure message when connect() rejects', async () => {
  const client = createFakeAdminClient({
    connect: async () => {
      throw new Error('ECONNREFUSED');
    },
  });
  const manager = new ConnectionManager(async () => client);

  await assert.rejects(() => manager.connect(profile), /ECONNREFUSED/);

  assert.deepEqual(manager.getState(profile.name), { status: 'error', error: 'ECONNREFUSED' });
  assert.equal(manager.getAdminService(profile.name), undefined);
});

test('disconnect resets status to idle and re-creates the client on the next connect', async () => {
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    return createFakeAdminClient();
  });

  await manager.connect(profile);
  await manager.disconnect(profile.name);

  assert.equal(manager.getState(profile.name).status, 'idle');
  assert.equal(manager.getAdminService(profile.name), undefined);

  await manager.connect(profile);

  assert.equal(createCount, 2);
});

test('getState returns idle for a profile that has never been connected', () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient());
  assert.deepEqual(manager.getState('never-seen'), { status: 'idle' });
});

test('reconnect discards the old client, creates a new one, and reconnects', async () => {
  let disconnectedOld = false;
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    if (createCount === 1) {
      return createFakeAdminClient({
        disconnect: async () => {
          disconnectedOld = true;
        },
      });
    }
    return createFakeAdminClient();
  });

  await manager.connect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  await manager.reconnect(profile);

  assert.equal(createCount, 2);
  assert.ok(disconnectedOld);
  assert.equal(manager.getState(profile.name).status, 'connected');
});

test('reconnect works when the profile was never connected', async () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient());

  await manager.reconnect(profile);

  assert.equal(manager.getState(profile.name).status, 'connected');
});

test('reconnect sets status to error when the new client fails to connect', async () => {
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    if (createCount === 1) return createFakeAdminClient();
    return createFakeAdminClient({
      connect: async () => {
        throw new Error('ECONNREFUSED');
      },
    });
  });

  await manager.connect(profile);
  await assert.rejects(() => manager.reconnect(profile), /ECONNREFUSED/);

  assert.deepEqual(manager.getState(profile.name), { status: 'error', error: 'ECONNREFUSED' });
});

test('a stale connect() failure does not overwrite a newer reconnect() success', async () => {
  let rejectFirstConnect: (err: Error) => void = () => {};
  const firstClient = createFakeAdminClient({
    connect: () =>
      new Promise<void>((_resolve, reject) => {
        rejectFirstConnect = reject;
      }),
  });
  const secondClient = createFakeAdminClient();
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    return createCount === 1 ? firstClient : secondClient;
  });

  const connectPromise = manager.connect(profile).catch(() => undefined);

  await manager.reconnect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  rejectFirstConnect(new Error('ECONNREFUSED'));
  await connectPromise;

  assert.equal(manager.getState(profile.name).status, 'connected');
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile errors, e.g. `Argument of type '() => KafkaAdminClient' is not assignable to parameter of type 'AdminClientFactory'` and `Property 'reconnect' does not exist on type 'ConnectionManager'`.

- [ ] **Step 3: Replace `src/connection/connectionManager.ts`**

```typescript
import { ConnectionProfile, ConnectionStatus } from './types';
import { KafkaAdminClient } from '../kafka/adminClient';
import { AdminService } from '../kafka/adminService';

export interface ConnectionState {
  status: ConnectionStatus;
  error?: string;
}

export type AdminClientFactory = (profile: ConnectionProfile) => Promise<KafkaAdminClient>;

export class ConnectionManager {
  private readonly clients = new Map<string, KafkaAdminClient>();
  private readonly states = new Map<string, ConnectionState>();
  private readonly generations = new Map<string, number>();

  constructor(private readonly createAdminClient: AdminClientFactory) {}

  getState(profileName: string): ConnectionState {
    return this.states.get(profileName) ?? { status: 'idle' };
  }

  private nextGeneration(profileName: string): number {
    const gen = (this.generations.get(profileName) ?? 0) + 1;
    this.generations.set(profileName, gen);
    return gen;
  }

  private isCurrentGeneration(profileName: string, gen: number): boolean {
    return this.generations.get(profileName) === gen;
  }

  async connect(profile: ConnectionProfile): Promise<void> {
    const gen = this.nextGeneration(profile.name);
    this.states.set(profile.name, { status: 'connecting' });
    try {
      let client = this.clients.get(profile.name);
      if (!client) {
        client = await this.createAdminClient(profile);
        this.clients.set(profile.name, client);
      }
      await client.connect();
      if (this.isCurrentGeneration(profile.name, gen)) {
        this.states.set(profile.name, { status: 'connected' });
      }
    } catch (err) {
      if (this.isCurrentGeneration(profile.name, gen)) {
        this.states.set(profile.name, { status: 'error', error: (err as Error).message });
      }
      throw err;
    }
  }

  async reconnect(profile: ConnectionProfile): Promise<void> {
    const gen = this.nextGeneration(profile.name);
    this.states.set(profile.name, { status: 'connecting' });

    const existing = this.clients.get(profile.name);
    if (existing) {
      this.clients.delete(profile.name);
      await existing.disconnect().catch(() => undefined);
    }

    try {
      const client = await this.createAdminClient(profile);
      this.clients.set(profile.name, client);
      await client.connect();
      if (this.isCurrentGeneration(profile.name, gen)) {
        this.states.set(profile.name, { status: 'connected' });
      }
    } catch (err) {
      if (this.isCurrentGeneration(profile.name, gen)) {
        this.states.set(profile.name, { status: 'error', error: (err as Error).message });
      }
      throw err;
    }
  }

  async disconnect(profileName: string): Promise<void> {
    const client = this.clients.get(profileName);
    if (client) {
      await client.disconnect();
      this.clients.delete(profileName);
    }
    this.states.set(profileName, { status: 'idle' });
  }

  getAdminService(profileName: string): AdminService | undefined {
    const state = this.getState(profileName);
    const client = this.clients.get(profileName);
    if (state.status !== 'connected' || !client) return undefined;
    return new AdminService(client);
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 46`, `# pass 46`, `# fail 0` (42 from Task 1, minus the 4 old tests in this file, plus the 8 new tests above).

- [ ] **Step 5: Commit**

```bash
git add src/connection/connectionManager.ts src/test/connectionManager.test.ts
git commit -m "feat: add reconnect() and a generation-counter race guard to ConnectionManager"
```

---

## Task 3: SASL credential wiring into the kafka client factory

**Files:**
- Modify: `src/extension.ts` (full rewrite)
- Modify: `package.json:36`

No new unit tests — this is vscode/kafkajs integration glue, matching the existing compile-only treatment of `extension.ts`.

- [ ] **Step 1: Replace `src/extension.ts`**

```typescript
import { Kafka, SASLOptions } from 'kafkajs';
import * as vscode from 'vscode';
import { ConnectionManager } from './connection/connectionManager';
import { getConnectionProfiles, getLagThresholds } from './connection/profileStore';
import { getCredential } from './connection/secretStore';
import { SaslMechanism } from './connection/types';
import { createKafkaAdminClient } from './kafka/kafkaAdminAdapter';
import { createKafkaLogCreator } from './logging/kafkaLogCreator';
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';

function buildSasl(mechanism: SaslMechanism, username: string, password: string): SASLOptions {
  switch (mechanism) {
    case 'plain':
      return { mechanism: 'plain', username, password };
    case 'scram-sha-256':
      return { mechanism: 'scram-sha-256', username, password };
    case 'scram-sha-512':
      return { mechanism: 'scram-sha-512', username, password };
  }
}

export function activate(context: vscode.ExtensionContext): void {
  const output = vscode.window.createOutputChannel('Kafka Lag Monitor');
  output.appendLine('Kafka Lag Monitor activated');
  context.subscriptions.push(output);

  const connectionManager = new ConnectionManager(async (profile) => {
    const sasl = profile.sasl
      ? buildSasl(
          profile.sasl.mechanism,
          (await getCredential(context.secrets, profile.name, 'username')) ?? '',
          (await getCredential(context.secrets, profile.name, 'password')) ?? '',
        )
      : undefined;
    const kafka = new Kafka({
      clientId: profile.clientId,
      brokers: profile.brokers,
      ssl: profile.ssl,
      sasl,
      logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
    });
    return createKafkaAdminClient(kafka.admin());
  });

  const onConfigError = (message: string) => output.appendLine(`[CONFIG] ${message}`);
  const profiles = getConnectionProfiles(onConfigError);
  const thresholds = getLagThresholds();

  const explorer = new KafkaExplorerProvider(profiles, connectionManager, thresholds);
  const treeView = vscode.window.createTreeView('kafkaLagMonitor.explorer', { treeDataProvider: explorer });
  context.subscriptions.push(treeView);

  context.subscriptions.push(vscode.commands.registerCommand('kafkaLagMonitor.refresh', () => explorer.refresh()));
}

export function deactivate(): void {}
```

- [ ] **Step 2: Update the `kafkaLagMonitor.connections` description in `package.json`**

In `package.json`, find the `kafkaLagMonitor.connections` property (around line 33-37) and change its `description` from:

```json
          "description": "Kafka cluster connection profiles. Each entry: { name, brokers: [\"host:port\", ...], sasl: null, ssl, clientId }. SASL is not yet supported."
```

to:

```json
          "description": "Kafka cluster connection profiles. Each entry: { name, brokers: [\"host:port\", ...], sasl: null or { mechanism: \"plain\"|\"scram-sha-256\"|\"scram-sha-512\" }, ssl, clientId }. Use the 'Kafka: Add Connection' command to create one — SASL credentials are stored in SecretStorage, not in settings."
```

- [ ] **Step 3: Verify compile and tests**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 46`, `# pass 46`, `# fail 0`.

- [ ] **Step 4: Commit**

```bash
git add src/extension.ts package.json
git commit -m "feat: wire SASL credentials from SecretStorage into the kafka client factory"
```

---

## Task 4: Add/Edit/Remove/Reconnect connection commands

**Files:**
- Create: `src/connection/connectionStore.ts`
- Create: `src/connection/connectionCommands.ts`
- Modify: `src/treeView/treeItems.ts:9`
- Modify: `src/treeView/kafkaExplorerProvider.ts`
- Modify: `package.json` (`contributes.commands`, `contributes.menus`)
- Modify: `src/extension.ts`

No new unit tests — `connectionStore.ts` and `connectionCommands.ts` are vscode QuickInput/settings glue, matching the established compile-only treatment of `profileStore.ts`/`secretStore.ts`.

- [ ] **Step 1: Export `STATUS_ICONS` from `treeItems.ts`**

In `src/treeView/treeItems.ts`, change line 9 from:

```typescript
const STATUS_ICONS: Record<ConnectionStatus, string> = {
```

to:

```typescript
export const STATUS_ICONS: Record<ConnectionStatus, string> = {
```

- [ ] **Step 2: Make `KafkaExplorerProvider.profiles` mutable and add `setProfiles`**

In `src/treeView/kafkaExplorerProvider.ts`, change the constructor (around line 23-27) from:

```typescript
  constructor(
    private readonly profiles: ConnectionProfile[],
    private readonly connectionManager: ConnectionManager,
    private readonly thresholds: Thresholds,
  ) {}
```

to:

```typescript
  constructor(
    private profiles: ConnectionProfile[],
    private readonly connectionManager: ConnectionManager,
    private readonly thresholds: Thresholds,
  ) {}

  setProfiles(profiles: ConnectionProfile[]): void {
    this.profiles = profiles;
  }
```

- [ ] **Step 3: Set `contextValue` on connection tree items**

In `src/treeView/kafkaExplorerProvider.ts`, in the `'connection'` case of `getTreeItem` (around line 35-41), change:

```typescript
      case 'connection': {
        const state = this.connectionManager.getState(element.profile.name);
        const view = buildConnectionNode(element.profile.name, state.status, state.error);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.Collapsed);
        item.description = view.description;
        return item;
      }
```

to:

```typescript
      case 'connection': {
        const state = this.connectionManager.getState(element.profile.name);
        const view = buildConnectionNode(element.profile.name, state.status, state.error);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.Collapsed);
        item.description = view.description;
        item.contextValue = 'kafkaConnection';
        return item;
      }
```

- [ ] **Step 4: Create `src/connection/connectionStore.ts`**

```typescript
import * as vscode from 'vscode';
import { ConnectionProfile } from './types';

export async function saveConnectionProfiles(profiles: ConnectionProfile[]): Promise<void> {
  await vscode.workspace
    .getConfiguration('kafkaLagMonitor')
    .update('connections', profiles, vscode.ConfigurationTarget.Global);
}
```

- [ ] **Step 5: Create `src/connection/connectionCommands.ts`**

```typescript
import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { saveConnectionProfiles } from './connectionStore';
import { parseBrokerList, validateProfileName } from './connectionWizard';
import { getConnectionProfiles } from './profileStore';
import { validateProfile } from './profileValidation';
import { deleteCredentials, setCredential } from './secretStore';
import { ConnectionProfile, SaslMechanism } from './types';
import { KafkaExplorerProvider } from '../treeView/kafkaExplorerProvider';
import { STATUS_ICONS } from '../treeView/treeItems';

const AUTH_TYPES: Array<{ label: string; mechanism: SaslMechanism | null }> = [
  { label: 'None', mechanism: null },
  { label: 'PLAIN', mechanism: 'plain' },
  { label: 'SCRAM-SHA-256', mechanism: 'scram-sha-256' },
  { label: 'SCRAM-SHA-512', mechanism: 'scram-sha-512' },
];

interface WizardResult {
  profile: ConnectionProfile;
  username?: string;
  password?: string;
}

async function runConnectionWizard(
  existingNames: string[],
  initial?: ConnectionProfile,
): Promise<WizardResult | undefined> {
  const nameTargets = initial ? existingNames.filter((n) => n !== initial.name) : existingNames;

  const name = await vscode.window.showInputBox({
    title: 'Connection name',
    value: initial?.name ?? '',
    validateInput: (value) => validateProfileName(value, nameTargets),
  });
  if (name === undefined) return undefined;

  const brokersInput = await vscode.window.showInputBox({
    title: 'Brokers (comma-separated host:port)',
    value: initial?.brokers.join(', ') ?? '',
    validateInput: (value) => {
      const { errors } = parseBrokerList(value);
      return errors.length > 0 ? errors.join('; ') : null;
    },
  });
  if (brokersInput === undefined) return undefined;
  const { brokers } = parseBrokerList(brokersInput);

  const sslChoice = await vscode.window.showQuickPick(['No', 'Yes'], {
    title: 'Use SSL?',
    placeHolder: initial?.ssl ? 'Yes' : 'No',
  });
  if (sslChoice === undefined) return undefined;

  const authChoice = await vscode.window.showQuickPick(
    AUTH_TYPES.map((a) => a.label),
    { title: 'Authentication', placeHolder: initial?.sasl?.mechanism ?? 'None' },
  );
  if (authChoice === undefined) return undefined;
  const mechanism = AUTH_TYPES.find((a) => a.label === authChoice)?.mechanism ?? null;

  let username: string | undefined;
  let password: string | undefined;
  if (mechanism) {
    username = await vscode.window.showInputBox({ title: 'Username (leave blank to keep existing)' });
    if (username === undefined) return undefined;

    password = await vscode.window.showInputBox({
      title: 'Password (leave blank to keep existing)',
      password: true,
    });
    if (password === undefined) return undefined;
  }

  const clientId = await vscode.window.showInputBox({
    title: 'Client ID',
    value: initial?.clientId ?? 'kafka-lag-monitor',
  });
  if (clientId === undefined) return undefined;

  const { profile, errors } = validateProfile({
    name,
    brokers,
    sasl: mechanism ? { mechanism } : null,
    ssl: sslChoice === 'Yes',
    clientId,
  });
  if (!profile) {
    vscode.window.showErrorMessage(`Invalid connection: ${errors.join('; ')}`);
    return undefined;
  }

  return { profile, username, password };
}

export function registerConnectionCommands(
  context: vscode.ExtensionContext,
  connectionManager: ConnectionManager,
  explorer: KafkaExplorerProvider,
  onConfigError: (message: string) => void,
): void {
  const refresh = (): void => {
    explorer.setProfiles(getConnectionProfiles(onConfigError));
    explorer.refresh();
  };

  context.subscriptions.push(
    vscode.commands.registerCommand('kafkaLagMonitor.addConnection', async () => {
      const existing = getConnectionProfiles(onConfigError);
      const result = await runConnectionWizard(existing.map((p) => p.name));
      if (!result) return;

      try {
        await saveConnectionProfiles([...existing, result.profile]);
        if (result.profile.sasl) {
          if (result.username) await setCredential(context.secrets, result.profile.name, 'username', result.username);
          if (result.password) await setCredential(context.secrets, result.profile.name, 'password', result.password);
        }
      } catch (err) {
        vscode.window.showErrorMessage((err as Error).message);
        return;
      }
      refresh();
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('kafkaLagMonitor.editConnection', async () => {
      const existing = getConnectionProfiles(onConfigError);
      const target = await vscode.window.showQuickPick(existing.map((p) => p.name), {
        title: 'Edit which connection?',
      });
      if (!target) return;
      const current = existing.find((p) => p.name === target);
      if (!current) return;

      const result = await runConnectionWizard(existing.map((p) => p.name), current);
      if (!result) return;

      try {
        await saveConnectionProfiles(existing.map((p) => (p.name === current.name ? result.profile : p)));
        if (result.profile.sasl) {
          if (result.username) await setCredential(context.secrets, result.profile.name, 'username', result.username);
          if (result.password) await setCredential(context.secrets, result.profile.name, 'password', result.password);
        }
      } catch (err) {
        vscode.window.showErrorMessage((err as Error).message);
        return;
      }
      refresh();
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('kafkaLagMonitor.removeConnection', async () => {
      const existing = getConnectionProfiles(onConfigError);
      const target = await vscode.window.showQuickPick(existing.map((p) => p.name), {
        title: 'Remove which connection?',
      });
      if (!target) return;

      const confirm = await vscode.window.showWarningMessage(
        `Remove connection "${target}" and its stored credentials?`,
        { modal: true },
        'Remove',
      );
      if (confirm !== 'Remove') return;

      try {
        await connectionManager.disconnect(target);
        await deleteCredentials(context.secrets, target, ['username', 'password']);
        await saveConnectionProfiles(existing.filter((p) => p.name !== target));
      } catch (err) {
        vscode.window.showErrorMessage((err as Error).message);
        return;
      }
      refresh();
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('kafkaLagMonitor.reconnect', async () => {
      const existing = getConnectionProfiles(onConfigError);
      const target = await vscode.window.showQuickPick(
        existing.map((p) => ({
          label: `${p.name} ${STATUS_ICONS[connectionManager.getState(p.name).status]}`,
          profile: p,
        })),
        { title: 'Reconnect which connection?' },
      );
      if (!target) return;

      try {
        await connectionManager.reconnect(target.profile);
      } catch (err) {
        vscode.window.showErrorMessage((err as Error).message);
      }
      explorer.refresh();
    }),
  );
}
```

- [ ] **Step 6: Add the four commands to `package.json`**

In `package.json`, replace the `"commands"` array (around line 63-69):

```json
    "commands": [
      {
        "command": "kafkaLagMonitor.refresh",
        "title": "Kafka Lag Monitor: Refresh",
        "icon": "$(refresh)"
      }
    ],
```

with:

```json
    "commands": [
      {
        "command": "kafkaLagMonitor.refresh",
        "title": "Kafka Lag Monitor: Refresh",
        "icon": "$(refresh)"
      },
      {
        "command": "kafkaLagMonitor.addConnection",
        "title": "Kafka: Add Connection",
        "icon": "$(add)"
      },
      {
        "command": "kafkaLagMonitor.editConnection",
        "title": "Kafka: Edit Connection"
      },
      {
        "command": "kafkaLagMonitor.removeConnection",
        "title": "Kafka: Remove Connection"
      },
      {
        "command": "kafkaLagMonitor.reconnect",
        "title": "Kafka: Reconnect"
      }
    ],
```

- [ ] **Step 7: Add menu entries to `package.json`**

In `package.json`, replace the `"menus"` block (around line 70-78):

```json
    "menus": {
      "view/title": [
        {
          "command": "kafkaLagMonitor.refresh",
          "when": "view == kafkaLagMonitor.explorer",
          "group": "navigation"
        }
      ]
    }
```

with:

```json
    "menus": {
      "view/title": [
        {
          "command": "kafkaLagMonitor.refresh",
          "when": "view == kafkaLagMonitor.explorer",
          "group": "navigation"
        },
        {
          "command": "kafkaLagMonitor.addConnection",
          "when": "view == kafkaLagMonitor.explorer",
          "group": "navigation"
        }
      ],
      "view/item/context": [
        {
          "command": "kafkaLagMonitor.editConnection",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.removeConnection",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.reconnect",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        }
      ]
    }
```

- [ ] **Step 8: Wire `registerConnectionCommands` into `extension.ts`**

In `src/extension.ts`, add the import:

```typescript
import { registerConnectionCommands } from './connection/connectionCommands';
```

(add it alphabetically among the existing `./connection/...` imports — i.e. right after the `getConnectionProfiles, getLagThresholds` import line).

Then, after the `vscode.commands.registerCommand('kafkaLagMonitor.refresh', ...)` line, add:

```typescript
  registerConnectionCommands(context, connectionManager, explorer, onConfigError);
```

- [ ] **Step 9: Verify compile, tests, and package.json validity**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 46`, `# pass 46`, `# fail 0`.

Run: `node -e "JSON.parse(require('fs').readFileSync('package.json','utf8')); console.log('valid json')"`
Expected: `valid json`

- [ ] **Step 10: Commit**

```bash
git add src/connection/connectionStore.ts src/connection/connectionCommands.ts src/treeView/treeItems.ts src/treeView/kafkaExplorerProvider.ts src/extension.ts package.json
git commit -m "feat: add Add/Edit/Remove/Reconnect connection commands"
```

---

## Task 5: Topic Metadata webview — pure render functions

**Files:**
- Create: `src/webviews/topicMetadataPanel.ts`
- Test: `src/test/topicMetadataPanel.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `src/test/topicMetadataPanel.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { renderErrorHtml, renderTopicMetadataHtml } from '../webviews/topicMetadataPanel';
import { ConfigEntry, TopicMetadata } from '../kafka/adminService';

const metadata: TopicMetadata = {
  name: 'orders.events',
  partitions: [
    { partitionId: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] },
    { partitionId: 1, leader: 2, replicas: [2, 3, 1], isr: [2, 3] },
  ],
};

const configEntries: ConfigEntry[] = [
  { name: 'retention.ms', value: '604800000', isDefault: false },
  { name: 'cleanup.policy', value: 'delete', isDefault: true },
];

test('renderTopicMetadataHtml includes the topic name and a refresh button wired to postMessage', () => {
  const html = renderTopicMetadataHtml('orders.events', metadata, configEntries);
  assert.match(html, /<h2>orders\.events<\/h2>/);
  assert.match(html, /id="refresh"/);
  assert.match(html, /postMessage\(\{ type: 'refresh' \}\)/);
});

test('renderTopicMetadataHtml renders a partition row per partition with leader/replicas/isr', () => {
  const html = renderTopicMetadataHtml('orders.events', metadata, configEntries);
  assert.match(html, /<td>0<\/td><td>1<\/td><td>1, 2, 3<\/td><td>1, 2, 3<\/td>/);
  assert.match(html, /<td>1<\/td><td>2<\/td><td>2, 3, 1<\/td><td>2, 3<\/td>/);
});

test('renderTopicMetadataHtml renders a config row per entry with its default flag', () => {
  const html = renderTopicMetadataHtml('orders.events', metadata, configEntries);
  assert.match(html, /<td>retention\.ms<\/td><td>604800000<\/td><td>No<\/td>/);
  assert.match(html, /<td>cleanup\.policy<\/td><td>delete<\/td><td>Yes<\/td>/);
});

test('renderTopicMetadataHtml escapes HTML in the topic name and config entries', () => {
  const html = renderTopicMetadataHtml('<script>', metadata, [{ name: '<x>', value: '<y>', isDefault: false }]);
  assert.match(html, /<h2>&lt;script&gt;<\/h2>/);
  assert.match(html, /<td>&lt;x&gt;<\/td><td>&lt;y&gt;<\/td><td>No<\/td>/);
});

test('renderErrorHtml escapes and includes the error message', () => {
  const html = renderErrorHtml('Not connected — <b>retry</b>');
  assert.match(html, /<p>Not connected — &lt;b&gt;retry&lt;\/b&gt;<\/p>/);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error `Cannot find module '../webviews/topicMetadataPanel'`.

- [ ] **Step 3: Create `src/webviews/topicMetadataPanel.ts`**

```typescript
import { ConfigEntry, TopicMetadata } from '../kafka/adminService';

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderPartitionRows(metadata: TopicMetadata): string {
  return metadata.partitions
    .map(
      (p) =>
        `<tr><td>${p.partitionId}</td><td>${p.leader}</td><td>${p.replicas.join(', ')}</td><td>${p.isr.join(', ')}</td></tr>`,
    )
    .join('');
}

function renderConfigRows(configEntries: ConfigEntry[]): string {
  return configEntries
    .map(
      (c) =>
        `<tr><td>${escapeHtml(c.name)}</td><td>${escapeHtml(c.value ?? '')}</td><td>${c.isDefault ? 'Yes' : 'No'}</td></tr>`,
    )
    .join('');
}

export function renderTopicMetadataHtml(topicName: string, metadata: TopicMetadata, configEntries: ConfigEntry[]): string {
  const safeName = escapeHtml(topicName);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Topic: ${safeName}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  table { border-collapse: collapse; width: 100%; margin-bottom: 24px; }
  th, td { border: 1px solid var(--vscode-panel-border, #ccc); padding: 4px 8px; text-align: left; }
  th { background: var(--vscode-editor-lineHighlightBackground, #eee); }
</style>
</head>
<body>
<h2>${safeName}</h2>
<button id="refresh">Refresh</button>
<h3>Partitions</h3>
<table>
<thead><tr><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th></tr></thead>
<tbody>${renderPartitionRows(metadata)}</tbody>
</table>
<h3>Config</h3>
<table>
<thead><tr><th>Name</th><th>Value</th><th>Default?</th></tr></thead>
<tbody>${renderConfigRows(configEntries)}</tbody>
</table>
<script>
  const vscode = acquireVsCodeApi();
  document.getElementById('refresh').addEventListener('click', () => {
    vscode.postMessage({ type: 'refresh' });
  });
</script>
</body>
</html>`;
}

export function renderErrorHtml(message: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Topic Metadata</title></head>
<body>
<p>${escapeHtml(message)}</p>
</body>
</html>`;
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 51`, `# pass 51`, `# fail 0` (46 from Task 2, plus the 5 new tests above).

- [ ] **Step 5: Commit**

```bash
git add src/webviews/topicMetadataPanel.ts src/test/topicMetadataPanel.test.ts
git commit -m "feat: add Topic Metadata webview render functions"
```

---

## Task 6: Topic Metadata webview panel + tree wiring

**Files:**
- Modify: `src/webviews/topicMetadataPanel.ts`
- Modify: `src/treeView/kafkaExplorerProvider.ts`
- Modify: `src/extension.ts`

No new unit tests — `TopicMetadataPanel` is a vscode `WebviewPanel` glue class, matching the established compile-only treatment of vscode-API-touching code. The pure render functions it calls are already covered by Task 5's tests.

- [ ] **Step 1: Add the `TopicMetadataPanel` glue class to `src/webviews/topicMetadataPanel.ts`**

At the top of `src/webviews/topicMetadataPanel.ts`, change the existing import line from:

```typescript
import { ConfigEntry, TopicMetadata } from '../kafka/adminService';
```

to:

```typescript
import * as vscode from 'vscode';
import { ConfigEntry, TopicMetadata } from '../kafka/adminService';
import { ConnectionManager } from '../connection/connectionManager';
```

Then, at the end of the file (after `renderErrorHtml`), append:

```typescript

export class TopicMetadataPanel {
  private static currentPanel: TopicMetadataPanel | undefined;

  private profileName = '';
  private topicName = '';

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
  ) {
    this.panel.webview.onDidReceiveMessage((message: { type: string }) => {
      if (message.type === 'refresh') {
        void this.render();
      }
    });
    this.panel.onDidDispose(() => {
      TopicMetadataPanel.currentPanel = undefined;
    });
  }

  static async show(connectionManager: ConnectionManager, profileName: string, topicName: string): Promise<void> {
    let instance = TopicMetadataPanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaTopicMetadata', 'Topic Metadata', vscode.ViewColumn.Active, {
        enableScripts: true,
      });
      instance = new TopicMetadataPanel(panel, connectionManager);
      TopicMetadataPanel.currentPanel = instance;
    }
    instance.panel.title = `Topic: ${topicName}`;
    instance.profileName = profileName;
    instance.topicName = topicName;
    await instance.render();
  }

  private async render(): Promise<void> {
    const adminService = this.connectionManager.getAdminService(this.profileName);
    if (!adminService) {
      this.panel.webview.html = renderErrorHtml('Not connected — expand the connection in the sidebar first.');
      return;
    }
    try {
      const [metadata, configEntries] = await Promise.all([
        adminService.getTopicMetadata(this.topicName),
        adminService.getTopicConfig(this.topicName),
      ]);
      this.panel.webview.html = renderTopicMetadataHtml(this.topicName, metadata, configEntries);
    } catch (err) {
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }
}
```

- [ ] **Step 2: Give the `'topic'` tree node a `profile` field and wire up the click command**

In `src/treeView/kafkaExplorerProvider.ts`, change the `KafkaTreeNode` union's `'topic'` variant (line 13) from:

```typescript
  | { kind: 'topic'; topic: TopicSummary }
```

to:

```typescript
  | { kind: 'topic'; topic: TopicSummary; profile: ConnectionProfile }
```

In `getTreeItem`, change the `'topic'` case (around line 46-51) from:

```typescript
      case 'topic': {
        const view = buildTopicNode(element.topic.name, element.topic.partitionCount);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.None);
        item.description = view.description;
        return item;
      }
```

to:

```typescript
      case 'topic': {
        const view = buildTopicNode(element.topic.name, element.topic.partitionCount);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.None);
        item.description = view.description;
        item.command = {
          command: 'kafkaLagMonitor.showTopicMetadata',
          title: 'Show Topic Metadata',
          arguments: [element.profile, element.topic.name],
        };
        return item;
      }
```

In `getChildren`, change the `'topicsFolder'` case's success branch (around line 106-107) from:

```typescript
          const topics = await adminService.listTopics();
          return topics.map((topic) => ({ kind: 'topic', topic }));
```

to:

```typescript
          const topics = await adminService.listTopics();
          return topics.map((topic) => ({ kind: 'topic', topic, profile: element.profile }));
```

- [ ] **Step 3: Register `kafkaLagMonitor.showTopicMetadata` in `extension.ts`**

In `src/extension.ts`, change the `./connection/types` import from:

```typescript
import { SaslMechanism } from './connection/types';
```

to:

```typescript
import { ConnectionProfile, SaslMechanism } from './connection/types';
```

Add a new import (alphabetically, after the `./treeView/kafkaExplorerProvider` import):

```typescript
import { TopicMetadataPanel } from './webviews/topicMetadataPanel';
```

After the `registerConnectionCommands(context, connectionManager, explorer, onConfigError);` line, add:

```typescript
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.showTopicMetadata',
      async (profile: ConnectionProfile, topicName: string) => {
        await TopicMetadataPanel.show(connectionManager, profile.name, topicName);
      },
    ),
  );
```

- [ ] **Step 4: Run the tests to verify nothing broke**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 51`, `# pass 51`, `# fail 0` (unchanged from Task 5 — this task adds no new tests).

- [ ] **Step 5: Commit**

```bash
git add src/webviews/topicMetadataPanel.ts src/treeView/kafkaExplorerProvider.ts src/extension.ts
git commit -m "feat: open a Topic Metadata webview when a topic is clicked in the sidebar"
```

---

## Task 7: Update README and final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the "Status" section**

In `README.md`, replace the "Status" section (lines 7-17):

```markdown
## Status

**Phase 1 (this version):** read-only Explorer view showing, per configured
connection, the list of topics (with partition counts) and consumer groups
(with total lag and per-partition breakdown). Connections are configured
directly in `settings.json` — a connection-management wizard, the Lag
Dashboard, Message Browser, and Produce webviews are planned in follow-up
phases (see `docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`).

SASL/SSL authentication is not yet wired up; only PLAINTEXT and SSL-without-SASL
connections are supported.
```

with:

```markdown
## Status

**Phase 1 (this version):** an Explorer view showing, per configured
connection, the list of topics (with partition counts) and consumer groups
(with total lag and per-partition breakdown). Connections are managed with
the **Kafka: Add/Edit/Remove Connection** and **Kafka: Reconnect** commands
(available from the Explorer view title bar and by right-clicking a
connection), backed by VS Code settings and SecretStorage. Clicking a topic
opens a Topic Metadata webview showing its partitions (leader, replicas, ISR)
and configuration. The Lag Dashboard, Message Browser, and Produce webviews
are planned in follow-up phases (see
`docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`).

SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and SSL connections are supported.
mTLS / client-certificate SSL is not yet supported.
```

- [ ] **Step 2: Update the "Configuration" section and add a "Commands" section**

In `README.md`, replace the "Configuration" section (lines 19-35):

```markdown
## Configuration

Add one or more connection profiles to your VS Code settings:

```jsonc
"kafkaLagMonitor.connections": [
  {
    "name": "local-cluster",
    "brokers": ["localhost:9091", "localhost:9092", "localhost:9095"],
    "sasl": null,
    "ssl": false,
    "clientId": "kafka-lag-monitor"
  }
],
"kafkaLagMonitor.lagWarningThreshold": 100,
"kafkaLagMonitor.lagCriticalThreshold": 1000
```
```

with:

```markdown
## Configuration

The easiest way to add a connection is the **Kafka: Add Connection** command
(the `+` icon in the Explorer view title bar), which prompts for a name,
brokers, SSL, authentication, and (for SASL) a username/password. SASL
credentials are stored in VS Code's SecretStorage, not in settings.

Connection profiles can also be viewed or hand-edited in your VS Code
settings (SASL credentials are not stored here — use the Add/Edit Connection
commands for those):

```jsonc
"kafkaLagMonitor.connections": [
  {
    "name": "local-cluster",
    "brokers": ["localhost:9091", "localhost:9092", "localhost:9095"],
    "sasl": null,
    "ssl": false,
    "clientId": "kafka-lag-monitor"
  },
  {
    "name": "secure-cluster",
    "brokers": ["broker1:9093"],
    "sasl": { "mechanism": "scram-sha-512" },
    "ssl": true,
    "clientId": "kafka-lag-monitor"
  }
],
"kafkaLagMonitor.lagWarningThreshold": 100,
"kafkaLagMonitor.lagCriticalThreshold": 1000
```

## Commands

- **Kafka: Add Connection** — wizard to create a new connection profile.
- **Kafka: Edit Connection** — wizard to update an existing connection profile (leave the username/password fields blank to keep the currently stored credentials).
- **Kafka: Remove Connection** — removes a connection profile and its stored credentials, after confirmation.
- **Kafka: Reconnect** — disconnects and re-creates a connection's client (useful after editing brokers or credentials).
- **Kafka Lag Monitor: Refresh** — refreshes the Explorer view.
```

- [ ] **Step 3: Final verification**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 51`, `# pass 51`, `# fail 0`.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document connection commands, SASL support, and the Topic Metadata webview"
```
