# Produce Webview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement roadmap Phase 4 — a "Produce" webview, opened via a right-click "Kafka: Produce Message" action on a topic node, that sends a single message (key, value, optional explicit partition, optional headers) to the topic using a kafkajs `Producer` cached per connection.

**Architecture:** A new pure `src/kafka/producerClient.ts` defines the `KafkaProducerClient`/`ProducerSendResult` interface (mirrors `adminClient.ts`/`consumerClient.ts`). `src/kafka/kafkaProducerAdapter.ts` implements it with a kafkajs `Producer`. A new pure `src/kafka/producerService.ts` holds `ProducerService.send`, normalizing headers/key/partition before delegating to the client. `ConnectionManager` gains a `producers` cache, a `ProducerClientFactory` constructor argument, and an async `getProducerService(profile)` that lazily creates+connects a producer on first use and disposes it on `disconnect()`/`reconnect()`. A new pure `src/webviews/producePanel.ts` holds `renderProduceHtml` and the `ProduceSendMessage` type (mirrors `messageBrowserPanel.ts`). A new `src/webviews/producePanelController.ts` holds the singleton `ProducePanel` vscode glue class (mirrors `MessageBrowserPanel`). `package.json` gains the `kafkaLagMonitor.produce` command and a `view/item/context` entry for `kafkaTopic`. `extension.ts` wires the second `ConnectionManager` factory and registers the command.

**Tech Stack:** TypeScript, vscode Extension API (WebviewPanel, `postMessage`), kafkajs (`Producer`), node:test.

**Reference spec:** `docs/superpowers/specs/2026-06-15-produce-webview-design.md`

---

## Task 1: `KafkaProducerClient` interface and kafkajs adapter

**Files:**
- Create: `src/kafka/producerClient.ts`
- Create: `src/kafka/kafkaProducerAdapter.ts`

No new unit tests — `kafkaProducerAdapter.ts` is a thin wrapper around kafkajs, matching the established compile-only treatment of `kafkaAdminAdapter.ts`/`kafkaConsumerAdapter.ts`. `producerClient.ts` is a pure interface with no logic to test.

- [ ] **Step 1: Create `src/kafka/producerClient.ts`**

```typescript
export interface ProducerSendResult {
  partition: number;
  offset: string;
}

export interface KafkaProducerClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(args: {
    topic: string;
    partition?: number;
    key: string | null;
    value: string;
    headers: Record<string, string>;
  }): Promise<ProducerSendResult>;
}
```

- [ ] **Step 2: Create `src/kafka/kafkaProducerAdapter.ts`**

```typescript
import { Producer } from 'kafkajs';
import { KafkaProducerClient } from './producerClient';

export function createKafkaProducerClient(producer: Producer): KafkaProducerClient {
  return {
    connect: () => producer.connect(),
    disconnect: () => producer.disconnect(),
    send: async ({ topic, partition, key, value, headers }) => {
      const [metadata] = await producer.send({
        topic,
        messages: [{ partition, key, value, headers }],
      });
      return { partition: metadata.partition, offset: metadata.baseOffset ?? '0' };
    },
  };
}
```

- [ ] **Step 3: Run the tests to verify everything still compiles and passes**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 85`, `# pass 85`, `# fail 0` (no new tests; this confirms the two new files type-check cleanly under `tsc -p ./`, since `tsconfig.json`'s `include: ["src/**/*"]` picks up every file regardless of whether it's imported yet).

- [ ] **Step 4: Commit**

```bash
git add src/kafka/producerClient.ts src/kafka/kafkaProducerAdapter.ts
git commit -m "feat: add KafkaProducerClient interface and kafkajs adapter"
```

---

## Task 2: `ProducerService`

**Files:**
- Create: `src/kafka/producerService.ts`
- Create: `src/test/producerService.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `src/test/producerService.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { ProducerService } from '../kafka/producerService';
import { KafkaProducerClient } from '../kafka/producerClient';

function createFakeProducerClient(): { client: KafkaProducerClient; calls: Parameters<KafkaProducerClient['send']>[0][] } {
  const calls: Parameters<KafkaProducerClient['send']>[0][] = [];
  const client: KafkaProducerClient = {
    connect: async () => {},
    disconnect: async () => {},
    send: async (args) => {
      calls.push(args);
      return { partition: 0, offset: '42' };
    },
  };
  return { client, calls };
}

test('send drops header rows with an empty key and keeps rows with a non-empty key', async () => {
  const { client, calls } = createFakeProducerClient();
  const service = new ProducerService(client);

  await service.send({
    topic: 'orders.events',
    partition: null,
    key: '',
    value: 'payload',
    headers: [
      { key: '', value: 'dropped' },
      { key: 'trace-id', value: 'abc-123' },
    ],
  });

  assert.deepEqual(calls[0].headers, { 'trace-id': 'abc-123' });
});

test('send converts an empty key to null, and passes a non-empty key through unchanged', async () => {
  const { client, calls } = createFakeProducerClient();
  const service = new ProducerService(client);

  await service.send({ topic: 'orders.events', partition: null, key: '', value: 'payload', headers: [] });
  await service.send({ topic: 'orders.events', partition: null, key: 'order-1', value: 'payload', headers: [] });

  assert.equal(calls[0].key, null);
  assert.equal(calls[1].key, 'order-1');
});

test('send converts partition null to undefined, and passes a numeric partition through unchanged', async () => {
  const { client, calls } = createFakeProducerClient();
  const service = new ProducerService(client);

  await service.send({ topic: 'orders.events', partition: null, key: 'order-1', value: 'payload', headers: [] });
  await service.send({ topic: 'orders.events', partition: 2, key: 'order-1', value: 'payload', headers: [] });

  assert.equal(calls[0].partition, undefined);
  assert.equal(calls[1].partition, 2);
});

test('send passes topic and value through unchanged, and returns the client result unchanged', async () => {
  const { client, calls } = createFakeProducerClient();
  const service = new ProducerService(client);

  const result = await service.send({ topic: 'orders.events', partition: null, key: 'order-1', value: 'payload', headers: [] });

  assert.equal(calls[0].topic, 'orders.events');
  assert.equal(calls[0].value, 'payload');
  assert.deepEqual(result, { partition: 0, offset: '42' });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error, e.g. `Cannot find module '../kafka/producerService' or its corresponding type declarations.`

- [ ] **Step 3: Create `src/kafka/producerService.ts`**

```typescript
import { KafkaProducerClient, ProducerSendResult } from './producerClient';

export interface HeaderEntry {
  key: string;
  value: string;
}

export interface ProduceRequest {
  topic: string;
  partition: number | null;
  key: string;
  value: string;
  headers: HeaderEntry[];
}

export class ProducerService {
  constructor(private readonly client: KafkaProducerClient) {}

  async send(request: ProduceRequest): Promise<ProducerSendResult> {
    const headers: Record<string, string> = {};
    for (const header of request.headers) {
      if (header.key === '') continue;
      headers[header.key] = header.value;
    }
    return this.client.send({
      topic: request.topic,
      partition: request.partition ?? undefined,
      key: request.key === '' ? null : request.key,
      value: request.value,
      headers,
    });
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 89`, `# pass 89`, `# fail 0` (85 existing + 4 new).

- [ ] **Step 5: Commit**

```bash
git add src/kafka/producerService.ts src/test/producerService.test.ts
git commit -m "feat: add ProducerService for the Produce webview"
```

---

## Task 3: Cache a producer client per connection in `ConnectionManager`

**Files:**
- Modify: `src/connection/connectionManager.ts`
- Modify: `src/test/connectionManager.test.ts`

- [ ] **Step 1: Write the failing tests**

Replace the entire contents of `src/test/connectionManager.test.ts` with:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { ConnectionManager } from '../connection/connectionManager';
import { KafkaAdminClient } from '../kafka/adminClient';
import { KafkaProducerClient } from '../kafka/producerClient';
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

function createFakeProducerClient(overrides: Partial<KafkaProducerClient> = {}): KafkaProducerClient {
  return {
    connect: async () => {},
    disconnect: async () => {},
    send: async () => ({ partition: 0, offset: '0' }),
    ...overrides,
  };
}

const fakeProducerFactory = async () => createFakeProducerClient();

const profile: ConnectionProfile = {
  name: 'local-cluster',
  brokers: ['localhost:9091'],
  sasl: null,
  ssl: false,
  clientId: 'kafka-lag-monitor',
};

test('connect transitions idle -> connected and exposes an AdminService', async () => {
  const client = createFakeAdminClient();
  const manager = new ConnectionManager(async () => client, fakeProducerFactory);

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
  const manager = new ConnectionManager(async () => client, fakeProducerFactory);

  await assert.rejects(() => manager.connect(profile), /ECONNREFUSED/);

  assert.deepEqual(manager.getState(profile.name), { status: 'error', error: 'ECONNREFUSED' });
  assert.equal(manager.getAdminService(profile.name), undefined);
});

test('disconnect resets status to idle and re-creates the client on the next connect', async () => {
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    return createFakeAdminClient();
  }, fakeProducerFactory);

  await manager.connect(profile);
  await manager.disconnect(profile.name);

  assert.equal(manager.getState(profile.name).status, 'idle');
  assert.equal(manager.getAdminService(profile.name), undefined);

  await manager.connect(profile);

  assert.equal(createCount, 2);
});

test('getState returns idle for a profile that has never been connected', () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient(), fakeProducerFactory);
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
  }, fakeProducerFactory);

  await manager.connect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  await manager.reconnect(profile);

  assert.equal(createCount, 2);
  assert.ok(disconnectedOld);
  assert.equal(manager.getState(profile.name).status, 'connected');
});

test('reconnect works when the profile was never connected', async () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient(), fakeProducerFactory);

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
  }, fakeProducerFactory);

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
  }, fakeProducerFactory);

  const connectPromise = manager.connect(profile).catch(() => undefined);

  await manager.reconnect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  rejectFirstConnect(new Error('ECONNREFUSED'));
  await connectPromise;

  assert.equal(manager.getState(profile.name).status, 'connected');
});

test('disconnect() during an in-flight reconnect() leaves status idle', async () => {
  let resolveReconnectConnect: () => void = () => {};
  let connectCalled: () => void = () => {};
  const connectCalledPromise = new Promise<void>((resolve) => {
    connectCalled = resolve;
  });
  const firstClient = createFakeAdminClient();
  const secondClient = createFakeAdminClient({
    connect: () =>
      new Promise<void>((resolve) => {
        resolveReconnectConnect = resolve;
        connectCalled();
      }),
  });
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    return createCount === 1 ? firstClient : secondClient;
  }, fakeProducerFactory);

  await manager.connect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  const reconnectPromise = manager.reconnect(profile);

  // Wait until reconnect() has reached its in-flight client.connect() call.
  await connectCalledPromise;

  await manager.disconnect(profile.name);
  assert.equal(manager.getState(profile.name).status, 'idle');

  resolveReconnectConnect();
  await reconnectPromise;

  assert.equal(manager.getState(profile.name).status, 'idle');
});

test('getProducerService returns undefined when not connected', async () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient(), fakeProducerFactory);

  assert.equal(await manager.getProducerService(profile), undefined);
});

test('getProducerService creates and connects a producer client lazily, and reuses it on subsequent calls', async () => {
  let createCount = 0;
  let connected = 0;
  const manager = new ConnectionManager(
    async () => createFakeAdminClient(),
    async () => {
      createCount += 1;
      return createFakeProducerClient({
        connect: async () => {
          connected += 1;
        },
      });
    },
  );

  await manager.connect(profile);

  const first = await manager.getProducerService(profile);
  const second = await manager.getProducerService(profile);

  assert.ok(first);
  assert.ok(second);
  assert.equal(createCount, 1);
  assert.equal(connected, 1);
});

test('disconnect disposes the cached producer client', async () => {
  let createCount = 0;
  let disconnected = 0;
  const manager = new ConnectionManager(
    async () => createFakeAdminClient(),
    async () => {
      createCount += 1;
      return createFakeProducerClient({
        disconnect: async () => {
          disconnected += 1;
        },
      });
    },
  );

  await manager.connect(profile);
  await manager.getProducerService(profile);

  await manager.disconnect(profile.name);

  assert.equal(disconnected, 1);

  await manager.connect(profile);
  await manager.getProducerService(profile);

  assert.equal(createCount, 2);
});

test('reconnect disposes the cached producer client', async () => {
  let createCount = 0;
  let disconnected = 0;
  const manager = new ConnectionManager(
    async () => createFakeAdminClient(),
    async () => {
      createCount += 1;
      return createFakeProducerClient({
        disconnect: async () => {
          disconnected += 1;
        },
      });
    },
  );

  await manager.connect(profile);
  await manager.getProducerService(profile);

  await manager.reconnect(profile);

  assert.equal(disconnected, 1);

  await manager.getProducerService(profile);

  assert.equal(createCount, 2);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile errors, e.g. `Expected 1 arguments, but got 2.` on every `new ConnectionManager(..., fakeProducerFactory)` call, and `Property 'getProducerService' does not exist on type 'ConnectionManager'`.

- [ ] **Step 3: Replace the entire contents of `src/connection/connectionManager.ts`**

```typescript
import { ConnectionProfile, ConnectionStatus } from './types';
import { KafkaAdminClient } from '../kafka/adminClient';
import { AdminService } from '../kafka/adminService';
import { KafkaProducerClient } from '../kafka/producerClient';
import { ProducerService } from '../kafka/producerService';

export interface ConnectionState {
  status: ConnectionStatus;
  error?: string;
}

export type AdminClientFactory = (profile: ConnectionProfile) => Promise<KafkaAdminClient>;
export type ProducerClientFactory = (profile: ConnectionProfile) => Promise<KafkaProducerClient>;

export class ConnectionManager {
  private readonly clients = new Map<string, KafkaAdminClient>();
  private readonly producers = new Map<string, KafkaProducerClient>();
  private readonly states = new Map<string, ConnectionState>();
  private readonly generations = new Map<string, number>();

  constructor(
    private readonly createAdminClient: AdminClientFactory,
    private readonly createProducerClient: ProducerClientFactory,
  ) {}

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
    const existingProducer = this.producers.get(profile.name);
    if (existingProducer) {
      this.producers.delete(profile.name);
      await existingProducer.disconnect().catch(() => undefined);
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
    this.nextGeneration(profileName);
    const client = this.clients.get(profileName);
    if (client) {
      await client.disconnect();
      this.clients.delete(profileName);
    }
    const producer = this.producers.get(profileName);
    if (producer) {
      this.producers.delete(profileName);
      await producer.disconnect().catch(() => undefined);
    }
    this.states.set(profileName, { status: 'idle' });
  }

  getAdminService(profileName: string): AdminService | undefined {
    const state = this.getState(profileName);
    const client = this.clients.get(profileName);
    if (state.status !== 'connected' || !client) return undefined;
    return new AdminService(client);
  }

  async getProducerService(profile: ConnectionProfile): Promise<ProducerService | undefined> {
    if (this.getState(profile.name).status !== 'connected') return undefined;

    let client = this.producers.get(profile.name);
    if (!client) {
      client = await this.createProducerClient(profile);
      await client.connect();
      this.producers.set(profile.name, client);
    }
    return new ProducerService(client);
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 93`, `# pass 93`, `# fail 0` (89 existing + 4 new).

- [ ] **Step 5: Commit**

```bash
git add src/connection/connectionManager.ts src/test/connectionManager.test.ts
git commit -m "feat: cache a producer client per connection in ConnectionManager"
```

---

## Task 4: `renderProduceHtml` for the Produce webview

**Files:**
- Create: `src/webviews/producePanel.ts`
- Create: `src/test/producePanel.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `src/test/producePanel.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { renderProduceHtml } from '../webviews/producePanel';

test('renderProduceHtml escapes the topic name in the title and heading', () => {
  const html = renderProduceHtml('orders<events>', 3);

  assert.match(html, /<title>Produce: orders&lt;events&gt;<\/title>/);
  assert.match(html, /<h2>Produce: orders&lt;events&gt;<\/h2>/);
});

test('renderProduceHtml includes the form element ids', () => {
  const html = renderProduceHtml('orders.events', 3);

  assert.match(html, /id="partition"/);
  assert.match(html, /id="key"/);
  assert.match(html, /id="value"/);
  assert.match(html, /id="headers"/);
  assert.match(html, /id="addHeader"/);
  assert.match(html, /id="send"/);
  assert.match(html, /id="result"/);
});

test('renderProduceHtml embeds PARTITION_COUNT and builds the "Auto (by key)" option', () => {
  const html = renderProduceHtml('orders.events', 3);

  assert.match(html, /const PARTITION_COUNT = 3;/);
  assert.match(html, /autoOption\.value = '';/);
  assert.match(html, /autoOption\.textContent = 'Auto \(by key\)';/);
  assert.match(html, /for \(let i = 0; i < PARTITION_COUNT; i\+\+\)/);
});

test('renderProduceHtml wires the Send button to post a send message with the form values', () => {
  const html = renderProduceHtml('orders.events', 3);

  assert.match(html, /sendButton\.addEventListener\('click', \(\) => \{/);
  assert.match(html, /type: 'send',/);
  assert.match(html, /partition: partitionValue === '' \? null : Number\(partitionValue\),/);
  assert.match(html, /key: document\.getElementById\('key'\)\.value,/);
  assert.match(html, /value: document\.getElementById\('value'\)\.value,/);
});

test('renderProduceHtml renders success and error results from the result message', () => {
  const html = renderProduceHtml('orders.events', 3);

  assert.match(html, /if \(message\.type !== 'result'\) return;/);
  assert.match(html, /resultDiv\.className = 'success';/);
  assert.match(html, /resultDiv\.className = 'error';/);
  assert.match(html, /'Sent to partition ' \+ message\.partition \+ ', offset ' \+ message\.offset/);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error, e.g. `Cannot find module '../webviews/producePanel' or its corresponding type declarations.`

- [ ] **Step 3: Create `src/webviews/producePanel.ts`**

```typescript
import { escapeHtml } from './topicMetadataPanel';
import { HeaderEntry } from '../kafka/producerService';

export interface ProduceSendMessage {
  type: 'send';
  partition: number | null;
  key: string;
  value: string;
  headers: HeaderEntry[];
}

export function renderProduceHtml(topic: string, partitionCount: number): string {
  const safeTopic = escapeHtml(topic);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Produce: ${safeTopic}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  label { display: block; margin: 12px 0 4px; font-weight: bold; }
  select, input[type="text"], textarea {
    width: 100%; box-sizing: border-box; font-family: var(--vscode-font-family, sans-serif);
  }
  textarea { font-family: var(--vscode-editor-font-family, monospace); min-height: 120px; }
  .header-row { display: flex; gap: 8px; margin-bottom: 4px; }
  .header-row input { flex: 1; }
  #result { margin: 12px 0; padding: 8px; display: none; }
  #result.success {
    background: var(--vscode-inputValidation-infoBackground, #1d3a5a);
    border: 1px solid var(--vscode-inputValidation-infoBorder, #3794ff);
  }
  #result.error {
    background: var(--vscode-inputValidation-errorBackground, #5a1d1d);
    border: 1px solid var(--vscode-inputValidation-errorBorder, #be1100);
  }
  .actions { margin: 16px 0; }
</style>
</head>
<body>
<h2>Produce: ${safeTopic}</h2>

<label for="partition">Partition</label>
<select id="partition"></select>

<label for="key">Key</label>
<input type="text" id="key">

<label for="value">Value</label>
<textarea id="value"></textarea>

<label>Headers</label>
<div id="headers"></div>
<button id="addHeader" type="button">+ Add header</button>

<div class="actions">
  <button id="send" type="button">Send</button>
</div>
<div id="result"></div>

<script>
  const vscode = acquireVsCodeApi();
  const PARTITION_COUNT = ${partitionCount};

  const partitionSelect = document.getElementById('partition');
  const autoOption = document.createElement('option');
  autoOption.value = '';
  autoOption.textContent = 'Auto (by key)';
  partitionSelect.appendChild(autoOption);
  for (let i = 0; i < PARTITION_COUNT; i++) {
    const option = document.createElement('option');
    option.value = String(i);
    option.textContent = String(i);
    partitionSelect.appendChild(option);
  }

  const headersContainer = document.getElementById('headers');
  function addHeaderRow() {
    const row = document.createElement('div');
    row.className = 'header-row';

    const keyInput = document.createElement('input');
    keyInput.type = 'text';
    keyInput.placeholder = 'Header key';
    row.appendChild(keyInput);

    const valueInput = document.createElement('input');
    valueInput.type = 'text';
    valueInput.placeholder = 'Header value';
    row.appendChild(valueInput);

    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.textContent = 'Remove';
    removeButton.addEventListener('click', () => row.remove());
    row.appendChild(removeButton);

    headersContainer.appendChild(row);
  }
  document.getElementById('addHeader').addEventListener('click', addHeaderRow);

  const sendButton = document.getElementById('send');
  const resultDiv = document.getElementById('result');

  sendButton.addEventListener('click', () => {
    const headers = [];
    for (const row of headersContainer.children) {
      const inputs = row.querySelectorAll('input');
      headers.push({ key: inputs[0].value, value: inputs[1].value });
    }
    const partitionValue = partitionSelect.value;
    sendButton.disabled = true;
    resultDiv.style.display = 'none';
    vscode.postMessage({
      type: 'send',
      partition: partitionValue === '' ? null : Number(partitionValue),
      key: document.getElementById('key').value,
      value: document.getElementById('value').value,
      headers,
    });
  });

  window.addEventListener('message', (event) => {
    const message = event.data;
    if (message.type !== 'result') return;
    sendButton.disabled = false;
    resultDiv.style.display = 'block';
    if (message.success) {
      resultDiv.className = 'success';
      resultDiv.textContent = 'Sent to partition ' + message.partition + ', offset ' + message.offset;
    } else {
      resultDiv.className = 'error';
      resultDiv.textContent = message.message;
    }
  });
</script>
</body>
</html>`;
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 98`, `# pass 98`, `# fail 0` (93 existing + 5 new).

- [ ] **Step 5: Commit**

```bash
git add src/webviews/producePanel.ts src/test/producePanel.test.ts
git commit -m "feat: add renderProduceHtml for the Produce webview"
```

---

## Task 5: `ProducePanel` webview controller

**Files:**
- Create: `src/webviews/producePanelController.ts`

No new unit tests — `producePanelController.ts` is vscode glue (singleton webview panel), matching the compile-only treatment of `messageBrowserPanelController.ts`/`topicMetadataPanelController.ts`.

- [ ] **Step 1: Create `src/webviews/producePanelController.ts`**

```typescript
import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { ConnectionProfile } from '../connection/types';
import { renderProduceHtml, ProduceSendMessage } from './producePanel';
import { renderErrorHtml } from './topicMetadataPanel';

export class ProducePanel {
  private static currentPanel: ProducePanel | undefined;

  private profile: ConnectionProfile | undefined;
  private topicName = '';
  private generation = 0;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
  ) {
    this.panel.webview.onDidReceiveMessage((message: ProduceSendMessage) => {
      if (message.type === 'send') void this.send(message);
    });
    this.panel.onDidDispose(() => {
      ProducePanel.currentPanel = undefined;
    });
  }

  static async show(connectionManager: ConnectionManager, profile: ConnectionProfile, topicName: string): Promise<void> {
    let instance = ProducePanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaProduce', 'Produce', vscode.ViewColumn.Active, {
        enableScripts: true,
        retainContextWhenHidden: true,
      });
      instance = new ProducePanel(panel, connectionManager);
      ProducePanel.currentPanel = instance;
    }
    instance.panel.title = `Produce: ${topicName}`;
    instance.profile = profile;
    instance.topicName = topicName;
    await instance.renderFull();
  }

  private async renderFull(): Promise<void> {
    const gen = ++this.generation;
    const profile = this.profile!;
    const adminService = this.connectionManager.getAdminService(profile.name);
    if (!adminService) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml('Not connected — expand the connection in the sidebar first.');
      return;
    }
    try {
      const metadata = await adminService.getTopicMetadata(this.topicName);
      if (gen !== this.generation) return;
      this.panel.webview.html = renderProduceHtml(this.topicName, metadata.partitions.length);
    } catch (err) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }

  private async send(message: ProduceSendMessage): Promise<void> {
    const gen = this.generation;
    const profile = this.profile!;
    const topic = this.topicName;
    try {
      const producerService = await this.connectionManager.getProducerService(profile);
      if (gen !== this.generation) return;
      if (!producerService) {
        void this.panel.webview.postMessage({
          type: 'result',
          success: false,
          message: 'Not connected — expand the connection in the sidebar first.',
        });
        return;
      }
      const result = await producerService.send({
        topic,
        partition: message.partition,
        key: message.key,
        value: message.value,
        headers: message.headers,
      });
      if (gen !== this.generation) return;
      void this.panel.webview.postMessage({ type: 'result', success: true, partition: result.partition, offset: result.offset });
    } catch (err) {
      if (gen !== this.generation) return;
      void this.panel.webview.postMessage({ type: 'result', success: false, message: (err as Error).message });
    }
  }
}
```

- [ ] **Step 2: Run the tests to verify everything still compiles and passes**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 98`, `# pass 98`, `# fail 0`.

- [ ] **Step 3: Commit**

```bash
git add src/webviews/producePanelController.ts
git commit -m "feat: add ProducePanel webview controller"
```

---

## Task 6: `package.json` command and context menu entry

**Files:**
- Modify: `package.json`

No new unit tests — `package.json` contribution points aren't exercised by `node:test`; this task is verified by `npm test` still passing (i.e., the JSON stays valid and nothing else breaks) plus manual inspection.

- [ ] **Step 1: Add the `kafkaLagMonitor.produce` command**

In `package.json`, in the `contributes.commands` array, insert a new entry immediately before the `kafkaLagMonitor.browseMessages` entry.

Change:
```jsonc
      {
        "command": "kafkaLagMonitor.reconnect",
        "title": "Kafka: Reconnect"
      },
      {
        "command": "kafkaLagMonitor.browseMessages",
        "title": "Kafka: Browse Messages"
      }
    ],
```
to:
```jsonc
      {
        "command": "kafkaLagMonitor.reconnect",
        "title": "Kafka: Reconnect"
      },
      {
        "command": "kafkaLagMonitor.produce",
        "title": "Kafka: Produce Message"
      },
      {
        "command": "kafkaLagMonitor.browseMessages",
        "title": "Kafka: Browse Messages"
      }
    ],
```

- [ ] **Step 2: Add the `view/item/context` menu entry**

Change:
```jsonc
        {
          "command": "kafkaLagMonitor.reconnect",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.browseMessages",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaTopic"
        }
      ]
```
to:
```jsonc
        {
          "command": "kafkaLagMonitor.reconnect",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.produce",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaTopic"
        },
        {
          "command": "kafkaLagMonitor.browseMessages",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaTopic"
        }
      ]
```

- [ ] **Step 3: Run the tests to verify everything still compiles and passes**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 98`, `# pass 98`, `# fail 0`.

- [ ] **Step 4: Commit**

```bash
git add package.json
git commit -m "feat: add Kafka: Produce Message context menu entry on topic nodes"
```

---

## Task 7: Register the `kafkaLagMonitor.produce` command

**Files:**
- Modify: `src/extension.ts`

No new unit tests — `extension.ts` is vscode activation glue with no unit tests, matching existing precedent for the `browseMessages`/`showTopicMetadata`/`showLagDashboard` registrations.

- [ ] **Step 1: Add the producer adapter and `ProducePanel` imports**

Change:
```typescript
import { createKafkaConsumerClient } from './kafka/kafkaConsumerAdapter';
```
to:
```typescript
import { createKafkaConsumerClient } from './kafka/kafkaConsumerAdapter';
import { createKafkaProducerClient } from './kafka/kafkaProducerAdapter';
```

And change:
```typescript
import { MessageBrowserPanel } from './webviews/messageBrowserPanelController';
```
to:
```typescript
import { MessageBrowserPanel } from './webviews/messageBrowserPanelController';
import { ProducePanel } from './webviews/producePanelController';
```

- [ ] **Step 2: Give `ConnectionManager` a producer-client factory**

Change:
```typescript
  const connectionManager = new ConnectionManager(async (profile) =>
    createKafkaAdminClient((await buildKafka(profile)).admin()),
  );
```
to:
```typescript
  const connectionManager = new ConnectionManager(
    async (profile) => createKafkaAdminClient((await buildKafka(profile)).admin()),
    async (profile) => createKafkaProducerClient((await buildKafka(profile)).producer()),
  );
```

- [ ] **Step 3: Register the `kafkaLagMonitor.produce` command**

After the existing `kafkaLagMonitor.browseMessages` registration (the final `context.subscriptions.push(...)` block in `activate`), add:

```typescript
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.produce',
      async (profile: ConnectionProfile, topicName: string) => {
        await ProducePanel.show(connectionManager, profile, topicName);
      },
    ),
  );
```

- [ ] **Step 4: Run the tests to verify everything still compiles and passes**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 98`, `# pass 98`, `# fail 0`.

- [ ] **Step 5: Commit**

```bash
git add src/extension.ts
git commit -m "feat: register the Kafka: Produce Message command"
```

---

## Task 8: Document the Produce webview in `README.md`

**Files:**
- Modify: `README.md`

No new unit tests — documentation only, verified by `npm test` still passing.

- [ ] **Step 1: Update the "Status" section**

Change:
```markdown
Clicking a consumer group opens a Lag Dashboard webview showing total lag,
overall status, and a per-topic/per-partition progress-bar breakdown, with a
manual refresh button and an auto-refresh toggle (interval configured via
`kafkaLagMonitor.pollIntervalSeconds`). Right-clicking a topic and choosing
**Kafka: Browse Messages** opens a Message Browser webview showing a table of
the topic's most recent messages (Offset, Timestamp, Key, Value, Headers) for
a chosen partition, with Earliest/Prev/Next/Latest/Refresh navigation and a
partition selector. A Produce webview is planned in a follow-up phase (see
`docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`).
```
to:
```markdown
Clicking a consumer group opens a Lag Dashboard webview showing total lag,
overall status, and a per-topic/per-partition progress-bar breakdown, with a
manual refresh button and an auto-refresh toggle (interval configured via
`kafkaLagMonitor.pollIntervalSeconds`). Right-clicking a topic and choosing
**Kafka: Browse Messages** opens a Message Browser webview showing a table of
the topic's most recent messages (Offset, Timestamp, Key, Value, Headers) for
a chosen partition, with Earliest/Prev/Next/Latest/Refresh navigation and a
partition selector. Right-clicking a topic and choosing **Kafka: Produce
Message** opens a Produce webview with Partition, Key, Value, and Headers
fields and a Send button; on success the result banner shows the partition
and offset of the produced message, and on failure it shows the kafkajs error
message verbatim.
```

- [ ] **Step 2: Add a Produce step to the "Manual integration test" section**

Change:
```markdown
Then `F5` the extension and expand `local-cluster` in the Explorer sidebar —
`orders.events` should show 3 partitions, and `order-service` should show a
total lag of 3. Clicking `order-service` opens the Lag Dashboard, which should
show a Total Lag of 3 with one `orders.events` section and per-partition
progress bars. Right-click `orders.events` and choose **Kafka: Browse
Messages** — the panel should open for partition 0 showing the most recent
messages with Offset/Timestamp/Key/Value/Headers columns; use the partition
selector and the Earliest/Prev/Next/Latest/Refresh buttons to navigate.
```
to:
```markdown
Then `F5` the extension and expand `local-cluster` in the Explorer sidebar —
`orders.events` should show 3 partitions, and `order-service` should show a
total lag of 3. Clicking `order-service` opens the Lag Dashboard, which should
show a Total Lag of 3 with one `orders.events` section and per-partition
progress bars. Right-click `orders.events` and choose **Kafka: Browse
Messages** — the panel should open for partition 0 showing the most recent
messages with Offset/Timestamp/Key/Value/Headers columns; use the partition
selector and the Earliest/Prev/Next/Latest/Refresh buttons to navigate.
Right-click `orders.events` and choose **Kafka: Produce Message** — fill in a
key (e.g. `order-6`), a value (e.g. `{"id":6,"status":"created"}`), and one
header (e.g. `trace-id` / `abc-123`), then click **Send**. The result banner
should show `Sent to partition <p>, offset <o>`. Switch to (or re-open via
**Kafka: Browse Messages**) the Message Browser panel and click **Latest** to
confirm the new message appears at that offset with the key, value, and
header you entered.
```

- [ ] **Step 3: Run the tests to verify everything still passes**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 98`, `# pass 98`, `# fail 0`.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document the Produce webview"
```
